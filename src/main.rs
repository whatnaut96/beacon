use clap::Parser;
use futures::{
    FutureExt, StreamExt,
    future::{Either, pending},
    select_biased,
};
use futures_timer::Delay;
use notify::{Event, RecursiveMode, Result, Watcher};
use reqwest::{Certificate, Client, Url};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::{debug, error, info};

/// A small client to send data from Elite Dangerous Journals to an HTTP Server
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct BeaconArgs {
    /// Path of the journals to watch
    #[arg(short, long)]
    watch_path: PathBuf,
    /// Debounce interval for the file watcher (milliseconds)
    #[arg(long, default_value_t = 50)]
    debounce_ms: u64,
    /// HTTP endpoint to send data to
    #[arg(long)]
    endpoint: String,
    /// Enable TLS
    #[arg(long)]
    enable_tls: bool,
    /// PEM CA certificate bundle to use instead of built-in roots
    #[arg(long, value_name = "PATH", requires = "enable_tls")]
    tls_ca_cert: Vec<PathBuf>,
}

fn is_json(path: &PathBuf) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "json")
}
fn is_journal_log(path: &PathBuf) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("Journal.") && name.ends_with(".log"))
}

async fn send_json(client: &Client, endpoint: &str, body: String) {
    if let Err(err) = client
        .post(endpoint)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .and_then(|response| response.error_for_status())
    {
        error!(%err, "failed to send journal data");
    }
}

async fn run(watch_path: PathBuf, debounce_ms: u64, client: &Client, endpoint: &str) -> Result<()> {
    let mut pending_files: HashMap<PathBuf, Instant> = HashMap::new();
    let mut log_offsets: HashMap<PathBuf, u64> = HashMap::new();

    let (mut tx, mut rx) = futures::channel::mpsc::channel::<Result<Event>>(20);
    let mut watcher = notify::recommended_watcher(move |result| {
        let _ = tx.start_send(result);
    })?;

    watcher.watch(&watch_path, RecursiveMode::NonRecursive)?;

    let debounce_duration = debounce_ms;
    loop {
        let next_timeout = match pending_files.values().min() {
            Some(earliest) => Either::Left(Delay::new(
                earliest.saturating_duration_since(Instant::now()),
            )),
            None => Either::Right(pending::<()>()),
        };
        let mut next_event = rx.next().fuse();
        select_biased! {
            event_result = next_event => match event_result {
                Some(Ok(event)) if matches!(
                    event.kind, notify::EventKind::Create(_) | notify::EventKind::Modify(_)) => {
                    for path in event.paths {
                        if is_json(&path) || is_journal_log(&path){
                            debug!(path = %path.display(), "resetting debounce");
                            pending_files.insert(path, Instant::now() + Duration::from_millis(debounce_duration));
                        }
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => {
                    error!(%err, "file watcher error")
                }
                None => {},
            },
            _ = next_timeout.fuse() => {
                let now = Instant::now();
                let ready_files: Vec<PathBuf> = pending_files
                    .iter()
                    .filter(|(_, deadline)| **deadline <= now)
                    .map(|(path, _)| path.clone())
                    .collect();

                for path in ready_files {
                    pending_files.remove(&path);
                    info!(path=%path.display(), "debounce timer expired. processing journal");
                    if is_json(&path) {
                        let contents = fs::read_to_string(path)?;
                        send_json(client, endpoint, contents).await;
                    } else if is_journal_log(&path) {
                        let mut file = File::open(&path)?;
                        let file_len = file.metadata()?.len();
                        let offset = log_offsets.get(&path).copied().unwrap_or(0);
                        let offset = if offset <= file_len { offset } else { 0 };
                        file.seek(SeekFrom::Start(offset))?;
                        let mut line = String::new();
                        let mut reader = BufReader::new(file);

                        while reader.read_line(&mut line)? > 0 {
                            send_json(client, endpoint, std::mem::take(&mut line)).await;
                        }
                        log_offsets.insert(path, reader.stream_position()?);
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let args = BeaconArgs::parse();

    let expected_scheme = if args.enable_tls { "https" } else { "http" };
    let endpoint = if args.endpoint.contains("://") {
        args.endpoint
    } else {
        format!("{expected_scheme}://{}", args.endpoint)
    };
    let endpoint = Url::parse(&endpoint)?;
    if endpoint.scheme() != expected_scheme {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "endpoint must use {expected_scheme} when --enable-tls is {}",
                args.enable_tls
            ),
        )
        .into());
    }

    let mut client_builder = Client::builder().https_only(args.enable_tls);
    if !args.enable_tls {
        client_builder = client_builder.tls_certs_only(Vec::<Certificate>::new());
    } else if !args.tls_ca_cert.is_empty() {
        let mut certificates = Vec::new();
        for path in &args.tls_ca_cert {
            certificates.extend(Certificate::from_pem_bundle(&fs::read(path)?)?);
        }
        client_builder = client_builder.tls_certs_only(certificates);
    }
    let client = client_builder.build()?;

    info!("staring file watcher");
    run(
        args.watch_path,
        args.debounce_ms,
        &client,
        endpoint.as_str(),
    )
    .await?;
    Ok(())
}
