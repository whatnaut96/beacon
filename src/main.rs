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
    io::{self, BufRead, BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{debug, error, info};

const OFFSET_CACHE_FILENAME: &str = ".beacon-offsets";

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

fn is_json(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "json")
}
fn is_journal_log(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("Journal.") && name.ends_with(".log"))
}

fn load_log_offsets(cache_path: &Path) -> io::Result<HashMap<PathBuf, u64>> {
    match fs::read(cache_path) {
        Ok(contents) => serde_json::from_slice(&contents)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err),
    }
}

fn save_log_offsets(cache_path: &Path, offsets: &HashMap<PathBuf, u64>) -> io::Result<()> {
    let contents = serde_json::to_vec(offsets)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let temporary_path = cache_path.with_extension("tmp");
    fs::write(&temporary_path, contents)?;
    fs::rename(temporary_path, cache_path)
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
    let offset_cache_path = watch_path.join(OFFSET_CACHE_FILENAME);
    let mut log_offsets = load_log_offsets(&offset_cache_path)?;

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
                            log_offsets.insert(path.clone(), reader.stream_position()?);
                            save_log_offsets(&offset_cache_path, &log_offsets)?;
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    #[test]
    fn identifies_json_files_by_extension() {
        assert!(is_json(&PathBuf::from("Status.json")));
        assert!(is_json(&PathBuf::from("nested/Market.json")));
        assert!(!is_json(&PathBuf::from("Status.JSON")));
        assert!(!is_json(&PathBuf::from("Status.json.backup")));
        assert!(!is_json(&PathBuf::from("json")));
    }

    #[test]
    fn identifies_journal_logs_by_filename() {
        assert!(is_journal_log(&PathBuf::from(
            "Journal.2026-10-15T210635.01.log"
        )));
        assert!(is_journal_log(&PathBuf::from("nested/Journal.current.log")));
        assert!(is_journal_log(&PathBuf::from("Journal.log")));
        assert!(!is_journal_log(&PathBuf::from("journal.current.log")));
        assert!(!is_journal_log(&PathBuf::from(
            "Journal.current.log.backup"
        )));
        assert!(!is_journal_log(&PathBuf::from(OFFSET_CACHE_FILENAME)));
    }

    #[test]
    fn saves_and_loads_log_offsets() {
        let test_directory = std::env::temp_dir().join(format!(
            "beacon-offset-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after the Unix epoch")
                .as_nanos()
        ));
        fs::create_dir(&test_directory).expect("test directory should be created");
        let cache_path = test_directory.join(OFFSET_CACHE_FILENAME);

        assert_eq!(
            load_log_offsets(&cache_path).expect("a missing cache should load"),
            HashMap::new()
        );

        let offsets = HashMap::from([
            (PathBuf::from("Journal.log"), 128),
            (PathBuf::from("Journal.current.log"), 4096),
        ]);
        save_log_offsets(&cache_path, &offsets).expect("offsets should be saved");

        assert_eq!(
            load_log_offsets(&cache_path).expect("saved offsets should load"),
            offsets
        );

        fs::remove_dir_all(test_directory).expect("test directory should be removed");
    }

    #[test]
    fn parses_required_arguments_and_default_debounce() {
        let args = BeaconArgs::try_parse_from([
            "beacon",
            "--watch-path",
            "test_logs",
            "--endpoint",
            "localhost:8080",
        ])
        .expect("valid arguments should parse");

        assert_eq!(args.watch_path, PathBuf::from("test_logs"));
        assert_eq!(args.debounce_ms, 50);
        assert_eq!(args.endpoint, "localhost:8080");
        assert!(!args.enable_tls);
        assert!(args.tls_ca_cert.is_empty());
    }

    #[test]
    fn parses_tls_options_and_custom_debounce() {
        let args = BeaconArgs::try_parse_from([
            "beacon",
            "-w",
            "journals",
            "--debounce-ms",
            "250",
            "--endpoint",
            "example.com",
            "--enable-tls",
            "--tls-ca-cert",
            "first.pem",
            "--tls-ca-cert",
            "second.pem",
        ])
        .expect("valid TLS arguments should parse");

        assert_eq!(args.debounce_ms, 250);
        assert!(args.enable_tls);
        assert_eq!(
            args.tls_ca_cert,
            [PathBuf::from("first.pem"), PathBuf::from("second.pem")]
        );
    }

    #[test]
    fn rejects_ca_certificate_without_tls() {
        let error = BeaconArgs::try_parse_from([
            "beacon",
            "-w",
            "journals",
            "--endpoint",
            "example.com",
            "--tls-ca-cert",
            "ca.pem",
        ])
        .expect_err("a CA certificate should require TLS");

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }
}
