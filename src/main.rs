use futures::{
    FutureExt, StreamExt,
    future::{Either, pending},
    select_biased,
};
use futures_executor::block_on;
use futures_timer::Delay;
use notify::{Event, RecursiveMode, Result, Watcher};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::{debug, error, info};

struct BeaconConfig {
    watch_path: PathBuf,
    debounce_ms: u64,
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

async fn run(conf: &BeaconConfig) -> Result<()> {
    let mut pending_files: HashMap<PathBuf, Instant> = HashMap::new();
    let mut log_offsets: HashMap<PathBuf, u64> = HashMap::new();

    let (mut tx, mut rx) = futures::channel::mpsc::channel::<Result<Event>>(20);
    let mut watcher = notify::recommended_watcher(move |result| {
        let _ = tx.start_send(result);
    })?;

    watcher.watch(&conf.watch_path, RecursiveMode::NonRecursive)?;

    let debounce_duration = conf.debounce_ms;
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
                        // send contents to host from config
                        println!("{:?}", contents);
                    } else if is_journal_log(&path) {
                        let mut file = File::open(&path)?;
                        let file_len = file.metadata()?.len();
                        let offset = log_offsets.get(&path).copied().unwrap_or(0);
                        let offset = if offset <= file_len { offset } else { 0 };
                        file.seek(SeekFrom::Start(offset))?;
                        let mut line = String::new();
                        let mut reader = BufReader::new(file);

                        while reader.read_line(&mut line)? > 0 {
                            println!("{:?}", line);
                            line.clear();
                        }
                        log_offsets.insert(path, reader.stream_position()?);
                    }
                }
            }
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let config = BeaconConfig {
        watch_path: PathBuf::from("test_logs"),
        debounce_ms: 50,
    };

    info!("staring watcher");
    block_on(run(&config))?;
    Ok(())
}
