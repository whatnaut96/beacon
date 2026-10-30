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
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::{debug, error, info};

struct BeaconConfig {
    watch_path: PathBuf,
    debounce_ms: u64,
}

async fn run(conf: &BeaconConfig) -> Result<()> {
    let mut pending_files: HashMap<PathBuf, Instant> = HashMap::new();

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
                        let is_json = path.extension().is_some_and(|extension| extension == "json");
                        let is_journal = path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("Journal.") && name.ends_with(".log"));

                        if is_json || is_journal {
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
                    info!(path=%path.display(), "debounced file");
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
