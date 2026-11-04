//! Wait for kernel readiness instead of periodically reading directory events.
use super::*;
use crate::plugin::common::EventStream;
use futures_util::stream;
use tokio::io::unix::AsyncFd;

pub(crate) async fn subscribe(
    state: Arc<State>,
    owner: usize,
    args: Value,
) -> Result<EventStream, Error> {
    let permit = state
        .services
        .require::<crate::projects::ProjectService>()?
        .watch_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::too_many("PROJECT_WATCH_LIMIT"))?;
    let snapshot = state.clone();
    let request = args.clone();
    let (watch, token) = tokio::task::spawn_blocking(move || {
        let project = authorized(&snapshot, owner, &request)?;
        let directory = match request.get("directory") {
            Some(value) => Some(value.as_str().ok_or_else(Error::invalid)?),
            None => None,
        };
        create(&snapshot, &project, directory)
    })
    .await
    .map_err(|_| Error::internal())??;
    let fd = AsyncFd::new(rustix::io::dup(&watch.fd)?).map_err(Error::from)?;
    let watch = Arc::new(std::sync::Mutex::new(watch));
    let feed = Feed {
        state,
        owner,
        args,
        watch,
        fd,
        token,
        _permit: permit,
        first: true,
        generation: 0,
    };
    Ok(Box::pin(stream::unfold(feed, |mut feed| async move {
        if !feed.first {
            loop {
                tokio::select! {
                    ready = feed.fd.readable() => match ready { Ok(mut guard) => { guard.clear_ready(); break; }, Err(_) => return None },
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {
                        if let Err(error) = check(&feed).await { return Some((Err(error), feed)); }
                    }
                }
            }
        }
        feed.first = false;
        let value = page(&feed).await.and_then(|value| {
            rearm(&mut feed)?;
            Ok(value)
        });
        Some((value, feed))
    })))
}
struct Feed {
    state: Arc<State>,
    owner: usize,
    args: Value,
    watch: Arc<std::sync::Mutex<Watch>>,
    fd: AsyncFd<OwnedFd>,
    token: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
    first: bool,
    generation: u64,
}
fn rearm(feed: &mut Feed) -> Result<(), Error> {
    let watch = feed.watch.lock().map_err(|_| Error::internal())?;
    if feed.generation != watch.generation {
        feed.fd = AsyncFd::new(rustix::io::dup(&watch.fd)?).map_err(Error::from)?;
        feed.generation = watch.generation;
    }
    Ok(())
}
async fn check(feed: &Feed) -> Result<(), Error> {
    let (state, args, owner) = (feed.state.clone(), feed.args.clone(), feed.owner);
    tokio::task::spawn_blocking(move || authorized(&state, owner, &args).map(|_| ()))
        .await
        .map_err(|_| Error::internal())?
}
async fn page(feed: &Feed) -> Result<Value, Error> {
    let (state, args, watch, owner, token) = (
        feed.state.clone(),
        feed.args.clone(),
        feed.watch.clone(),
        feed.owner,
        feed.token.clone(),
    );
    tokio::task::spawn_blocking(move || {
        let project = authorized(&state, owner, &args)?;
        let mut watch = watch.lock().map_err(|_| Error::internal())?;
        tree::verify(&state, &project, &watch.roots)?;
        let gap = poll(&mut watch)?;
        tree::verify(&state, &project, &watch.roots)?;
        Ok(json!({"version":format!("{token}:{}",watch.version),"gap":gap,"truncated":watch.truncated}))
    }).await.map_err(|_| Error::internal())?
}
