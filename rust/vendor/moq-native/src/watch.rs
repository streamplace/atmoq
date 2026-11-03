use std::path::{Path, PathBuf};

use notify::Watcher;
use tokio::sync::mpsc;

/// Watches a set of files and resolves whenever something changes in their
/// directories.
///
/// Reacting to the filesystem (rather than a SIGHUP/SIGUSR1) is what lets
/// cert-manager, Kubernetes secret mounts, and `mv`-into-place rotate files with
/// no extra signalling: they rewrite the file and the watcher fires.
///
/// Watches each file's *parent directory*, not the file itself. Editors,
/// cert-manager, and K8s secret mounts replace files by atomic rename or symlink
/// swap, which changes the inode (and, for the K8s `..data` symlink, fires on the
/// directory without ever naming the file), so a watch set directly on the path
/// would be missed.
pub(crate) struct FileWatcher {
	// Holds the OS watcher alive; dropping it stops events.
	_watcher: notify::RecommendedWatcher,
	events: mpsc::Receiver<()>,
}

impl FileWatcher {
	/// Start watching the parent directories of `paths`. Errors if the OS watcher
	/// can't be created or a directory can't be watched (e.g. the inotify
	/// instance/watch limit is hit). `notify` already falls back to a built-in
	/// poll watcher on platforms without a native backend, so there's no manual
	/// polling here.
	pub(crate) fn new(paths: &[PathBuf]) -> notify::Result<Self> {
		// A capacity-1 channel of unit wakeups coalesces the burst of raw events
		// notify emits per change (and any unrelated churn in the directory): a
		// full buffer already has a pending wakeup, so extra sends are dropped.
		let (tx, rx) = mpsc::channel(1);
		let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
			// Reloading reads these files. Linux reports those reads as access
			// events, so treating them as changes creates a self-triggering loop.
			if matches!(&event, Ok(event) if matches!(&event.kind, notify::EventKind::Access(_))) {
				return;
			}
			let _ = tx.try_send(());
		})?;

		// Watch each distinct parent directory once.
		let mut dirs: Vec<&Path> = paths.iter().filter_map(|p| p.parent()).collect();
		dirs.sort_unstable();
		dirs.dedup();
		for dir in dirs {
			watcher.watch(dir, notify::RecursiveMode::NonRecursive)?;
		}

		Ok(Self {
			_watcher: watcher,
			events: rx,
		})
	}

	/// Resolve once the OS reports activity in a watched directory. The caller
	/// reloads on return; reloads are idempotent, so the coarse "something
	/// changed" granularity at worst costs an occasional redundant reload.
	pub(crate) async fn changed(&mut self) {
		// The sender lives inside `_watcher`, which we hold for `&mut self`, so the
		// channel can't be closed here.
		self.events
			.recv()
			.await
			.expect("file watcher channel closed unexpectedly");
	}
}

#[cfg(test)]
mod tests {
	use super::FileWatcher;
	use std::time::{Duration, SystemTime, UNIX_EPOCH};

	#[tokio::test]
	async fn certificate_reads_do_not_reload_but_rotation_does() {
		let dir = std::env::temp_dir().join(format!(
			"atmoq-cert-watch-{}-{}",
			std::process::id(),
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
		));
		std::fs::create_dir(&dir).unwrap();
		let cert = dir.join("cert.pem");
		std::fs::write(&cert, b"original certificate").unwrap();
		let replacement = dir.join("replacement.pem");
		std::fs::write(&replacement, b"rotated certificate").unwrap();
		let mut watcher = FileWatcher::new(std::slice::from_ref(&cert)).unwrap();

		for _ in 0..3 {
			assert_eq!(std::fs::read(&cert).unwrap(), b"original certificate");
		}
		assert!(
			tokio::time::timeout(Duration::from_millis(300), watcher.changed())
				.await
				.is_err(),
			"reading certificates must not trigger another reload"
		);

		// Secret mounts and editors commonly rotate certificates by rename.
		std::fs::rename(&replacement, &cert).unwrap();
		tokio::time::timeout(Duration::from_secs(3), watcher.changed())
			.await
			.expect("certificate rotation must wake the loader");
		assert_eq!(std::fs::read(&cert).unwrap(), b"rotated certificate");
		drop(watcher);
		std::fs::remove_dir_all(dir).unwrap();
	}
}
