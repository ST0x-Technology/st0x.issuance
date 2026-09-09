//! Pause control for the redemption [`TransferPoller`](super::poller::TransferPoller).
//!
//! Lets an operator HTTP handler quiesce a live poller - the current tick has
//! finished and no new one has started - before it mutates state the poller
//! would otherwise race, then resume it on every exit path.
//!
//! `burn-excess external` is the first user: it must record a funding-Transfer
//! exclusion before the poller reads that log, or the live poller treats the
//! funding transfer as an AP redemption and opens a spurious `Redemption` for
//! shares being burned at the same time. The control is deliberately a reusable
//! primitive for any future operator op that must mutate while the poller is
//! live, not a one-off for burn-excess.

use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

use crate::tokenized_asset::Network;

/// Per-network pause controls, reachable from Rocket state so a handler can
/// quiesce the poller watching a given network's vaults.
pub(crate) struct PollerPauses(HashMap<Network, PollerPauseControl>);

impl PollerPauses {
    pub(crate) const fn new(
        controls: HashMap<Network, PollerPauseControl>,
    ) -> Self {
        Self(controls)
    }

    /// The pause control for `network`'s poller, or `None` when no poller runs
    /// for that network.
    pub(crate) fn control(
        &self,
        network: Network,
    ) -> Option<&PollerPauseControl> {
        self.0.get(&network)
    }
}

/// Controller side of a poller pause, held in Rocket state.
pub(crate) struct PollerPauseControl {
    pause: watch::Sender<bool>,
    parked: watch::Receiver<bool>,
}

impl PollerPauseControl {
    /// Requests a pause and returns only once the poller has parked: the
    /// current tick has finished and no new one has started. The returned guard
    /// resumes the poller when dropped, so a caller cannot forget to resume on
    /// an error or panic path.
    ///
    /// A poller that has already exited (its `parked` sender dropped) leaves
    /// nothing to quiesce, so the wait ends and the guard still resumes any
    /// future poller sharing the channel.
    pub(crate) async fn pause(&self) -> PollerPauseGuard {
        let _ = self.pause.send(true);
        let mut parked = self.parked.clone();
        while !*parked.borrow_and_update() {
            if parked.changed().await.is_err() {
                break;
            }
        }
        PollerPauseGuard { pause: self.pause.clone() }
    }
}

/// Resumes the poller when dropped. Resuming is a plain non-blocking signal, so
/// it runs from `Drop` on the success, error, and panic paths alike.
pub(crate) struct PollerPauseGuard {
    pause: watch::Sender<bool>,
}

impl Drop for PollerPauseGuard {
    fn drop(&mut self) {
        let _ = self.pause.send(false);
    }
}

/// Poller side of a pause. The poller calls [`Self::wait_while_paused`] at the
/// top of each loop iteration (the quiescence point) and
/// [`Self::interruptible_sleep`] between ticks so a pause request does not have
/// to wait out a full idle interval.
pub(crate) struct PollerPause {
    pause: watch::Receiver<bool>,
    parked: watch::Sender<bool>,
}

impl PollerPause {
    /// Parks while paused: signals parked, waits for resume, then signals
    /// running again. Called at the top of the loop, so it is reached only
    /// after any in-flight tick has completed - which is what makes the
    /// controller's [`PollerPauseControl::pause`] confirm true quiescence.
    pub(crate) async fn wait_while_paused(&mut self) {
        if !*self.pause.borrow_and_update() {
            return;
        }
        let _ = self.parked.send(true);
        while *self.pause.borrow_and_update() {
            if self.pause.changed().await.is_err() {
                break;
            }
        }
        let _ = self.parked.send(false);
    }

    /// Sleeps for `interval`, returning early if a pause is requested so the
    /// next [`Self::wait_while_paused`] parks promptly instead of after a full
    /// idle interval.
    pub(crate) async fn interruptible_sleep(&mut self, interval: Duration) {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = self.pause.changed() => {}
        }
    }
}

/// Builds a linked controller/poller pair sharing two watch channels: `pause`
/// (controller to poller) and `parked` (poller to controller).
pub(crate) fn poller_pause() -> (PollerPauseControl, PollerPause) {
    let (pause_tx, pause_rx) = watch::channel(false);
    let (parked_tx, parked_rx) = watch::channel(false);
    (
        PollerPauseControl { pause: pause_tx, parked: parked_rx },
        PollerPause { pause: pause_rx, parked: parked_tx },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    /// A pause must wait for the tick already running to finish, hold off every
    /// new tick while paused, and let the poller run again once resumed.
    #[tokio::test(start_paused = true)]
    async fn pause_quiesces_the_in_flight_tick_and_resume_continues() {
        let (control, mut pause) = poller_pause();
        let ticks = Arc::new(AtomicUsize::new(0));
        let mid_tick = Arc::new(AtomicBool::new(false));

        let loop_ticks = ticks.clone();
        let loop_mid_tick = mid_tick.clone();
        let poller = tokio::spawn(async move {
            loop {
                pause.wait_while_paused().await;
                loop_mid_tick.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(1)).await;
                loop_mid_tick.store(false, Ordering::SeqCst);
                loop_ticks.fetch_add(1, Ordering::SeqCst);
                pause.interruptible_sleep(Duration::from_secs(5)).await;
            }
        });

        // Land inside a running tick (its 1s sleep), then request the pause.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(mid_tick.load(Ordering::SeqCst), "expected a tick in flight");

        let guard = control.pause().await;
        assert!(
            !mid_tick.load(Ordering::SeqCst),
            "pause must wait for the in-flight tick to finish"
        );

        let ticks_when_paused = ticks.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            ticks_when_paused,
            "no ticks may run while paused"
        );

        drop(guard);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            ticks.load(Ordering::SeqCst) > ticks_when_paused,
            "the poller must resume ticking after the guard drops"
        );

        poller.abort();
    }

    /// Pausing a poller that has already exited must not hang: the dropped
    /// `parked` sender ends the wait.
    #[tokio::test]
    async fn pause_of_an_exited_poller_does_not_hang() {
        let (control, pause) = poller_pause();
        drop(pause);
        let _guard = control.pause().await;
    }
}
