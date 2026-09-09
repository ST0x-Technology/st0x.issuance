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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard, watch};

use crate::tokenized_asset::Network;

/// How long [`PollerPauseControl::pause`] waits for the poller to confirm it
/// parked before giving up. The park point is the top of the poll loop, so a
/// pause waits out at most the current `poll_once` pass (the inter-tick sleeps
/// yield to a pause at once). In steady state - when a breakglass burn is run -
/// a pass is one block-number read plus a small `eth_getLogs` per vault, well
/// under this window. A poller that has not parked within it is stuck or
/// mid-catch-up, so the pause is refused with `PollerNotParked` rather than
/// blocking the handler indefinitely.
const POLLER_PARK_TIMEOUT: Duration = Duration::from_secs(30);

/// The poller did not confirm it parked within [`POLLER_PARK_TIMEOUT`], so the
/// caller must not mutate state the live poller would race.
#[derive(Debug, thiserror::Error)]
#[error("the transfer poller did not confirm it parked")]
pub(crate) struct PollerNotParked;

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
    /// Serializes concurrent pausers so one guard's resume cannot free the
    /// poller while another pauser is still mutating; held for the guard's
    /// lifetime.
    serialize: Arc<Mutex<()>>,
}

impl PollerPauseControl {
    /// Requests a pause and returns only once the poller has parked: the
    /// current tick has finished and no new one has started. The returned guard
    /// resumes the poller when dropped, so a caller cannot forget to resume on
    /// an error or panic path.
    ///
    /// Pausers are serialized: a second caller waits until the first guard
    /// drops (and the poller resumes) before it pauses, so overlapping
    /// breakglass ops cannot resume the poller out from under one another.
    ///
    /// Returns [`PollerNotParked`] when the poller does not confirm it parked
    /// within [`POLLER_PARK_TIMEOUT`] (including a poller that has exited),
    /// leaving the poller running rather than blocking the caller forever.
    pub(crate) async fn pause(
        &self,
    ) -> Result<PollerPauseGuard, PollerNotParked> {
        let permit = Arc::clone(&self.serialize).lock_owned().await;

        let _ = self.pause.send(true);
        let mut parked = self.parked.clone();
        let confirmed = tokio::time::timeout(POLLER_PARK_TIMEOUT, async {
            while !*parked.borrow_and_update() {
                parked.changed().await.map_err(|_| ())?;
            }
            Ok::<(), ()>(())
        })
        .await;

        if matches!(confirmed, Ok(Ok(()))) {
            Ok(PollerPauseGuard { pause: self.pause.clone(), _permit: permit })
        } else {
            // Not confirmed parked: undo the pause request so the poller is not
            // left stopped without a guard, then report failure. The permit
            // drops here, freeing the next pauser.
            let _ = self.pause.send(false);
            Err(PollerNotParked)
        }
    }
}

/// Resumes the poller when dropped. Resuming is a plain non-blocking signal, so
/// it runs from `Drop` on the success, error, and panic paths alike.
pub(crate) struct PollerPauseGuard {
    pause: watch::Sender<bool>,
    /// Held so the serialize mutex is released (unblocking the next pauser)
    /// only after this guard drops and the poller resumes.
    _permit: OwnedMutexGuard<()>,
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
        PollerPauseControl {
            pause: pause_tx,
            parked: parked_rx,
            serialize: Arc::new(Mutex::new(())),
        },
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

        let guard = control.pause().await.unwrap();
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

    /// A poller that has already exited cannot confirm it parked, so pause
    /// reports `PollerNotParked` promptly instead of hanging.
    #[tokio::test(start_paused = true)]
    async fn pause_of_an_exited_poller_reports_not_parked() {
        let (control, pause) = poller_pause();
        drop(pause);
        assert!(control.pause().await.is_err());
    }

    /// A poller that never parks must not stall the caller forever: pause gives
    /// up after `POLLER_PARK_TIMEOUT` and reports `PollerNotParked`.
    #[tokio::test(start_paused = true)]
    async fn pause_times_out_when_the_poller_never_parks() {
        // `_pause` is held but its loop is never run, so it never signals
        // parked; the wait must elapse rather than block forever.
        let (control, _pause) = poller_pause();
        assert!(control.pause().await.is_err());
    }

    /// Pausers serialize: while one guard is held a second pause cannot acquire;
    /// it completes only once the first guard drops and the poller has resumed
    /// and re-parked.
    #[tokio::test(start_paused = true)]
    async fn a_second_pause_waits_for_the_first_guard_to_drop() {
        let (control, mut pause) = poller_pause();
        let control = Arc::new(control);
        let ticks = Arc::new(AtomicUsize::new(0));

        let loop_ticks = ticks.clone();
        let poller = tokio::spawn(async move {
            loop {
                pause.wait_while_paused().await;
                loop_ticks.fetch_add(1, Ordering::SeqCst);
                pause.interruptible_sleep(Duration::from_secs(5)).await;
            }
        });

        let first = control.pause().await.unwrap();

        let second_control = Arc::clone(&control);
        let second =
            tokio::spawn(async move { second_control.pause().await.is_ok() });

        // No amount of time frees the second pause while the first guard holds
        // the serialize permit.
        tokio::time::sleep(Duration::from_secs(300)).await;
        assert!(
            !second.is_finished(),
            "a second pause must block until the first guard drops"
        );

        let ticks_before_resume = ticks.load(Ordering::SeqCst);
        drop(first);

        assert!(
            second.await.unwrap(),
            "the second pause proceeds once the first guard drops"
        );
        assert!(
            ticks.load(Ordering::SeqCst) > ticks_before_resume,
            "the poller resumed and ticked between the two pauses"
        );

        poller.abort();
    }
}
