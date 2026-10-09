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

/// How long [`PollerPauseControl::pause`] waits to pause the poller before
/// giving up: first for any other pauser's guard to drop, then for the poller
/// to confirm it parked. The park point is the top of the poll loop, so a
/// pause waits out at most the current `poll_once` pass (the inter-tick sleeps
/// yield to a pause at once). In steady state - when a breakglass burn is run -
/// a pass is one block-number read plus a small `eth_getLogs` per vault, well
/// under this window. A poller that has not parked within it is stuck or
/// mid-catch-up, and a poller another breakglass run still holds is busy, so
/// the pause is refused with a [`PollerPauseRefused`] rather than blocking or
/// queueing the handler without bound.
const POLLER_PARK_TIMEOUT: Duration = Duration::from_secs(30);

/// The least of the window a pause must still have once it holds the poller
/// before a park timeout is blamed on the poller rather than on the handoff
/// from another pauser: half the window, far above a healthy pass.
const MIN_PARK_BUDGET: Duration = Duration::from_secs(15);

/// Why the poller could not be paused within [`POLLER_PARK_TIMEOUT`]; the
/// caller must not mutate state the live poller would race. The causes differ
/// in what an operator does next, so they stay distinct in the log.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PollerPauseRefused {
    /// Another breakglass operation held the poller for all or most of the
    /// window (released too late for even a healthy pass to re-park): routine
    /// contention, retry once it finishes.
    #[error(
        "another breakglass operation held the transfer poller for (most of) \
         the window; retry once it finishes"
    )]
    Busy,
    /// The poller did not confirm it parked: it is stuck or mid-catch-up, so
    /// redemption detection on that network is itself in trouble.
    #[error("the transfer poller did not confirm it parked; it may be stuck")]
    NotParked,
    /// The poller task has exited, so redemptions on that network are not
    /// being detected at all.
    #[error("the transfer poller has exited")]
    Exited,
}

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
    /// lifetime. Holds whether the last pauser to hold it got the poller to
    /// park, so a pauser that waited behind it can tell contention from a
    /// stuck poller.
    serialize: Arc<Mutex<bool>>,
}

impl PollerPauseControl {
    /// Requests a pause and returns only once the poller has parked: the
    /// current tick has finished and no new one has started. The returned guard
    /// resumes the poller when dropped, so a caller cannot forget to resume on
    /// an error or panic path.
    ///
    /// Pausers are serialized: a second caller waits for the first guard to
    /// drop (and the poller to resume) before it pauses, so overlapping
    /// breakglass ops cannot resume the poller out from under one another. That
    /// wait counts against the same window, so a second pauser is refused
    /// rather than queued without bound behind a long first run.
    ///
    /// Returns [`PollerPauseRefused`] when the poller cannot be paused within
    /// [`POLLER_PARK_TIMEOUT`] (another guard still held, a poller that never
    /// parks, or one that has exited), leaving the poller as it was rather than
    /// blocking the caller forever.
    pub(crate) async fn pause(
        &self,
    ) -> Result<PollerPauseGuard, PollerPauseRefused> {
        // One deadline covers the wait for another pauser's guard and the
        // park itself, timed separately only so a refusal says which it was.
        let deadline = tokio::time::Instant::now()
            .checked_add(POLLER_PARK_TIMEOUT)
            .ok_or(PollerPauseRefused::NotParked)?;
        let mut permit =
            if let Ok(permit) = Arc::clone(&self.serialize).try_lock_owned() {
                permit
            } else {
                tokio::time::timeout_at(
                    deadline,
                    Arc::clone(&self.serialize).lock_owned(),
                )
                .await
                .map_err(|_| PollerPauseRefused::Busy)?
            };
        let previous_holder_parked = *permit;
        *permit = false;
        let park_budget =
            deadline.saturating_duration_since(tokio::time::Instant::now());

        let mut parked = self.parked.clone();
        let pause = self.pause.clone();
        let confirmed = tokio::time::timeout_at(deadline, async move {
            // The previous guard's resume may not have reached the poller
            // yet, so `parked` can still hold that request's stale `true`.
            // Wait for the poller to report itself running before asking
            // again: with pausers serialized, the `true` awaited below can
            // then only be the acknowledgement of THIS request. Accepting a
            // stale one would hand out a guard while the poller goes on to
            // run a tick - the exact race the pause exists to prevent.
            while *parked.borrow_and_update() {
                parked
                    .changed()
                    .await
                    .map_err(|_| PollerPauseRefused::Exited)?;
            }

            pause.send(true).map_err(|_| PollerPauseRefused::Exited)?;
            let mut guard = PollerPauseGuard { pause, permit };
            while !*parked.borrow_and_update() {
                parked
                    .changed()
                    .await
                    .map_err(|_| PollerPauseRefused::Exited)?;
            }
            *guard.permit = true;
            Ok(guard)
        })
        .await;

        // A pause that got the poller back from another guard with less than
        // `MIN_PARK_BUDGET` left may not leave even a healthy pass time to
        // finish and re-park, so that elapse is contention. With at least that
        // much of the window, or when the pauser it waited behind never got
        // the poller to park either, a poller that still did not park is
        // stuck.
        confirmed.unwrap_or(Err(
            if park_budget < MIN_PARK_BUDGET && previous_holder_parked {
                PollerPauseRefused::Busy
            } else {
                PollerPauseRefused::NotParked
            },
        ))
    }

    /// Test hook: a receiver on the poller's parked signal. The poller writes
    /// it only on a real park, so a receiver whose `has_changed()` stays false
    /// proves no pause ever reached the poller, not even one resumed before
    /// the test looked.
    #[cfg(test)]
    pub(crate) fn parked_signal(&self) -> watch::Receiver<bool> {
        self.parked.clone()
    }
}

/// Resumes the poller when dropped. Resuming is a plain non-blocking signal, so
/// it runs from `Drop` on the success, error, and panic paths alike.
pub(crate) struct PollerPauseGuard {
    pause: watch::Sender<bool>,
    /// Held so the serialize mutex is released (unblocking the next pauser)
    /// only after this guard drops and the poller resumes; set once the
    /// poller confirmed it parked for this guard.
    permit: OwnedMutexGuard<bool>,
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

    /// Sleeps for `interval`, returning early only if a pause is requested so
    /// the next [`Self::wait_while_paused`] parks promptly instead of after a
    /// full idle interval. Waits for the value `true` rather than any change:
    /// a pause that was requested and withdrawn during the previous tick (a
    /// pauser that timed out waiting for the park) leaves an unseen change
    /// behind, and reacting to it would cut a retry backoff short and re-poll
    /// a failing RPC at once.
    pub(crate) async fn interruptible_sleep(&mut self, interval: Duration) {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = self.pause.wait_for(|paused| *paused) => {}
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
            serialize: Arc::new(Mutex::new(true)),
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
    /// reports `Exited` promptly instead of hanging.
    #[tokio::test(start_paused = true)]
    async fn pause_of_an_exited_poller_reports_exited() {
        let (control, pause) = poller_pause();
        drop(pause);
        assert!(matches!(
            control.pause().await,
            Err(PollerPauseRefused::Exited)
        ));
    }

    /// A poller that never parks must not stall the caller forever: pause gives
    /// up after `POLLER_PARK_TIMEOUT` and reports `NotParked`.
    #[tokio::test(start_paused = true)]
    async fn pause_times_out_when_the_poller_never_parks() {
        // `_pause` is held but its loop is never run, so it never signals
        // parked; the wait must elapse rather than block forever.
        let (control, _pause) = poller_pause();
        assert!(matches!(
            control.pause().await,
            Err(PollerPauseRefused::NotParked)
        ));
    }

    /// Cancelling a pause after its request is sent must withdraw that request,
    /// or the poller would park forever without a guard that can resume it.
    #[tokio::test(start_paused = true)]
    async fn cancelling_pause_while_waiting_for_park_resumes_the_poller() {
        let (control, mut pause) = poller_pause();
        {
            let pause_request = control.pause();
            tokio::pin!(pause_request);
            tokio::select! {
                changed = pause.pause.changed() => changed.unwrap(),
                result = &mut pause_request => panic!(
                    "pause unexpectedly completed before cancellation: {}",
                    result.is_ok()
                ),
            }
            assert!(
                *pause.pause.borrow_and_update(),
                "pause must be requested"
            );
        }

        pause.pause.changed().await.unwrap();
        assert!(
            !*pause.pause.borrow_and_update(),
            "cancelling pause must withdraw the request"
        );
    }

    /// Pausers serialize, and waiting for another pauser's guard counts against
    /// `POLLER_PARK_TIMEOUT`: while one guard is held a second pause is refused
    /// when the window ends rather than queued without bound, and the refusal
    /// leaves the poller paused for the first guard.
    #[tokio::test(start_paused = true)]
    async fn a_second_pause_is_refused_while_the_first_guard_is_held() {
        let (control, mut pause) = poller_pause();
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
        let ticks_under_first = ticks.load(Ordering::SeqCst);
        let started = tokio::time::Instant::now();

        let refused = matches!(
            tokio::time::timeout(POLLER_PARK_TIMEOUT * 2, control.pause())
                .await
                .expect("a second pause must be refused within the window"),
            Err(PollerPauseRefused::Busy)
        );

        assert!(refused, "the second pause must be refused as busy");
        assert_eq!(started.elapsed(), POLLER_PARK_TIMEOUT);
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            ticks_under_first,
            "the refused pause must not resume the poller under the first guard"
        );

        drop(first);
        poller.abort();
    }

    /// A second pause that arrives while the first guard is held proceeds once
    /// that guard drops within the window, after the poller has resumed and
    /// re-parked for it.
    #[tokio::test(start_paused = true)]
    async fn a_second_pause_proceeds_when_the_first_guard_drops_in_time() {
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

        tokio::time::sleep(POLLER_PARK_TIMEOUT / 2).await;
        assert!(
            !second.is_finished(),
            "a second pause waits while the first guard is held"
        );

        let ticks_before_resume = ticks.load(Ordering::SeqCst);
        drop(first);

        assert!(
            second.await.unwrap(),
            "the second pause proceeds once the first guard drops in time"
        );
        assert!(
            ticks.load(Ordering::SeqCst) > ticks_before_resume,
            "the poller resumed and ticked between the two pauses"
        );

        poller.abort();
    }

    /// A second pause that had to wait for the first guard, and then ran out
    /// of window before a healthy poller finished its pass and re-parked, was
    /// refused by contention, not by a stuck poller: it reports `Busy`, so the
    /// operator retries rather than treating redemption detection as broken.
    #[tokio::test(start_paused = true)]
    async fn a_late_handoff_is_refused_as_busy_not_as_a_stuck_poller() {
        let (control, mut pause) = poller_pause();
        let control = Arc::new(control);

        // A healthy poller whose pass takes 10 s.
        let poller = tokio::spawn(async move {
            loop {
                pause.wait_while_paused().await;
                tokio::time::sleep(Duration::from_secs(10)).await;
                pause.interruptible_sleep(Duration::from_secs(5)).await;
            }
        });

        let first = control.pause().await.unwrap();

        let second_control = Arc::clone(&control);
        let second =
            tokio::spawn(async move { second_control.pause().await.err() });

        // Release 5 s before the second pause's window ends: the poller resumes
        // and needs a full 10 s pass before it can park again.
        tokio::time::sleep(
            POLLER_PARK_TIMEOUT.checked_sub(Duration::from_secs(5)).unwrap(),
        )
        .await;
        drop(first);

        assert!(matches!(
            second.await.unwrap(),
            Some(PollerPauseRefused::Busy)
        ));

        poller.abort();
    }

    /// A pause that waited for the first guard only briefly still had most of
    /// the window to itself, so a poller that then never re-parks is reported
    /// stuck (`NotParked`), not hidden as routine contention.
    #[tokio::test(start_paused = true)]
    async fn an_early_handoff_to_a_stuck_poller_is_refused_as_not_parked() {
        let (control, mut pause) = poller_pause();
        let control = Arc::new(control);

        // Parks for the first pause before running any pass, then hangs in
        // the pass it starts once resumed, so it never parks again.
        let poller = tokio::spawn(async move {
            pause.wait_while_paused().await;
            std::future::pending::<()>().await;
        });

        let first = control.pause().await.unwrap();

        let second_control = Arc::clone(&control);
        let second =
            tokio::spawn(async move { second_control.pause().await.err() });

        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(first);

        assert!(matches!(
            second.await.unwrap(),
            Some(PollerPauseRefused::NotParked)
        ));

        poller.abort();
    }

    /// A second pause that waited behind a first one which itself never got
    /// the poller to park was not held up by another breakglass run: the
    /// poller is stuck. It must report `NotParked` even though the handoff
    /// left it less than `MIN_PARK_BUDGET`, or the operator is told to retry
    /// instead of treating redemption detection as broken.
    #[tokio::test(start_paused = true)]
    async fn waiting_behind_a_pause_that_never_parked_reports_not_parked() {
        // Held but never polled, so it never parks for either pause.
        let (control, _pause) = poller_pause();
        let control = Arc::new(control);

        let first_control = Arc::clone(&control);
        let first =
            tokio::spawn(async move { first_control.pause().await.err() });
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Queues behind the first, which only releases the permit when its own
        // window elapses, leaving this one about 5 s.
        let second = control.pause().await.err();

        assert!(matches!(
            first.await.unwrap(),
            Some(PollerPauseRefused::NotParked)
        ));
        assert!(matches!(second, Some(PollerPauseRefused::NotParked)));
    }

    /// A guard's resume may not have reached the poller when the next pauser
    /// arrives, so `parked` can still hold the previous request's stale
    /// `true`. The second pause must not accept that acknowledgement: it has
    /// to let the poller actually resume and re-park for its own request, or
    /// a guard would exist while the poller runs a tick - the race that opens
    /// a spurious redemption. Deterministic on the single-threaded test
    /// runtime: a pause that reuses the stale ack returns without yielding, so
    /// the poller never gets to resume and the tick count stays put.
    #[tokio::test(start_paused = true)]
    async fn a_back_to_back_pause_waits_for_the_poller_to_resume_and_repark() {
        let (control, mut pause) = poller_pause();
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
        let ticks_under_first = ticks.load(Ordering::SeqCst);
        drop(first);

        // Re-pause before the poller has observed the resume: `parked` still
        // reads the first request's `true`.
        let second = control.pause().await.unwrap();
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            ticks_under_first + 1,
            "the second pause must let the poller resume and tick once before \
             taking a fresh park, not reuse the first request's acknowledgement"
        );

        // The poller is genuinely parked for the second guard: no tick while
        // it is held, however long.
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            ticks_under_first + 1,
            "no tick may run while the second guard is held"
        );

        drop(second);
        poller.abort();
    }

    /// A pause that was requested and withdrawn while the poller was mid-tick
    /// (a pauser that timed out waiting for the park) must not cut the next
    /// sleep short: the sleep is the failure backoff, and honouring a stale
    /// resume would re-poll a failing RPC at once.
    #[tokio::test(start_paused = true)]
    async fn a_withdrawn_pause_does_not_cut_the_sleep_short() {
        let (control, mut pause) = poller_pause();
        // The poller never parks (it is mid-tick), so the pause times out and
        // is withdrawn before the poller reaches its sleep.
        assert!(control.pause().await.is_err());

        let started = tokio::time::Instant::now();
        pause.interruptible_sleep(Duration::from_secs(5)).await;
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(5),
            "a pause already withdrawn must leave the full backoff in place"
        );
    }

    /// A live pause request still ends the sleep at once, so the poller parks
    /// promptly rather than after a full idle interval.
    #[tokio::test(start_paused = true)]
    async fn a_pending_pause_ends_the_sleep_at_once() {
        let (control, mut pause) = poller_pause();
        let _ = control.pause.send(true);

        let started = tokio::time::Instant::now();
        pause.interruptible_sleep(Duration::from_secs(5)).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}
