//! Operator notifications for the corporate-actions lifecycle.

mod telegram;

use alloy::primitives::{Address, U256, utils::format_ether};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::jobs::{Job, job_type};
use crate::redemption::IssuerRedemptionRequestId;
use crate::tokenized_asset::{Network, UnderlyingSymbol};

use telegram::TelegramNotifier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum NotificationKind {
    CorporateActionScheduled,
    CorporateActionsSyncFailed,
    FreezeApplied,
    UnfreezeApplied,
    FreezeTransitionFailed,
    RedemptionHeld,
    RedemptionResumed,
    RedemptionResumeFailed,
    LowGasBalance,
}

impl NotificationKind {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CorporateActionScheduled => "corporate_action_scheduled",
            Self::CorporateActionsSyncFailed => "corporate_actions_sync_failed",
            Self::FreezeApplied => "freeze_applied",
            Self::UnfreezeApplied => "unfreeze_applied",
            Self::FreezeTransitionFailed => "freeze_transition_failed",
            Self::RedemptionHeld => "redemption_held",
            Self::RedemptionResumed => "redemption_resumed",
            Self::RedemptionResumeFailed => "redemption_resume_failed",
            Self::LowGasBalance => "low_gas_balance",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum LifecycleNotification {
    CorporateActionScheduled {
        underlying: UnderlyingSymbol,
        ex_date: NaiveDate,
        freeze_at: DateTime<Utc>,
        unfreeze_at: DateTime<Utc>,
    },
    CorporateActionsSyncFailed,
    FreezeApplied {
        underlying: UnderlyingSymbol,
    },
    UnfreezeApplied {
        underlying: UnderlyingSymbol,
    },
    FreezeTransitionFailed {
        underlying: UnderlyingSymbol,
        transition: FreezeTransitionKind,
    },
    RedemptionHeld {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
    },
    RedemptionResumed {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
    },
    RedemptionResumeFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
    },
    LowGasBalance {
        network: Network,
        wallet: Address,
        balance: U256,
        threshold: U256,
    },
}

impl LifecycleNotification {
    #[must_use]
    pub(crate) const fn kind(&self) -> NotificationKind {
        match self {
            Self::CorporateActionScheduled { .. } => {
                NotificationKind::CorporateActionScheduled
            }
            Self::CorporateActionsSyncFailed => {
                NotificationKind::CorporateActionsSyncFailed
            }
            Self::FreezeApplied { .. } => NotificationKind::FreezeApplied,
            Self::UnfreezeApplied { .. } => NotificationKind::UnfreezeApplied,
            Self::FreezeTransitionFailed { .. } => {
                NotificationKind::FreezeTransitionFailed
            }
            Self::RedemptionHeld { .. } => NotificationKind::RedemptionHeld,
            Self::RedemptionResumed { .. } => {
                NotificationKind::RedemptionResumed
            }
            Self::RedemptionResumeFailed { .. } => {
                NotificationKind::RedemptionResumeFailed
            }
            Self::LowGasBalance { .. } => NotificationKind::LowGasBalance,
        }
    }

    #[must_use]
    pub(crate) fn message(&self) -> String {
        match self {
            Self::CorporateActionScheduled {
                underlying,
                ex_date,
                freeze_at,
                unfreeze_at,
            } => format!(
                "Corporate action scheduled: {underlying} ex-date {ex_date}; freeze {}; unfreeze {}",
                freeze_at.to_rfc3339(),
                unfreeze_at.to_rfc3339()
            ),
            Self::CorporateActionsSyncFailed => {
                "Corporate-actions sync failed; check structured logs"
                    .to_owned()
            }
            Self::FreezeApplied { underlying } => {
                format!("Freeze applied: {underlying}")
            }
            Self::UnfreezeApplied { underlying } => {
                format!("Unfreeze applied: {underlying}")
            }
            Self::FreezeTransitionFailed { underlying, transition } => {
                let transition = match transition {
                    FreezeTransitionKind::Freeze => "Freeze",
                    FreezeTransitionKind::Unfreeze => "Unfreeze",
                };
                format!(
                    "{transition} failed: {underlying}; check structured logs"
                )
            }
            Self::RedemptionHeld { issuer_request_id, underlying } => format!(
                "Redemption held: {issuer_request_id} {underlying}; funds parked until unfreeze"
            ),
            Self::RedemptionResumed { issuer_request_id, underlying } => {
                format!("Redemption resumed: {issuer_request_id} {underlying}")
            }
            Self::RedemptionResumeFailed { issuer_request_id, underlying } => {
                format!(
                    "Redemption resume failed: {issuer_request_id} {underlying}; check structured logs"
                )
            }
            Self::LowGasBalance { network, wallet, balance, threshold } => {
                let currency = network.native_currency();
                format!(
                    "Low gas on {network}: issuer wallet {wallet} holds {} \
                     {currency} (threshold {} {currency})",
                    format_ether(*balance),
                    format_ether(*threshold)
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) enum FreezeTransitionKind {
    Freeze,
    Unfreeze,
}

#[derive(Debug, thiserror::Error)]
#[error("lifecycle notification delivery failed")]
pub(crate) struct LifecycleNotificationError {
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl LifecycleNotificationError {
    fn new(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self { source: Box::new(source) }
    }
}

#[async_trait]
pub(crate) trait LifecycleNotifier: Send + Sync {
    async fn deliver(
        &self,
        notification: &LifecycleNotification,
    ) -> Result<(), LifecycleNotificationError>;

    async fn notify(&self, notification: &LifecycleNotification) {
        if let Err(error) = self.deliver(notification).await {
            tracing::error!(
                target: "notifications",
                event = "notification_delivery_failed",
                notification_kind = notification.kind().as_str(),
                error = %error,
                cause = ?std::error::Error::source(&error),
                "notification_delivery_failed"
            );
        }
    }
}

pub(crate) struct NoopLifecycleNotifier;

#[async_trait]
impl LifecycleNotifier for NoopLifecycleNotifier {
    async fn deliver(
        &self,
        _notification: &LifecycleNotification,
    ) -> Result<(), LifecycleNotificationError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SendLifecycleNotification {
    pub(crate) notification: LifecycleNotification,
}

pub(crate) struct LifecycleNotificationJobCtx {
    pub(crate) notifier: Arc<dyn LifecycleNotifier>,
}

impl Job<LifecycleNotificationJobCtx> for SendLifecycleNotification {
    type Output = ();
    type Error = LifecycleNotificationError;

    async fn perform(
        &self,
        ctx: &LifecycleNotificationJobCtx,
    ) -> Result<Self::Output, Self::Error> {
        ctx.notifier.deliver(&self.notification).await
    }
}

/// Releases one notification idempotency key only when its prior delivery can
/// no longer retry. Completed deliveries remain durable so recurring producers
/// do not send the same notification again.
pub(crate) async fn release_dead_lifecycle_notification_job(
    pool: &Pool<Sqlite>,
    idempotency_key: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND idempotency_key = ?
            AND (
                status = 'Killed'
                OR (status = 'Failed' AND max_attempts <= attempts)
            )
        ",
    )
    .bind(job_type::<SendLifecycleNotification>())
    .bind(idempotency_key)
    .execute(pool)
    .await?;

    Ok(())
}

/// Requeues lifecycle notifications left running by a previous process.
pub(crate) async fn reset_orphaned_lifecycle_notification_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE Jobs
        SET
            status = 'Pending',
            lock_at = NULL,
            lock_by = NULL
        WHERE
            job_type = ?
            AND status = 'Running'
        ",
    )
    .bind(job_type::<SendLifecycleNotification>())
    .execute(pool)
    .await?;

    Ok(())
}

/// Removes dead lifecycle-notification jobs while preserving retryable rows and
/// successful `Done` rows. Successful rows retain their idempotency keys so a
/// recurring producer cannot redeliver the same notification after restart.
pub(crate) async fn vacuum_terminal_lifecycle_notification_jobs(
    pool: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM Jobs
        WHERE
            job_type = ?
            AND (
                status = 'Killed'
                OR (status = 'Failed' AND max_attempts <= attempts)
            )
        ",
    )
    .bind(job_type::<SendLifecycleNotification>())
    .execute(pool)
    .await?;

    Ok(())
}

/// Optional lifecycle-notification delivery configuration.
#[derive(Clone)]
#[non_exhaustive]
pub struct LifecycleNotificationsConfig {
    delivery: NotificationDelivery,
}

#[derive(Clone)]
enum NotificationDelivery {
    Disabled,
    Telegram(TelegramSettings),
}

#[derive(Clone)]
struct TelegramSettings {
    bot_token: BotToken,
    chat_id: i64,
    message_thread_id: Option<i64>,
}

#[derive(Clone, PartialEq, Eq)]
struct BotToken(String);

impl Debug for BotToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl Debug for LifecycleNotificationsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.delivery {
            NotificationDelivery::Disabled => f
                .debug_struct("LifecycleNotificationsConfig")
                .field("delivery", &"disabled")
                .finish(),
            NotificationDelivery::Telegram(settings) => f
                .debug_struct("LifecycleNotificationsConfig")
                .field("delivery", &"telegram")
                .field("bot_token", &settings.bot_token)
                .field("chat_id", &settings.chat_id)
                .field("message_thread_id", &settings.message_thread_id)
                .finish(),
        }
    }
}

impl Default for LifecycleNotificationsConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

impl LifecycleNotificationsConfig {
    /// Disables lifecycle-notification delivery while preserving the
    /// financial workflow behavior that emits notification intents.
    #[must_use]
    pub const fn disabled() -> Self {
        Self { delivery: NotificationDelivery::Disabled }
    }

    /// Builds an enabled Telegram configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the token or destination is invalid.
    pub fn telegram(
        bot_token: impl Into<String>,
        chat_id: i64,
        message_thread_id: Option<i64>,
    ) -> Result<Self, LifecycleNotificationsConfigError> {
        Self::assemble(Some(bot_token.into()), Some(chat_id), message_thread_id)
    }

    pub(crate) fn assemble(
        bot_token: Option<String>,
        chat_id: Option<i64>,
        message_thread_id: Option<i64>,
    ) -> Result<Self, LifecycleNotificationsConfigError> {
        let (bot_token, chat_id) = match (bot_token, chat_id) {
            (None, None) if message_thread_id.is_none() => {
                return Ok(Self::disabled());
            }
            (None, None) => {
                return Err(
                    LifecycleNotificationsConfigError::ThreadWithoutChannel,
                );
            }
            (Some(_), None) => {
                return Err(LifecycleNotificationsConfigError::MissingChatId);
            }
            (None, Some(_)) => {
                return Err(LifecycleNotificationsConfigError::MissingBotToken);
            }
            (Some(bot_token), Some(chat_id)) => (bot_token, chat_id),
        };

        let bot_token = bot_token.trim();
        if bot_token.is_empty() {
            return Err(LifecycleNotificationsConfigError::BlankBotToken);
        }
        if chat_id == 0 {
            return Err(LifecycleNotificationsConfigError::ZeroChatId);
        }
        if message_thread_id.is_some_and(|thread_id| thread_id <= 0) {
            return Err(
                LifecycleNotificationsConfigError::InvalidMessageThreadId,
            );
        }

        Ok(Self {
            delivery: NotificationDelivery::Telegram(TelegramSettings {
                bot_token: BotToken(bot_token.to_owned()),
                chat_id,
                message_thread_id,
            }),
        })
    }

    /// Builds the configured notification delivery capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the Telegram HTTP client cannot be constructed.
    pub(crate) fn build_notifier(
        &self,
    ) -> Result<Arc<dyn LifecycleNotifier>, NotificationBuildError> {
        match &self.delivery {
            NotificationDelivery::Disabled => {
                Ok(Arc::new(NoopLifecycleNotifier))
            }
            NotificationDelivery::Telegram(settings) => {
                Ok(Arc::new(TelegramNotifier::new(
                    &settings.bot_token.0,
                    settings.chat_id,
                    settings.message_thread_id,
                )?))
            }
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_enabled(&self) -> bool {
        matches!(self.delivery, NotificationDelivery::Telegram(_))
    }
}

/// Invalid lifecycle-notification destination configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleNotificationsConfigError {
    /// A bot token was provided without a destination chat.
    #[error("Telegram bot token is present but chat id is missing")]
    MissingChatId,
    /// A destination chat was provided without a bot token.
    #[error("Telegram chat id is present but bot token is missing")]
    MissingBotToken,
    /// A message thread was configured without its Telegram channel.
    #[error("Telegram message thread id requires bot token and chat id")]
    ThreadWithoutChannel,
    /// The configured bot token contains no non-whitespace characters.
    #[error("Telegram bot token must not be blank")]
    BlankBotToken,
    /// Telegram chat ID zero is not a valid destination.
    #[error("Telegram chat id must not be zero")]
    ZeroChatId,
    /// Telegram message-thread IDs must be positive.
    #[error("Telegram message thread id must be positive")]
    InvalidMessageThreadId,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum NotificationBuildError {
    #[error("failed to build Telegram notification client")]
    TelegramClient(#[source] reqwest::Error),
}

#[cfg(test)]
pub(crate) use test_support::CapturingLifecycleNotifier;

#[cfg(test)]
mod test_support {
    use super::{LifecycleNotification, LifecycleNotifier};
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    pub(crate) struct CapturingLifecycleNotifier {
        captured: Mutex<Vec<LifecycleNotification>>,
    }

    impl CapturingLifecycleNotifier {
        pub(crate) fn notifications(&self) -> Vec<LifecycleNotification> {
            self.captured.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LifecycleNotifier for CapturingLifecycleNotifier {
        async fn deliver(
            &self,
            notification: &LifecycleNotification,
        ) -> Result<(), super::LifecycleNotificationError> {
            self.captured.lock().unwrap().push(notification.clone());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, utils::parse_ether};
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::jobs::JobQueue;
    use crate::mint::test_utils::TestHarness;
    use crate::test_utils::logs_contain_at;

    struct FailingLifecycleNotifier;

    #[async_trait]
    impl LifecycleNotifier for FailingLifecycleNotifier {
        async fn deliver(
            &self,
            _notification: &LifecycleNotification,
        ) -> Result<(), LifecycleNotificationError> {
            Err(LifecycleNotificationError::new(std::io::Error::other(
                "delivery unavailable",
            )))
        }
    }

    fn underlying() -> UnderlyingSymbol {
        UnderlyingSymbol::new("AAPL").unwrap()
    }

    fn redemption_id() -> IssuerRedemptionRequestId {
        IssuerRedemptionRequestId::new(b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ))
    }

    #[test]
    fn message_contract_is_typed_and_contains_no_amounts() {
        let freeze_at = Utc.with_ymd_and_hms(2026, 8, 14, 0, 0, 0).unwrap();
        let unfreeze_at = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
        let ex_date = freeze_at.date_naive();

        let cases = [
            (
                LifecycleNotification::CorporateActionScheduled {
                    underlying: underlying(),
                    ex_date,
                    freeze_at,
                    unfreeze_at,
                },
                "Corporate action scheduled: AAPL ex-date 2026-08-14; freeze 2026-08-14T00:00:00+00:00; unfreeze 2026-08-15T00:00:00+00:00",
            ),
            (
                LifecycleNotification::CorporateActionsSyncFailed,
                "Corporate-actions sync failed; check structured logs",
            ),
            (
                LifecycleNotification::FreezeApplied {
                    underlying: underlying(),
                },
                "Freeze applied: AAPL",
            ),
            (
                LifecycleNotification::UnfreezeApplied {
                    underlying: underlying(),
                },
                "Unfreeze applied: AAPL",
            ),
            (
                LifecycleNotification::FreezeTransitionFailed {
                    underlying: underlying(),
                    transition: FreezeTransitionKind::Freeze,
                },
                "Freeze failed: AAPL; check structured logs",
            ),
            (
                LifecycleNotification::FreezeTransitionFailed {
                    underlying: underlying(),
                    transition: FreezeTransitionKind::Unfreeze,
                },
                "Unfreeze failed: AAPL; check structured logs",
            ),
            (
                LifecycleNotification::RedemptionHeld {
                    issuer_request_id: redemption_id(),
                    underlying: underlying(),
                },
                "Redemption held: 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AAPL; funds parked until unfreeze",
            ),
            (
                LifecycleNotification::RedemptionResumed {
                    issuer_request_id: redemption_id(),
                    underlying: underlying(),
                },
                "Redemption resumed: 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AAPL",
            ),
            (
                LifecycleNotification::RedemptionResumeFailed {
                    issuer_request_id: redemption_id(),
                    underlying: underlying(),
                },
                "Redemption resume failed: 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa AAPL; check structured logs",
            ),
        ];

        for (notification, expected) in cases {
            assert_eq!(notification.message(), expected);
            assert!(!notification.message().contains("quantity"));
            assert!(!notification.message().contains("balance"));
        }
    }

    #[test]
    fn low_gas_message_uses_the_network_native_currency() {
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let base = LifecycleNotification::LowGasBalance {
            network: Network::Base,
            wallet,
            balance: parse_ether("0.05").unwrap(),
            threshold: parse_ether("0.10").unwrap(),
        };
        assert_eq!(
            base.message(),
            "Low gas on base: issuer wallet \
             0xABcdEFABcdEFabcdEfAbCdefabcdeFABcDEFabCD holds \
             0.050000000000000000 ETH (threshold 0.100000000000000000 ETH)"
        );

        let hyperevm = LifecycleNotification::LowGasBalance {
            network: Network::HyperEvm,
            wallet,
            balance: parse_ether("1").unwrap(),
            threshold: parse_ether("2").unwrap(),
        };
        assert_eq!(
            hyperevm.message(),
            "Low gas on hyperevm: issuer wallet \
             0xABcdEFABcdEFabcdEfAbCdefabcdeFABcDEFabCD holds \
             1.000000000000000000 HYPE (threshold 2.000000000000000000 HYPE)"
        );
    }

    #[test]
    fn configuration_is_all_or_none_and_validated_at_the_boundary() {
        assert!(
            !LifecycleNotificationsConfig::assemble(None, None, None)
                .unwrap()
                .is_enabled()
        );
        LifecycleNotificationsConfig::disabled().build_notifier().unwrap();
        assert!(
            LifecycleNotificationsConfig::assemble(
                Some("123:abc".to_owned()),
                Some(-1_001_234_567_890),
                Some(42),
            )
            .unwrap()
            .is_enabled()
        );
        LifecycleNotificationsConfig::telegram("123:abc", 42, None)
            .unwrap()
            .build_notifier()
            .unwrap();
        assert_eq!(
            LifecycleNotificationsConfig::assemble(
                Some("123:abc".to_owned()),
                None,
                None,
            )
            .unwrap_err(),
            LifecycleNotificationsConfigError::MissingChatId
        );
        assert_eq!(
            LifecycleNotificationsConfig::assemble(None, Some(42), None)
                .unwrap_err(),
            LifecycleNotificationsConfigError::MissingBotToken
        );
        assert_eq!(
            LifecycleNotificationsConfig::assemble(None, None, Some(42))
                .unwrap_err(),
            LifecycleNotificationsConfigError::ThreadWithoutChannel
        );
        assert_eq!(
            LifecycleNotificationsConfig::telegram("   ", 42, None)
                .unwrap_err(),
            LifecycleNotificationsConfigError::BlankBotToken
        );
        assert_eq!(
            LifecycleNotificationsConfig::telegram("123:abc", 0, None)
                .unwrap_err(),
            LifecycleNotificationsConfigError::ZeroChatId
        );
        assert_eq!(
            LifecycleNotificationsConfig::telegram("123:abc", 42, Some(0))
                .unwrap_err(),
            LifecycleNotificationsConfigError::InvalidMessageThreadId
        );
    }

    #[test]
    fn debug_output_redacts_the_bot_token() {
        let config = LifecycleNotificationsConfig {
            delivery: NotificationDelivery::Telegram(TelegramSettings {
                bot_token: BotToken("secret-token-marker".to_owned()),
                chat_id: 42,
                message_thread_id: Some(7),
            }),
        };

        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-token-marker"));
    }

    #[tokio::test]
    async fn capturing_notifier_records_typed_notifications() {
        let notifier = CapturingLifecycleNotifier::default();
        let notification =
            LifecycleNotification::FreezeApplied { underlying: underlying() };

        notifier.notify(&notification).await;

        assert_eq!(notifier.notifications(), vec![notification]);
    }

    #[traced_test]
    #[tokio::test]
    async fn notify_records_delivery_failure_without_propagating_it() {
        let notification =
            LifecycleNotification::FreezeApplied { underlying: underlying() };

        FailingLifecycleNotifier.notify(&notification).await;

        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "notification_delivery_failed",
                "freeze_applied",
                "delivery unavailable"
            ]
        ));
    }

    #[tokio::test]
    async fn durable_job_delivers_its_typed_notification() {
        let notifier = Arc::new(CapturingLifecycleNotifier::default());
        let notification =
            LifecycleNotification::FreezeApplied { underlying: underlying() };
        let job =
            SendLifecycleNotification { notification: notification.clone() };
        let ctx = LifecycleNotificationJobCtx { notifier: notifier.clone() };

        job.perform(&ctx).await.unwrap();

        assert_eq!(notifier.notifications(), vec![notification]);
    }

    #[tokio::test]
    async fn startup_maintenance_requeues_orphans_and_preserves_success_keys() {
        let harness = TestHarness::new().await;
        let mut queue =
            JobQueue::<SendLifecycleNotification>::new(&harness.apalis_pool);
        let job = SendLifecycleNotification {
            notification: LifecycleNotification::FreezeApplied {
                underlying: underlying(),
            },
        };
        queue
            .push_with_idempotency_key(job.clone(), "notification:orphan")
            .await
            .unwrap();
        queue
            .push_with_idempotency_key(job.clone(), "notification:terminal")
            .await
            .unwrap();
        queue
            .push_with_idempotency_key(job.clone(), "notification:dead")
            .await
            .unwrap();
        queue
            .push_with_idempotency_key(job, "notification:retryable")
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES ('dead-worker', ?, 'SqliteStorage')",
        )
        .bind(job_type::<SendLifecycleNotification>())
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_at = 1, lock_by = 'dead-worker' WHERE idempotency_key = 'notification:orphan'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Done' WHERE idempotency_key = 'notification:terminal'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Killed' WHERE idempotency_key = 'notification:dead'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE Jobs SET status = 'Failed', attempts = 1, max_attempts = 25 WHERE idempotency_key = 'notification:retryable'",
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        reset_orphaned_lifecycle_notification_jobs(&harness.pool)
            .await
            .unwrap();
        release_dead_lifecycle_notification_job(
            &harness.pool,
            "notification:dead",
        )
        .await
        .unwrap();
        let dead_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = 'notification:dead'",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(dead_count, 0);

        release_dead_lifecycle_notification_job(
            &harness.pool,
            "notification:retryable",
        )
        .await
        .unwrap();
        let retryable_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = 'notification:retryable'",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(retryable_count, 1);

        vacuum_terminal_lifecycle_notification_jobs(&harness.pool)
            .await
            .unwrap();

        let (status, lock_at, lock_by):
            (String, Option<i64>, Option<String>) = sqlx::query_as(
                "SELECT status, lock_at, lock_by FROM Jobs WHERE idempotency_key = 'notification:orphan'",
            )
            .fetch_one(&harness.pool)
            .await
            .unwrap();
        assert_eq!(status, "Pending");
        assert!(lock_at.is_none());
        assert!(lock_by.is_none());

        let terminal_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE idempotency_key = 'notification:terminal'",
        )
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(terminal_count, 1);
    }
}
