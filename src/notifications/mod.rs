//! Operator notifications for the corporate-actions lifecycle.

mod telegram;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::jobs::Job;
use crate::redemption::IssuerRedemptionRequestId;
use crate::tokenized_asset::UnderlyingSymbol;

use telegram::TelegramNotifier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotificationKind {
    CorporateActionScheduled,
    CorporateActionsSyncFailed,
    FreezeApplied,
    UnfreezeApplied,
    FreezeTransitionFailed,
    RedemptionHeld,
    RedemptionResumed,
    RedemptionResumeFailed,
}

impl NotificationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CorporateActionScheduled => "corporate_action_scheduled",
            Self::CorporateActionsSyncFailed => "corporate_actions_sync_failed",
            Self::FreezeApplied => "freeze_applied",
            Self::UnfreezeApplied => "unfreeze_applied",
            Self::FreezeTransitionFailed => "freeze_transition_failed",
            Self::RedemptionHeld => "redemption_held",
            Self::RedemptionResumed => "redemption_resumed",
            Self::RedemptionResumeFailed => "redemption_resume_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LifecycleNotification {
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
}

impl LifecycleNotification {
    #[must_use]
    pub const fn kind(&self) -> NotificationKind {
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
        }
    }

    #[must_use]
    pub fn message(&self) -> String {
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FreezeTransitionKind {
    Freeze,
    Unfreeze,
}

#[async_trait]
pub trait LifecycleNotifier: Send + Sync {
    async fn notify(&self, notification: &LifecycleNotification);
}

pub(crate) struct NoopLifecycleNotifier;

#[async_trait]
impl LifecycleNotifier for NoopLifecycleNotifier {
    async fn notify(&self, _notification: &LifecycleNotification) {}
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
    type Error = Infallible;

    async fn perform(
        &self,
        ctx: &LifecycleNotificationJobCtx,
    ) -> Result<Self::Output, Self::Error> {
        ctx.notifier.notify(&self.notification).await;
        Ok(())
    }
}

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
    pub fn build_notifier(
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleNotificationsConfigError {
    #[error("Telegram bot token is present but chat id is missing")]
    MissingChatId,
    #[error("Telegram chat id is present but bot token is missing")]
    MissingBotToken,
    #[error("Telegram message thread id requires bot token and chat id")]
    ThreadWithoutChannel,
    #[error("Telegram bot token must not be blank")]
    BlankBotToken,
    #[error("Telegram chat id must not be zero")]
    ZeroChatId,
    #[error("Telegram message thread id must be positive")]
    InvalidMessageThreadId,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NotificationBuildError {
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
        async fn notify(&self, notification: &LifecycleNotification) {
            self.captured.lock().unwrap().push(notification.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use chrono::{TimeZone, Utc};

    use super::*;

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
}
