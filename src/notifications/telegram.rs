use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::{LifecycleNotification, LifecycleNotifier, NotificationBuildError};

pub(super) struct TelegramNotifier {
    client: reqwest::Client,
    endpoint: String,
    chat_id: i64,
    message_thread_id: Option<i64>,
}

impl TelegramNotifier {
    pub(super) fn new(
        bot_token: &str,
        chat_id: i64,
        message_thread_id: Option<i64>,
    ) -> Result<Self, NotificationBuildError> {
        Self::with_base_url(
            "https://api.telegram.org",
            bot_token,
            chat_id,
            message_thread_id,
        )
    }

    fn with_base_url(
        base_url: &str,
        bot_token: &str,
        chat_id: i64,
        message_thread_id: Option<i64>,
    ) -> Result<Self, NotificationBuildError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| {
                NotificationBuildError::TelegramClient(error.without_url())
            })?;

        Ok(Self {
            client,
            endpoint: format!(
                "{}/bot{bot_token}/sendMessage",
                base_url.trim_end_matches('/')
            ),
            chat_id,
            message_thread_id,
        })
    }

    async fn send(
        &self,
        notification: &LifecycleNotification,
    ) -> Result<(), TelegramDeliveryError> {
        let message = notification.message();
        let request = SendMessageRequest {
            chat_id: self.chat_id,
            text: &message,
            message_thread_id: self.message_thread_id,
        };
        let response = self
            .client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                TelegramDeliveryError::Request(error.without_url())
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(TelegramDeliveryError::ApiStatus(status));
        }

        let envelope =
            response.json::<TelegramResponseEnvelope>().await.map_err(
                |error| TelegramDeliveryError::Response(error.without_url()),
            )?;
        if !envelope.ok {
            return Err(TelegramDeliveryError::ApiRejected);
        }

        Ok(())
    }
}

#[async_trait]
impl LifecycleNotifier for TelegramNotifier {
    async fn notify(&self, notification: &LifecycleNotification) {
        if let Err(error) = self.send(notification).await {
            tracing::error!(
                target: "notifications",
                event = "notification_delivery_failed",
                notification_kind = notification.kind().as_str(),
                error = %error,
                "notification_delivery_failed"
            );
        }
    }
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    chat_id: i64,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<i64>,
}

#[derive(Deserialize)]
struct TelegramResponseEnvelope {
    ok: bool,
}

#[derive(Debug, thiserror::Error)]
enum TelegramDeliveryError {
    #[error("Telegram request failed")]
    Request(#[source] reqwest::Error),
    #[error("Telegram response was invalid")]
    Response(#[source] reqwest::Error),
    #[error("Telegram API returned status {0}")]
    ApiStatus(StatusCode),
    #[error("Telegram API rejected request")]
    ApiRejected,
}

#[cfg(test)]
mod tests {
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::UnderlyingSymbol;

    fn notification() -> LifecycleNotification {
        LifecycleNotification::FreezeApplied {
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
        }
    }

    #[tokio::test]
    async fn sends_typed_message_to_the_configured_topic() {
        let server = MockServer::start_async().await;
        let request = server
            .mock_async(|when, then| {
                when.method(POST).path("/bot123:abc/sendMessage").json_body(
                    json!({
                        "chat_id": -1_001_234_567_890_i64,
                        "text": "Freeze applied: AAPL",
                        "message_thread_id": 42_i64,
                    }),
                );
                then.status(200).json_body(json!({ "ok": true }));
            })
            .await;

        let notifier = TelegramNotifier::with_base_url(
            &server.base_url(),
            "123:abc",
            -1_001_234_567_890,
            Some(42),
        )
        .unwrap();

        notifier.notify(&notification()).await;

        request.assert_async().await;
    }

    #[traced_test]
    #[tokio::test]
    async fn api_rejection_is_structured_and_does_not_log_the_response_body() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/bot123:abc/sendMessage");
                then.status(400).body("untrusted-response-marker");
            })
            .await;

        let notifier = TelegramNotifier::with_base_url(
            &server.base_url(),
            "123:abc",
            42,
            None,
        )
        .unwrap();

        notifier.notify(&notification()).await;

        assert!(logs_contain_at!(
            Level::ERROR,
            &["notification_delivery_failed", "freeze_applied", "400"]
        ));
        assert!(!logs_contain_at!(
            Level::ERROR,
            &["untrusted-response-marker"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn unsuccessful_response_envelope_is_not_treated_as_delivery() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).path("/bot123:abc/sendMessage");
                then.status(200).json_body(json!({
                    "ok": false,
                    "description": "untrusted-response-marker",
                }));
            })
            .await;

        let notifier = TelegramNotifier::with_base_url(
            &server.base_url(),
            "123:abc",
            42,
            None,
        )
        .unwrap();

        notifier.notify(&notification()).await;

        assert!(logs_contain_at!(
            Level::ERROR,
            &[
                "notification_delivery_failed",
                "freeze_applied",
                "Telegram API rejected request"
            ]
        ));
        assert!(!logs_contain_at!(
            Level::ERROR,
            &["untrusted-response-marker"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn connection_failure_does_not_log_the_token_bearing_url() {
        let notifier = TelegramNotifier::with_base_url(
            "http://127.0.0.1:0",
            "secret-token-marker",
            42,
            None,
        )
        .unwrap();

        notifier.notify(&notification()).await;

        assert!(logs_contain_at!(
            Level::ERROR,
            &["notification_delivery_failed", "freeze_applied"]
        ));
        assert!(!logs_contain_at!(Level::ERROR, &["secret-token-marker"]));
    }
}
