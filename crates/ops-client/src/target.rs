//! Target resolution: reads one environment's base URL and identity inputs
//! from the process environment and validates them into a `Target`.

use url::Url;

/// Deployment the client talks to. Selects the IAP-fronted S01 ops base URL,
/// the identity inputs, and the Cloud Logging project.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Env {
    Staging,
    Production,
}

impl Env {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Staging => "S01_ISSUANCE_STAGING",
            Self::Production => "S01_ISSUANCE_PROD",
        }
    }

    /// Lowercase environment name that keys the refresh-token cache file, so
    /// each environment's OAuth client keeps its own cached token.
    pub(crate) const fn cache_slug(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }

    /// GCP project whose Cloud Logging holds this environment's bot logs: the
    /// projects the staging deploy and production release workflows ship to.
    pub(crate) const fn logging_project(self) -> &'static str {
        match self {
            Self::Staging => "s01-issuance-staging",
            Self::Production => "s01-issuance",
        }
    }
}

/// How the client proves an S01 Google identity to IAP. Either way the result
/// is one ID token sent as `Authorization: Bearer` through the same transport.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Identity {
    /// CI: an ID token the job minted through S01 workload identity (service
    /// account impersonation). Minting stays in CI because Google's Rust auth
    /// library does not issue ID tokens from external-account credentials.
    WorkloadIdentity { id_token: String },
    /// Operator: browser sign-in to the current S01 Google account through the
    /// S01 Desktop OAuth client, whose id and secret Google treats as
    /// non-confidential for a Desktop app.
    DesktopOauth { client_id: String, client_secret: String },
}

/// Connection settings for one environment. None of it is a credential on its
/// own: each identity input is worthless without an identity Google signs.
#[derive(Debug)]
pub(crate) struct Target {
    pub(crate) base_url: Url,
    pub(crate) identity: Identity,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TargetError {
    #[error("set {variable} to {hint}")]
    Missing { variable: String, hint: &'static str },
    #[error("{variable} is set but empty; set it to {hint}")]
    Blank { variable: String, hint: &'static str },
    #[error("{variable} is not a valid URL: {source}")]
    InvalidUrl {
        variable: String,
        #[source]
        source: url::ParseError,
    },
    #[error("{variable} must use https, got {url}")]
    NotHttps { variable: String, url: Url },
}

const URL_HINT: &str = "the https base URL of the S01 ops load balancer";
const ID_TOKEN_HINT: &str =
    "an ID token minted through S01 workload identity, or unset it to sign in";
const CLIENT_ID_HINT: &str = "the S01 Desktop OAuth client id";
const CLIENT_SECRET_HINT: &str = "the S01 Desktop OAuth client secret (non-confidential for a Desktop client)";

/// Resolves `env` from `lookup` (the process environment in production). A set
/// `<PREFIX>_ID_TOKEN` selects workload identity and needs no OAuth client;
/// otherwise the Desktop OAuth client id and secret are required.
pub(crate) fn resolve(
    env: Env,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Target, TargetError> {
    let prefix = env.prefix();
    let url_variable = format!("{prefix}_URL");
    let raw_url = required(&lookup, &url_variable, URL_HINT)?;
    let base_url = Url::parse(&raw_url).map_err(|source| {
        TargetError::InvalidUrl { variable: url_variable.clone(), source }
    })?;

    if base_url.scheme() != "https" {
        return Err(TargetError::NotHttps {
            variable: url_variable,
            url: base_url,
        });
    }

    Ok(Target { base_url, identity: identity(prefix, &lookup)? })
}

fn identity(
    prefix: &str,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<Identity, TargetError> {
    let token_variable = format!("{prefix}_ID_TOKEN");

    // A set-but-empty token means the CI step that mints it did not run.
    // Falling back to the browser sign-in would hang a headless job, so it is
    // an error rather than a silent switch of identity.
    if lookup(&token_variable).is_some() {
        let id_token = required(lookup, &token_variable, ID_TOKEN_HINT)?;
        return Ok(Identity::WorkloadIdentity {
            id_token: id_token.trim().to_owned(),
        });
    }

    Ok(Identity::DesktopOauth {
        client_id: required(
            lookup,
            &format!("{prefix}_CLIENT_ID"),
            CLIENT_ID_HINT,
        )?,
        client_secret: required(
            lookup,
            &format!("{prefix}_CLIENT_SECRET"),
            CLIENT_SECRET_HINT,
        )?,
    })
}

fn required(
    lookup: &impl Fn(&str) -> Option<String>,
    variable: &str,
    hint: &'static str,
) -> Result<String, TargetError> {
    match lookup(variable) {
        None => {
            Err(TargetError::Missing { variable: variable.to_owned(), hint })
        }
        Some(value) if value.trim().is_empty() => {
            Err(TargetError::Blank { variable: variable.to_owned(), hint })
        }
        Some(value) => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{Env, Identity, TargetError, resolve};

    fn lookup(
        pairs: &[(&str, &str)],
    ) -> impl Fn(&str) -> Option<String> + use<> {
        let vars: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        move |key| vars.get(key).cloned()
    }

    #[test]
    fn a_workload_identity_token_needs_no_oauth_client() {
        let target = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "https://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_ID_TOKEN", " ci-token\n"),
            ]),
        )
        .unwrap();

        assert_eq!(target.base_url.as_str(), "https://ops.staging.example/");
        assert_eq!(
            target.identity,
            Identity::WorkloadIdentity { id_token: "ci-token".to_owned() }
        );
    }

    #[test]
    fn without_a_token_the_operator_signs_in_through_the_desktop_client() {
        let target = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "https://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_CLIENT_ID", "desktop-id"),
                ("S01_ISSUANCE_STAGING_CLIENT_SECRET", "desktop-secret"),
            ]),
        )
        .unwrap();

        assert_eq!(
            target.identity,
            Identity::DesktopOauth {
                client_id: "desktop-id".to_owned(),
                client_secret: "desktop-secret".to_owned(),
            }
        );
    }

    #[test]
    fn a_blank_token_is_refused_rather_than_falling_back_to_a_browser() {
        let error = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "https://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_ID_TOKEN", "  "),
                ("S01_ISSUANCE_STAGING_CLIENT_ID", "desktop-id"),
                ("S01_ISSUANCE_STAGING_CLIENT_SECRET", "desktop-secret"),
            ]),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TargetError::Blank { variable, .. }
                if variable == "S01_ISSUANCE_STAGING_ID_TOKEN"
        ));
    }

    #[test]
    fn the_operator_path_names_the_missing_client_variable() {
        let error = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "https://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_CLIENT_ID", "desktop-id"),
            ]),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TargetError::Missing { variable, .. }
                if variable == "S01_ISSUANCE_STAGING_CLIENT_SECRET"
        ));
    }

    #[test]
    fn production_never_reads_the_staging_variables() {
        let error = resolve(
            Env::Production,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "https://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_ID_TOKEN", "staging-token"),
            ]),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TargetError::Missing { variable, .. }
                if variable == "S01_ISSUANCE_PROD_URL"
        ));
    }

    #[test]
    fn a_plain_http_base_url_is_refused() {
        let error = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "http://ops.staging.example"),
                ("S01_ISSUANCE_STAGING_ID_TOKEN", "ci-token"),
            ]),
        )
        .unwrap_err();

        assert!(matches!(error, TargetError::NotHttps { .. }));
    }

    #[test]
    fn a_malformed_base_url_is_refused() {
        let error = resolve(
            Env::Staging,
            lookup(&[
                ("S01_ISSUANCE_STAGING_URL", "not a url"),
                ("S01_ISSUANCE_STAGING_ID_TOKEN", "ci-token"),
            ]),
        )
        .unwrap_err();

        assert!(matches!(error, TargetError::InvalidUrl { .. }));
    }
}
