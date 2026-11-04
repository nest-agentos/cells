//! Authentication and authorization policy.
//!
//! Principals and public read access compile into immutable client capabilities;
//! handlers share these checks across all transports:
//!
//! * **Authentication** turns an `Authorization` header into an identity.
//!   A password client uses HTTP Basic and never falls back to Bearer; a token
//!   client uses Bearer and has no username.
//! * **Authorization** decides whether that identity may read or write an
//!   export alias. The server never trusts a client's own conclusion.

use crate::core::error::Error;
use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// One authenticated principal and the aliases it may reach.
pub struct Client {
    id: String,
    anonymous: bool,
    restricted: bool,
    execution_exports: Vec<String>,
    harnesses: Vec<String>,
    revision: String,
    secret: String,
    username: Option<String>,
    exports: Vec<String>,
    write_exports: Vec<String>,
}

impl Client {
    pub fn new(
        secret: String,
        username: Option<String>,
        exports: Vec<String>,
        write_exports: Vec<String>,
    ) -> Self {
        Self {
            id: "owner".into(),
            anonymous: false,
            restricted: false,
            execution_exports: exports.clone(),
            harnesses: Vec::new(),
            revision: String::new(),
            secret,
            username,
            exports,
            write_exports,
        }
        .revised()
    }

    pub fn scoped(mut self, id: String, execution: Vec<String>, harnesses: Vec<String>) -> Self {
        self.id = id;
        self.restricted = true;
        self.execution_exports = execution;
        self.harnesses = harnesses;
        self.revised()
    }
    pub fn public(exports: Vec<String>) -> Self {
        let mut client = Self::new(String::new(), None, exports, vec![]).scoped(
            "anonymous".into(),
            vec![],
            vec![],
        );
        client.anonymous = true;
        client
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn anonymous(&self) -> bool {
        self.anonymous
    }
    pub fn legacy(&self) -> bool {
        !self.restricted
    }
    pub fn may_execute(&self, alias: &str) -> bool {
        !self.anonymous && self.execution_exports.iter().any(|a| a == alias)
    }
    pub fn may_harness(&self, profile: &str) -> bool {
        !self.anonymous && (!self.restricted || self.harnesses.iter().any(|id| id == profile))
    }
    pub fn has_execution(&self) -> bool {
        !self.execution_exports.is_empty()
    }
    pub fn has_harness(&self) -> bool {
        !self.anonymous && (!self.restricted || !self.harnesses.is_empty())
    }
    pub fn authorization(&self) -> serde_json::Value {
        serde_json::json!({"principalId": self.id, "anonymous": self.anonymous, "revision": self.revision})
    }
    pub(crate) fn bind_resources(&mut self, resources: &[String]) {
        self.revision = format!(
            "{:x}",
            Sha256::digest(
                serde_json::json!([self.revision, resources])
                    .to_string()
                    .as_bytes()
            )
        );
    }
    fn revised(mut self) -> Self {
        let policy = serde_json::json!([
            self.id,
            self.exports,
            self.write_exports,
            self.execution_exports,
            self.harnesses,
            self.restricted
        ]);
        self.revision = format!("{:x}", Sha256::digest(policy.to_string().as_bytes()));
        self
    }

    /// Whether this client may read `alias`.
    pub fn may_read(&self, alias: &str) -> bool {
        self.exports.iter().any(|candidate| candidate == alias)
    }

    /// Whether this client may write `alias`.
    pub fn may_write(&self, alias: &str) -> bool {
        self.write_exports
            .iter()
            .any(|candidate| candidate == alias)
    }

    /// Whether this client may write anywhere.
    pub fn can_write(&self) -> bool {
        !self.write_exports.is_empty()
    }

    /// Aliases visible to this client, in configuration order.
    pub fn exports(&self) -> &[String] {
        &self.exports
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// The configured secret. Exposed for configuration tests and diagnostics;
    /// Only the startup console may show it; never include it in structured
    /// events or HTTP responses.
    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Compare the presented header against this client without early exit.
    fn matches(&self, authorization: &str, basic: Option<(&str, &str)>) -> bool {
        if self.anonymous {
            return false;
        }
        match (&self.username, basic) {
            (Some(username), Some((name, password))) => {
                equal(username.as_bytes(), name.as_bytes())
                    & equal(self.secret.as_bytes(), password.as_bytes())
            }
            // Token clients accept Bearer only; Basic never authenticates them.
            (None, _) => split_scheme(authorization).is_some_and(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("bearer")
                    && equal(self.secret.as_bytes(), token.as_bytes())
            }),
            _ => false,
        }
    }
}

/// The configured identities plus the installation identity they belong to.
pub struct Auth {
    server_id: Option<String>,
    clients: Vec<Client>,
    bearers: HashMap<[u8; 32], usize>,
    basics: HashMap<String, usize>,
    anonymous: Option<usize>,
}

impl Auth {
    pub fn new(server_id: Option<String>, clients: Vec<Client>) -> Self {
        let mut auth = Self {
            server_id,
            clients,
            bearers: HashMap::new(),
            basics: HashMap::new(),
            anonymous: None,
        };
        for (index, client) in auth.clients.iter().enumerate() {
            if client.anonymous {
                auth.anonymous = Some(index);
            } else if let Some(name) = &client.username {
                auth.basics.insert(name.clone(), index);
            } else {
                auth.bearers
                    .insert(Sha256::digest(client.secret.as_bytes()).into(), index);
            }
        }
        auth
    }

    /// Stable installation identity, advertised through `/v1/capabilities`.
    pub fn server_id(&self) -> Option<&str> {
        self.server_id.as_deref()
    }

    pub fn clients(&self) -> &[Client] {
        &self.clients
    }

    /// Resolve the request identity, or `EACCES`.
    pub fn identify(&self, headers: &HeaderMap) -> Result<usize, Error> {
        if !headers.contains_key("authorization") {
            return self.anonymous.ok_or_else(Error::unauthenticated);
        }
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(Error::unauthenticated)?;
        // RFC 7235: the scheme name is case-insensitive.
        let basic = split_scheme(authorization)
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("basic"))
            .and_then(|(_, value)| STANDARD.decode(value).ok())
            .and_then(|bytes| String::from_utf8(bytes).ok());
        let credentials = basic.as_deref().and_then(|value| value.split_once(':'));
        let index = if let Some((name, _)) = credentials {
            self.basics.get(name)
        } else {
            split_scheme(authorization)
                .filter(|(s, _)| s.eq_ignore_ascii_case("bearer"))
                .and_then(|(_, token)| {
                    self.bearers
                        .get(&<[u8; 32]>::from(Sha256::digest(token.as_bytes())))
                })
        };
        index
            .copied()
            .filter(|index| self.clients[*index].matches(authorization, credentials))
            .ok_or_else(Error::unauthenticated)
    }

    /// The client behind an identity returned by [`Self::identify`].
    pub fn client(&self, identity: usize) -> &Client {
        &self.clients[identity]
    }
    pub fn try_client(&self, identity: usize) -> Result<&Client, Error> {
        self.clients
            .get(identity)
            .ok_or_else(|| Error::forbidden("EACCES"))
    }
    pub fn index(&self, id: &str) -> Result<usize, Error> {
        self.clients
            .iter()
            .position(|c| c.id == id)
            .ok_or_else(|| Error::forbidden("EACCES"))
    }

    /// [`Self::identify`] followed by a read check on `alias`.
    pub fn authorize(&self, identity: usize, alias: &str) -> Result<&Client, Error> {
        let client = self.client(identity);
        if !client.may_read(alias) {
            return Err(Error::forbidden("EACCES"));
        }
        Ok(client)
    }
}

/// Split an `Authorization` header into its scheme and credentials.
fn split_scheme(authorization: &str) -> Option<(&str, &str)> {
    authorization.split_once(' ')
}

/// Length-independent, branch-free comparison of two secrets.
fn equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Only the legacy single-principal index has an unambiguous migration.
pub(crate) fn deserialize_owner<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<String, D::Error> {
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Owner {
        Id(String),
        Legacy(u64),
    }
    match <Owner as serde::Deserialize>::deserialize(d)? {
        Owner::Id(id) if crate::core::ids::is_identifier_within(&id, 128) && id != "anonymous" => {
            Ok(id)
        }
        Owner::Legacy(0) => Ok("owner".into()),
        _ => Err(serde::de::Error::custom(
            "Invalid persistent principal identity",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", value.parse().unwrap());
        headers
    }

    fn password_client() -> Auth {
        Auth::new(
            None,
            vec![Client::new(
                "password-at-least-8-bytes".into(),
                Some("alice".into()),
                vec!["docs".into()],
                vec![],
            )],
        )
    }

    #[test]
    fn password_clients_require_basic_and_the_right_username() {
        let auth = password_client();
        let message = |value: &str| format!("Basic {}", STANDARD.encode(value));
        assert!(auth
            .identify(&headers(&message("alice:password-at-least-8-bytes")))
            .is_ok());
        assert!(auth
            .identify(&headers(&message("bob:password-at-least-8-bytes")))
            .is_err());
        assert!(auth.identify(&headers(&message("alice:wrong"))).is_err());
        assert!(auth
            .identify(&headers("Bearer password-at-least-8-bytes"))
            .is_err());
        assert!(auth.identify(&headers("Basic not-base64!")).is_err());
        assert!(auth.identify(&HeaderMap::new()).is_err());
    }

    #[test]
    fn token_clients_require_bearer() {
        let auth = Auth::new(
            None,
            vec![Client::new(
                "token-at-least-24-bytes-long".into(),
                None,
                vec!["docs".into()],
                vec![],
            )],
        );
        assert!(auth
            .identify(&headers("Bearer token-at-least-24-bytes-long"))
            .is_ok());
        assert!(auth
            .identify(&headers("Bearer token-at-least-24-bytes-lonG"))
            .is_err());
        assert!(auth
            .identify(&headers(&format!(
                "Basic {}",
                STANDARD.encode(":token-at-least-24-bytes-long")
            )))
            .is_err());
    }

    #[test]
    fn authorization_is_scoped_to_the_alias() {
        let auth = password_client();
        assert!(auth.authorize(0, "docs").is_ok());
        assert!(auth.authorize(0, "other").is_err());
        assert!(!auth.client(0).may_write("docs"));
    }
}
