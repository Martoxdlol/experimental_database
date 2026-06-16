//! JWT authentication and authorization helpers for the wire protocol.

use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

use crate::error::{Result, WireError};

/// Authentication configuration for the wire layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: JwtAlgorithm,
    pub secret: Option<Vec<u8>>,
    pub public_key: Option<Vec<u8>>,
    pub issuer: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: JwtAlgorithm::HS256,
            secret: None,
            public_key: None,
            issuer: None,
        }
    }
}

impl AuthConfig {
    /// Create an enabled HMAC config from raw secret bytes.
    pub fn hmac(algorithm: JwtAlgorithm, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            enabled: true,
            algorithm,
            secret: Some(secret.into()),
            public_key: None,
            issuer: None,
        }
    }

    /// Create an enabled HMAC config from the base64-encoded secret used by
    /// server configuration files.
    pub fn hmac_base64(algorithm: JwtAlgorithm, secret: &str) -> Result<Self> {
        let secret = STANDARD
            .decode(secret)
            .map_err(|e| WireError::InvalidMessage(format!("invalid base64 JWT secret: {e}")))?;
        Ok(Self::hmac(algorithm, secret))
    }

    /// Create an enabled public-key config from PEM bytes.
    pub fn public_key(algorithm: JwtAlgorithm, public_key: impl Into<Vec<u8>>) -> Self {
        Self {
            enabled: true,
            algorithm,
            secret: None,
            public_key: Some(public_key.into()),
            issuer: None,
        }
    }

    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Validate configured key material before a server starts accepting
    /// connections.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let algorithm = self.algorithm;
        if algorithm.is_hmac() {
            let secret = self.secret.as_deref().ok_or_else(|| {
                WireError::InvalidMessage(
                    "auth.jwt_secret is required for HMAC JWT algorithms".to_string(),
                )
            })?;
            if secret.is_empty() {
                return Err(WireError::InvalidMessage(
                    "auth.jwt_secret must not be empty".to_string(),
                ));
            }
            return Ok(());
        }

        let public_key = self.public_key.as_deref().ok_or_else(|| {
            WireError::InvalidMessage(
                "auth.jwt_public_key_file is required for RSA/EC JWT algorithms".to_string(),
            )
        })?;

        if algorithm.is_rsa() {
            DecodingKey::from_rsa_pem(public_key).map_err(|e| {
                WireError::InvalidMessage(format!("invalid RSA JWT public key: {e}"))
            })?;
            return Ok(());
        }
        if algorithm.is_ec() {
            DecodingKey::from_ec_pem(public_key).map_err(|e| {
                WireError::InvalidMessage(format!("invalid EC JWT public key: {e}"))
            })?;
            return Ok(());
        }

        Err(WireError::InvalidMessage(
            "unsupported JWT algorithm".to_string(),
        ))
    }
}

/// JWT algorithms accepted by the server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtAlgorithm {
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES384,
}

impl JwtAlgorithm {
    fn to_jsonwebtoken(self) -> Algorithm {
        match self {
            JwtAlgorithm::HS256 => Algorithm::HS256,
            JwtAlgorithm::HS384 => Algorithm::HS384,
            JwtAlgorithm::HS512 => Algorithm::HS512,
            JwtAlgorithm::RS256 => Algorithm::RS256,
            JwtAlgorithm::RS384 => Algorithm::RS384,
            JwtAlgorithm::RS512 => Algorithm::RS512,
            JwtAlgorithm::ES256 => Algorithm::ES256,
            JwtAlgorithm::ES384 => Algorithm::ES384,
        }
    }

    fn is_hmac(self) -> bool {
        matches!(
            self,
            JwtAlgorithm::HS256 | JwtAlgorithm::HS384 | JwtAlgorithm::HS512
        )
    }

    fn is_rsa(self) -> bool {
        matches!(
            self,
            JwtAlgorithm::RS256 | JwtAlgorithm::RS384 | JwtAlgorithm::RS512
        )
    }

    fn is_ec(self) -> bool {
        matches!(self, JwtAlgorithm::ES256 | JwtAlgorithm::ES384)
    }
}

/// Authenticated claims used by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthClaims {
    pub sub: Option<String>,
    pub databases: Option<Vec<String>>,
    pub role: Role,
    pub exp: u64,
    pub issuer: Option<String>,
}

impl AuthClaims {
    /// Claims used internally when authentication is disabled.
    pub fn unauthenticated_admin() -> Self {
        Self {
            sub: None,
            databases: None,
            role: Role::Admin,
            exp: u64::MAX,
            issuer: None,
        }
    }
}

/// Authorization role carried by the JWT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    User,
}

fn default_role() -> Role {
    Role::User
}

#[derive(Debug, Clone, Deserialize)]
struct JwtClaimsPayload {
    exp: u64,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    databases: Option<Vec<String>>,
    #[serde(default = "default_role")]
    role: Role,
}

impl From<JwtClaimsPayload> for AuthClaims {
    fn from(value: JwtClaimsPayload) -> Self {
        Self {
            sub: value.sub,
            databases: value.databases,
            role: value.role,
            exp: value.exp,
            issuer: value.iss,
        }
    }
}

/// Validate a JWT and return the claims relevant to the server.
pub fn validate_token(token: &str, config: &AuthConfig) -> Result<AuthClaims> {
    if !config.enabled {
        return Ok(AuthClaims::unauthenticated_admin());
    }

    let algorithm = config.algorithm.to_jsonwebtoken();
    let key = decoding_key(config)?;
    let mut validation = Validation::new(algorithm);
    validation.validate_nbf = true;
    validation.validate_aud = false;
    validation.set_required_spec_claims(&["exp"]);
    if let Some(issuer) = &config.issuer {
        validation.set_issuer(&[issuer]);
    }

    decode::<JwtClaimsPayload>(token, &key, &validation)
        .map(|data| data.claims.into())
        .map_err(|e| WireError::AuthFailed(e.to_string()))
}

fn decoding_key(config: &AuthConfig) -> Result<DecodingKey> {
    let algorithm = config.algorithm;
    if algorithm.is_hmac() {
        let secret = config
            .secret
            .as_deref()
            .ok_or_else(|| WireError::AuthFailed("missing JWT secret".to_string()))?;
        return Ok(DecodingKey::from_secret(secret));
    }

    let public_key = config
        .public_key
        .as_deref()
        .ok_or_else(|| WireError::AuthFailed("missing JWT public key".to_string()))?;

    if algorithm.is_rsa() {
        return DecodingKey::from_rsa_pem(public_key)
            .map_err(|e| WireError::AuthFailed(format!("invalid RSA public key: {e}")));
    }
    if algorithm.is_ec() {
        return DecodingKey::from_ec_pem(public_key)
            .map_err(|e| WireError::AuthFailed(format!("invalid EC public key: {e}")));
    }

    Err(WireError::AuthFailed(
        "unsupported JWT algorithm".to_string(),
    ))
}

/// Return true if the claims allow access to the named database.
pub fn check_database_access(claims: &AuthClaims, database: &str) -> bool {
    claims.role == Role::Admin
        || claims
            .databases
            .as_ref()
            .is_none_or(|databases| databases.iter().any(|allowed| allowed == database))
}

/// Return true if the claims allow administrative operations.
pub fn check_admin_access(claims: &AuthClaims) -> bool {
    claims.role == Role::Admin
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode, get_current_timestamp};
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestClaims<'a> {
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sub: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        iss: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        databases: Option<Vec<&'a str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<&'a str>,
    }

    fn token(claims: TestClaims<'_>, secret: &[u8]) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    #[test]
    fn disabled_auth_returns_admin_claims() {
        let claims = validate_token("", &AuthConfig::default()).unwrap();

        assert_eq!(claims.role, Role::Admin);
        assert!(check_admin_access(&claims));
        assert!(check_database_access(&claims, "anything"));
    }

    #[test]
    fn validates_hmac_token_and_claims() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()).with_issuer("issuer-a");
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: None,
            sub: Some("svc-1"),
            iss: Some("issuer-a"),
            databases: Some(vec!["app", "analytics"]),
            role: Some("user"),
        };

        let claims = validate_token(&token(claims, secret), &config).unwrap();

        assert_eq!(claims.sub.as_deref(), Some("svc-1"));
        assert_eq!(claims.issuer.as_deref(), Some("issuer-a"));
        assert_eq!(claims.role, Role::User);
        assert!(check_database_access(&claims, "app"));
        assert!(!check_database_access(&claims, "other"));
        assert!(!check_admin_access(&claims));
    }

    #[test]
    fn admin_can_access_all_databases() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec());
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: None,
            sub: None,
            iss: None,
            databases: Some(vec!["app"]),
            role: Some("admin"),
        };

        let claims = validate_token(&token(claims, secret), &config).unwrap();

        assert!(check_admin_access(&claims));
        assert!(check_database_access(&claims, "other"));
    }

    #[test]
    fn missing_databases_claim_allows_user_access_to_all_databases() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec());
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: None,
            sub: None,
            iss: None,
            databases: None,
            role: None,
        };

        let claims = validate_token(&token(claims, secret), &config).unwrap();

        assert_eq!(claims.role, Role::User);
        assert!(check_database_access(&claims, "any-db"));
    }

    #[test]
    fn rejects_expired_token() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec());
        let claims = TestClaims {
            exp: get_current_timestamp() - 120,
            nbf: None,
            sub: None,
            iss: None,
            databases: None,
            role: None,
        };

        assert!(matches!(
            validate_token(&token(claims, secret), &config),
            Err(WireError::AuthFailed(_))
        ));
    }

    #[test]
    fn rejects_wrong_issuer() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()).with_issuer("expected");
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: None,
            sub: None,
            iss: Some("other"),
            databases: None,
            role: None,
        };

        assert!(matches!(
            validate_token(&token(claims, secret), &config),
            Err(WireError::AuthFailed(_))
        ));
    }

    #[test]
    fn rejects_not_before_in_future() {
        let secret = b"super-secret";
        let config = AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec());
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: Some(get_current_timestamp() + 3600),
            sub: None,
            iss: None,
            databases: None,
            role: None,
        };

        assert!(matches!(
            validate_token(&token(claims, secret), &config),
            Err(WireError::AuthFailed(_))
        ));
    }

    #[test]
    fn hmac_base64_config_decodes_secret() {
        let secret = b"super-secret";
        let config =
            AuthConfig::hmac_base64(JwtAlgorithm::HS256, &STANDARD.encode(secret)).unwrap();
        let claims = TestClaims {
            exp: get_current_timestamp() + 3600,
            nbf: None,
            sub: None,
            iss: None,
            databases: None,
            role: None,
        };

        assert!(validate_token(&token(claims, secret), &config).is_ok());
    }

    #[test]
    fn hmac_base64_config_rejects_invalid_base64() {
        assert!(matches!(
            AuthConfig::hmac_base64(JwtAlgorithm::HS256, "not base64!"),
            Err(WireError::InvalidMessage(message)) if message.contains("invalid base64 JWT secret")
        ));
    }

    #[test]
    fn validate_rejects_enabled_auth_without_usable_key_material() {
        let missing_secret = AuthConfig {
            enabled: true,
            algorithm: JwtAlgorithm::HS256,
            secret: None,
            public_key: None,
            issuer: None,
        };
        assert!(matches!(
            missing_secret.validate(),
            Err(WireError::InvalidMessage(message)) if message.contains("jwt_secret")
        ));

        let empty_secret = AuthConfig::hmac(JwtAlgorithm::HS256, Vec::new());
        assert!(matches!(
            empty_secret.validate(),
            Err(WireError::InvalidMessage(message)) if message.contains("must not be empty")
        ));

        let invalid_public_key =
            AuthConfig::public_key(JwtAlgorithm::RS256, b"not a public key".to_vec());
        assert!(matches!(
            invalid_public_key.validate(),
            Err(WireError::InvalidMessage(message)) if message.contains("invalid RSA JWT public key")
        ));
    }
}
