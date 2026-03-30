use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512};
use subtle::ConstantTimeEq;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Error)]
pub enum PasswordError {
    #[error("bcrypt error: {0}")]
    Bcrypt(#[from] bcrypt::BcryptError),
    #[error("unknown hashing algorithm: {0}")]
    UnknownAlgorithm(String),
    #[error("HMAC error: invalid key length")]
    HmacInvalidKeyLength,
}

/// Supported password hashing algorithms.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum HashAlgorithm {
    #[default]
    Bcrypt,
    Sha256,
    Sha512,
    Plaintext,
}

/// Hash a password with the given algorithm.
///
/// For SHA-256/SHA-512, generates a random 16-byte salt and produces
/// `hex(salt):hex(hmac)` format output.
pub fn hash_password(password: &str, algorithm: &HashAlgorithm) -> Result<String, PasswordError> {
    match algorithm {
        HashAlgorithm::Bcrypt => {
            let hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)?;
            Ok(hash)
        }
        HashAlgorithm::Sha256 => {
            let salt = generate_salt();
            let mac = hmac_sha256(&salt, password.as_bytes())?;
            Ok(format!("{}:{}", hex::encode(salt), hex::encode(&mac)))
        }
        HashAlgorithm::Sha512 => {
            let salt = generate_salt();
            let mac = hmac_sha512(&salt, password.as_bytes())?;
            Ok(format!("{}:{}", hex::encode(salt), hex::encode(&mac)))
        }
        HashAlgorithm::Plaintext => Ok(password.to_string()),
    }
}

/// Verify a password against a stored hash.
///
/// For SHA paths, uses constant-time comparison via `subtle::ConstantTimeEq`.
/// Supports legacy unsalted hashes (no `:` separator) with a deprecation warning.
pub fn verify_password(
    password: &str,
    hash: &str,
    algorithm: &HashAlgorithm,
) -> Result<bool, PasswordError> {
    match algorithm {
        HashAlgorithm::Bcrypt => Ok(bcrypt::verify(password, hash)?),
        HashAlgorithm::Sha256 => {
            if let Some((salt_hex, hash_hex)) = hash.split_once(':') {
                // New salted HMAC format
                let salt = hex::decode(salt_hex);
                let mac = hmac_sha256(&salt, password.as_bytes())?;
                let expected = hex::decode(hash_hex);
                Ok(mac.ct_eq(&expected).into())
            } else {
                // Legacy unsalted format
                tracing::warn!(
                    "verifying against legacy unsalted SHA-256 hash; \
                     re-hash with a salted algorithm"
                );
                let mac = legacy_sha256(password.as_bytes());
                let expected = hex::decode(hash);
                Ok(mac.ct_eq(&expected).into())
            }
        }
        HashAlgorithm::Sha512 => {
            if let Some((salt_hex, hash_hex)) = hash.split_once(':') {
                let salt = hex::decode(salt_hex);
                let mac = hmac_sha512(&salt, password.as_bytes())?;
                let expected = hex::decode(hash_hex);
                Ok(mac.ct_eq(&expected).into())
            } else {
                tracing::warn!(
                    "verifying against legacy unsalted SHA-512 hash; \
                     re-hash with a salted algorithm"
                );
                let mac = legacy_sha512(password.as_bytes());
                let expected = hex::decode(hash);
                Ok(mac.ct_eq(&expected).into())
            }
        }
        HashAlgorithm::Plaintext => Ok(password == hash),
    }
}

fn generate_salt() -> [u8; 16] {
    use rand::RngCore;
    let mut salt = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    salt
}

fn hmac_sha256(salt: &[u8], data: &[u8]) -> Result<Vec<u8>, PasswordError> {
    let mut mac =
        HmacSha256::new_from_slice(salt).map_err(|_| PasswordError::HmacInvalidKeyLength)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hmac_sha512(salt: &[u8], data: &[u8]) -> Result<Vec<u8>, PasswordError> {
    let mut mac =
        HmacSha512::new_from_slice(salt).map_err(|_| PasswordError::HmacInvalidKeyLength)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// Legacy unsalted SHA-256 (for backward compatibility only).
fn legacy_sha256(data: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Legacy unsalted SHA-512 (for backward compatibility only).
fn legacy_sha512(data: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = Sha512::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Hex encoding/decoding helpers.
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn decode(hex_str: &str) -> Vec<u8> {
        (0..hex_str.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).unwrap_or(0))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bcrypt_hash_and_verify() {
        let hash = hash_password("secret", &HashAlgorithm::Bcrypt).unwrap();
        assert!(verify_password("secret", &hash, &HashAlgorithm::Bcrypt).unwrap());
        assert!(!verify_password("wrong", &hash, &HashAlgorithm::Bcrypt).unwrap());
    }

    #[test]
    fn test_sha256_salted_round_trip() {
        let hash = hash_password("secret", &HashAlgorithm::Sha256).unwrap();
        // Hash must contain a colon separating salt and HMAC
        assert!(hash.contains(':'), "salted hash should contain ':'");
        let parts: Vec<&str> = hash.split(':').collect();
        assert_eq!(parts.len(), 2);
        // Salt is 16 bytes = 32 hex chars
        assert_eq!(parts[0].len(), 32);

        assert!(verify_password("secret", &hash, &HashAlgorithm::Sha256).unwrap());
        assert!(!verify_password("wrong", &hash, &HashAlgorithm::Sha256).unwrap());
    }

    #[test]
    fn test_sha512_salted_round_trip() {
        let hash = hash_password("secret", &HashAlgorithm::Sha512).unwrap();
        assert!(hash.contains(':'), "salted hash should contain ':'");
        let parts: Vec<&str> = hash.split(':').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 32);

        assert!(verify_password("secret", &hash, &HashAlgorithm::Sha512).unwrap());
        assert!(!verify_password("wrong", &hash, &HashAlgorithm::Sha512).unwrap());
    }

    #[test]
    fn test_sha256_unique_salts() {
        let h1 = hash_password("secret", &HashAlgorithm::Sha256).unwrap();
        let h2 = hash_password("secret", &HashAlgorithm::Sha256).unwrap();
        // Different salts should produce different hashes
        assert_ne!(h1, h2);
        // Both must still verify
        assert!(verify_password("secret", &h1, &HashAlgorithm::Sha256).unwrap());
        assert!(verify_password("secret", &h2, &HashAlgorithm::Sha256).unwrap());
    }

    #[test]
    fn test_legacy_unsalted_sha256_verification() {
        // Simulate a legacy hash (no salt, plain SHA-256)
        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(b"secret");
        let legacy_hash = hex::encode(hasher.finalize());

        // Should not contain ':'
        assert!(!legacy_hash.contains(':'));

        // Must still verify with deprecation path
        assert!(verify_password("secret", &legacy_hash, &HashAlgorithm::Sha256).unwrap());
        assert!(!verify_password("wrong", &legacy_hash, &HashAlgorithm::Sha256).unwrap());
    }

    #[test]
    fn test_legacy_unsalted_sha512_verification() {
        use sha2::Digest;
        let mut hasher = Sha512::new();
        hasher.update(b"secret");
        let legacy_hash = hex::encode(hasher.finalize());

        assert!(!legacy_hash.contains(':'));
        assert!(verify_password("secret", &legacy_hash, &HashAlgorithm::Sha512).unwrap());
        assert!(!verify_password("wrong", &legacy_hash, &HashAlgorithm::Sha512).unwrap());
    }

    #[test]
    fn test_plaintext() {
        let hash = hash_password("secret", &HashAlgorithm::Plaintext).unwrap();
        assert_eq!(hash, "secret");
        assert!(verify_password("secret", &hash, &HashAlgorithm::Plaintext).unwrap());
    }
}
