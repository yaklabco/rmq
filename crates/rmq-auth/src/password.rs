use sha2::{Digest, Sha256, Sha512};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PasswordError {
    #[error("bcrypt error: {0}")]
    Bcrypt(#[from] bcrypt::BcryptError),
    #[error("unknown hashing algorithm: {0}")]
    UnknownAlgorithm(String),
}

/// Supported password hashing algorithms.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    Bcrypt,
    Sha256,
    Sha512,
    Plaintext,
}

impl Default for HashAlgorithm {
    fn default() -> Self {
        Self::Bcrypt
    }
}

/// Hash a password with the given algorithm.
pub fn hash_password(password: &str, algorithm: &HashAlgorithm) -> Result<String, PasswordError> {
    match algorithm {
        HashAlgorithm::Bcrypt => {
            let hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)?;
            Ok(hash)
        }
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(password.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        HashAlgorithm::Sha512 => {
            let mut hasher = Sha512::new();
            hasher.update(password.as_bytes());
            Ok(hex::encode(hasher.finalize()))
        }
        HashAlgorithm::Plaintext => Ok(password.to_string()),
    }
}

/// Verify a password against a stored hash.
pub fn verify_password(
    password: &str,
    hash: &str,
    algorithm: &HashAlgorithm,
) -> Result<bool, PasswordError> {
    match algorithm {
        HashAlgorithm::Bcrypt => Ok(bcrypt::verify(password, hash)?),
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(password.as_bytes());
            Ok(hex::encode(hasher.finalize()) == hash)
        }
        HashAlgorithm::Sha512 => {
            let mut hasher = Sha512::new();
            hasher.update(password.as_bytes());
            Ok(hex::encode(hasher.finalize()) == hash)
        }
        HashAlgorithm::Plaintext => Ok(password == hash),
    }
}

// Small hex encoding helper (avoid adding another dependency)
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
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
    fn test_sha256_hash_and_verify() {
        let hash = hash_password("secret", &HashAlgorithm::Sha256).unwrap();
        assert!(verify_password("secret", &hash, &HashAlgorithm::Sha256).unwrap());
        assert!(!verify_password("wrong", &hash, &HashAlgorithm::Sha256).unwrap());
    }

    #[test]
    fn test_sha512_hash_and_verify() {
        let hash = hash_password("secret", &HashAlgorithm::Sha512).unwrap();
        assert!(verify_password("secret", &hash, &HashAlgorithm::Sha512).unwrap());
        assert!(!verify_password("wrong", &hash, &HashAlgorithm::Sha512).unwrap());
    }

    #[test]
    fn test_plaintext() {
        let hash = hash_password("secret", &HashAlgorithm::Plaintext).unwrap();
        assert_eq!(hash, "secret");
        assert!(verify_password("secret", &hash, &HashAlgorithm::Plaintext).unwrap());
    }
}
