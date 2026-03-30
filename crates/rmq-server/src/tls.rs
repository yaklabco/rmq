use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

/// Ensure the rustls crypto provider is installed.
pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Load TLS certificates and private key from PEM files and build a TlsAcceptor.
pub fn load_tls_config(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> io::Result<TlsAcceptor> {
    ensure_crypto_provider();
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a TlsAcceptor from raw PEM bytes (useful for tests).
pub fn tls_config_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> io::Result<TlsAcceptor> {
    ensure_crypto_provider();
    let certs = load_certs_from_reader(&mut BufReader::new(cert_pem))?;
    let key = load_key_from_reader(&mut BufReader::new(key_pem))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: impl AsRef<Path>) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    load_certs_from_reader(&mut reader)
}

fn load_certs_from_reader(
    reader: &mut dyn io::BufRead,
) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(reader).collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no certificates found in PEM file",
        ));
    }
    Ok(certs)
}

fn load_key(path: impl AsRef<Path>) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    load_key_from_reader(&mut reader)
}

fn load_key_from_reader(reader: &mut dyn io::BufRead) -> io::Result<PrivateKeyDer<'static>> {
    loop {
        match rustls_pemfile::read_one(reader)? {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(PrivateKeyDer::Pkcs1(key)),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(PrivateKeyDer::Pkcs8(key)),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(PrivateKeyDer::Sec1(key)),
            Some(_) => continue, // skip other items
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no private key found in PEM file",
                ));
            }
        }
    }
}
