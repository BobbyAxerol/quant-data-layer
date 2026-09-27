//! Mutual-TLS listener for the gateway, built on `rustls` + `tokio-rustls`
//! directly.
//!
//! tonic's own `tls` feature pulls `rustls-pemfile`, which is unmaintained
//! (RUSTSEC-2025-0134, no safe upgrade) and refused by the repository's
//! `cargo deny` policy. PEM is parsed with `rustls-pki-types`, already in the
//! locked tree. Client certificates are mandatory, verified against the
//! configured client-CA bundle; ALPN offers only HTTP/2, as gRPC requires.

use rustls::crypto::ring::default_provider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;

pub fn server_config(
    cert_pem: &str,
    key_pem: &str,
    client_ca_pem: &str,
) -> Result<Arc<ServerConfig>, String> {
    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("server certificate: {error}"))?;
    if certs.is_empty() {
        return Err("server certificate file holds no certificate".into());
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|error| format!("server key: {error}"))?;
    let mut roots = RootCertStore::empty();
    for ca in CertificateDer::pem_slice_iter(client_ca_pem.as_bytes()) {
        roots
            .add(ca.map_err(|error| format!("client CA: {error}"))?)
            .map_err(|error| format!("client CA: {error}"))?;
    }
    if roots.is_empty() {
        return Err("client CA bundle holds no certificate".into());
    }
    let provider = Arc::new(default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|error| format!("client verifier: {error}"))?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|error| format!("server identity: {error}"))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// A completed TLS connection handed to tonic.
pub struct TlsIo(TlsStream<TcpStream>);

impl tonic::transport::server::Connected for TlsIo {
    type ConnectInfo = Option<SocketAddr>;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().0.peer_addr().ok()
    }
}

impl AsyncRead for TlsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(context, buffer)
    }
}

impl AsyncWrite for TlsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(context)
    }
}

/// Accepts TCP connections and completes handshakes concurrently; a failed
/// handshake (no or untrusted client certificate) drops only that
/// connection. Handshakes are bounded in time.
pub async fn incoming(
    address: SocketAddr,
    config: Arc<ServerConfig>,
) -> io::Result<ReceiverStream<Result<TlsIo, io::Error>>> {
    let listener = TcpListener::bind(address).await?;
    let acceptor = TlsAcceptor::from(config);
    let (sender, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                continue;
            };
            let acceptor = acceptor.clone();
            let connection = sender.clone();
            tokio::spawn(async move {
                let handshake =
                    tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp))
                        .await;
                if let Ok(Ok(stream)) = handshake {
                    let _ = connection.send(Ok(TlsIo(stream))).await;
                }
            });
            if sender.is_closed() {
                return;
            }
        }
    });
    Ok(ReceiverStream::new(receiver))
}
