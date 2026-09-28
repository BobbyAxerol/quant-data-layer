#!/usr/bin/env python3
"""Create a fresh two-hour test CA and mTLS identities in PRIVATE_DIR/tls."""
import argparse
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
import os
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def create(directory):
    root = Path(directory) / "tls"
    root.mkdir(mode=0o700)
    now = datetime.now(timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "capture-test-only-ca")])

    def builder(subject, key):
        return (x509.CertificateBuilder().subject_name(subject).issuer_name(ca_name)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(minutes=1))
                .not_valid_after(now + timedelta(hours=2)))

    def write(name, data):
        descriptor = os.open(root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    ca = (builder(ca_name, ca_key).add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
          .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
              key_encipherment=False, data_encipherment=False, key_agreement=False,
              key_cert_sign=True, crl_sign=True, encipher_only=False, decipher_only=False), True)
          .sign(ca_key, hashes.SHA256()))
    write("ca.pem", ca.public_bytes(serialization.Encoding.PEM))
    for name, usage in (("server", ExtendedKeyUsageOID.SERVER_AUTH),
                        ("client", ExtendedKeyUsageOID.CLIENT_AUTH)):
        key = ec.generate_private_key(ec.SECP256R1())
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "capture-test-" + name)])
        certificate = (builder(subject, key)
                       .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
                       .add_extension(x509.ExtendedKeyUsage([usage]), False))
        if name == "server":
            certificate = certificate.add_extension(x509.SubjectAlternativeName(
                [x509.IPAddress(ip_address("127.0.0.1")), x509.DNSName("localhost")]), False)
        certificate = certificate.sign(ca_key, hashes.SHA256())
        write(name + ".pem", certificate.public_bytes(serialization.Encoding.PEM))
        write(name + ".key", key.private_bytes(serialization.Encoding.PEM,
              serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    # CA private key is deliberately never persisted.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    create(parser.parse_args().directory)
