#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:?usage: phase80_generate_tls.sh OUTPUT_DIR}"
KAFKA_IMAGE="${QDL_PHASE8_KAFKA_IMAGE:-apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9}"
PASSWORD="${QDL_PHASE8_CERT_PASSWORD:-phase8-certification-only}"
CERT_UID="${QDL_PHASE8_CERT_UID:-$(id -u)}"
CERT_GID="${QDL_PHASE8_CERT_GID:-$(id -g)}"

umask 077
mkdir -p "${OUTPUT_DIR}"
chmod 0755 "${OUTPUT_DIR}"

openssl genrsa -out "${OUTPUT_DIR}/ca.key" 3072 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 2 \
  -key "${OUTPUT_DIR}/ca.key" \
  -subj "/CN=qdl-phase8-certification-ca" \
  -out "${OUTPUT_DIR}/ca.crt" >/dev/null 2>&1

issue_certificate() {
  local principal="$1"
  local dns_name="$2"
  local extension_file="${OUTPUT_DIR}/${principal}.ext"

  openssl genrsa -out "${OUTPUT_DIR}/${principal}.key" 2048 >/dev/null 2>&1
  openssl req -new -sha256 \
    -key "${OUTPUT_DIR}/${principal}.key" \
    -subj "/CN=${principal}" \
    -out "${OUTPUT_DIR}/${principal}.csr" >/dev/null 2>&1
  printf 'subjectAltName=DNS:%s,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n' "${dns_name}" >"${extension_file}"
  openssl x509 -req -sha256 -days 2 \
    -in "${OUTPUT_DIR}/${principal}.csr" \
    -CA "${OUTPUT_DIR}/ca.crt" \
    -CAkey "${OUTPUT_DIR}/ca.key" \
    -CAcreateserial \
    -extfile "${extension_file}" \
    -out "${OUTPUT_DIR}/${principal}.crt" >/dev/null 2>&1
  openssl pkcs12 -export \
    -name "${principal}" \
    -inkey "${OUTPUT_DIR}/${principal}.key" \
    -in "${OUTPUT_DIR}/${principal}.crt" \
    -certfile "${OUTPUT_DIR}/ca.crt" \
    -out "${OUTPUT_DIR}/${principal}.keystore.p12" \
    -passout "pass:${PASSWORD}" >/dev/null 2>&1
}

for broker in kafka1 kafka2 kafka3; do
  issue_certificate "${broker}" "${broker}"
done
for client in phase8-admin phase8-producer phase8-consumer phase8-core phase8-unauthorized; do
  issue_certificate "${client}" "${client}"
done

printf '%s\n' "${PASSWORD}" >"${OUTPUT_DIR}/key.password"
printf '%s\n' "${PASSWORD}" >"${OUTPUT_DIR}/store.password"
printf '%s\n' "${PASSWORD}" >"${OUTPUT_DIR}/truststore.password"

# Match the host caller so bind-mounted stores remain chmod/removal-safe on
# rootless, user-namespaced and migrated Docker hosts.
docker run --rm --user "${CERT_UID}:${CERT_GID}" \
  --mount "type=bind,source=${OUTPUT_DIR},target=/certs" \
  --entrypoint /opt/java/openjdk/bin/keytool \
  "${KAFKA_IMAGE}" \
  -importcert -noprompt -alias qdl-phase8-ca \
  -file /certs/ca.crt -keystore /certs/truststore.jks \
  -storepass "${PASSWORD}" -storetype JKS >/dev/null

write_client_properties() {
  local principal="$1"
  local output="$2"
  cat >"${OUTPUT_DIR}/${output}" <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=${PASSWORD}
ssl.truststore.type=JKS
ssl.keystore.location=/etc/kafka/secrets/${principal}.keystore.p12
ssl.keystore.password=${PASSWORD}
ssl.key.password=${PASSWORD}
ssl.keystore.type=PKCS12
ssl.endpoint.identification.algorithm=https
EOF
}

write_client_properties phase8-admin admin.properties
write_client_properties phase8-producer producer.properties
cat >>"${OUTPUT_DIR}/producer.properties" <<'EOF'
acks=all
enable.idempotence=true
retries=2147483647
max.in.flight.requests.per.connection=5
request.timeout.ms=5000
delivery.timeout.ms=15000
compression.type=zstd
EOF
write_client_properties phase8-consumer consumer.properties
cat >>"${OUTPUT_DIR}/consumer.properties" <<'EOF'
enable.auto.commit=false
isolation.level=read_committed
auto.offset.reset=earliest
EOF
write_client_properties phase8-unauthorized unauthorized.properties

find "${OUTPUT_DIR}" -type f -exec chmod 0644 {} +
# Keep only the two ephemeral PEM client keys required by the Rust transport
# smoke. The entire output directory is removed by the certification harness.
find "${OUTPUT_DIR}" -maxdepth 1 -name '*.key' \
  ! -name 'phase8-producer.key' \
  ! -name 'phase8-consumer.key' \
  ! -name 'phase8-core.key' \
  -delete
rm -f "${OUTPUT_DIR}"/*.csr "${OUTPUT_DIR}"/*.ext "${OUTPUT_DIR}"/*.srl

printf 'phase8 TLS material generated at %s\n' "${OUTPUT_DIR}"
