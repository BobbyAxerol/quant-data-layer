#!/usr/bin/env bash
set -euo pipefail

# Generate only the new external paper-consumer material needed by Phase 10.5-B.
# It never receives the existing CA private key and must not rotate the Kafka or
# server TLS mesh. Query/stream may later trust client-ca-bundle.crt while they
# continue serving their existing certificate.

OUTPUT_DIR="${1:?usage: phase105_prepare_external_consumer_extension.sh OUTPUT_DIR SERVER_CA_FILE}"
SERVER_CA_FILE="${2:?usage: phase105_prepare_external_consumer_extension.sh OUTPUT_DIR SERVER_CA_FILE}"
CERT_DAYS="${QDL_PHASE105_EXTERNAL_CERT_DAYS:-90}"
REQUESTED_ROLES="${QDL_PHASE105_EXTERNAL_ROLES:-monitoring,alpha-okx,reference-l2}"

if [[ ! -f "${SERVER_CA_FILE}" ]]; then
  printf 'server CA file is unavailable: %s\n' "${SERVER_CA_FILE}" >&2
  exit 64
fi
if [[ -e "${OUTPUT_DIR}" ]] && find "${OUTPUT_DIR}" -mindepth 1 -print -quit | grep -q .; then
  printf 'output directory must be empty: %s\n' "${OUTPUT_DIR}" >&2
  exit 64
fi

umask 077
mkdir -p "${OUTPUT_DIR}"
chmod 0750 "${OUTPUT_DIR}"
openssl x509 -in "${SERVER_CA_FILE}" -noout >/dev/null 2>&1

EXTERNAL_CA_KEY="${OUTPUT_DIR}/external-client-ca.key"
EXTERNAL_CA_CERT="${OUTPUT_DIR}/external-client-ca.crt"
openssl genrsa -out "${EXTERNAL_CA_KEY}" 3072 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days "${CERT_DAYS}" \
  -key "${EXTERNAL_CA_KEY}" \
  -subj '/CN=qdl-phase105b-external-client-ca' \
  -out "${EXTERNAL_CA_CERT}" >/dev/null 2>&1

issue_client() {
  local role="$1"
  local subject="$2"
  local directory="${OUTPUT_DIR}/${role}"
  local key="${directory}/client.key"
  local csr="${directory}/client.csr"
  local extension="${directory}/client.ext"

  mkdir -p "${directory}"
  openssl genrsa -out "${key}" 2048 >/dev/null 2>&1
  openssl req -new -sha256 -key "${key}" -subj "/CN=stable-${role}" \
    -out "${csr}" >/dev/null 2>&1
  cat >"${extension}" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectAltName=URI:${subject}
EOF
  openssl x509 -req -sha256 -days "${CERT_DAYS}" -in "${csr}" \
    -CA "${EXTERNAL_CA_CERT}" -CAkey "${EXTERNAL_CA_KEY}" -CAcreateserial \
    -extfile "${extension}" -out "${directory}/client.crt" >/dev/null 2>&1
  openssl verify -CAfile "${EXTERNAL_CA_CERT}" "${directory}/client.crt" >/dev/null
  cp "${SERVER_CA_FILE}" "${directory}/ca.crt"
  rm -f "${csr}" "${extension}"
  chmod 0440 "${key}"
  chmod 0444 "${directory}/client.crt" "${directory}/ca.crt"
}

issue_jwt_key() {
  local role="$1"
  local directory="${OUTPUT_DIR}/${role}-jwt"
  mkdir -p "${directory}"
  openssl genrsa -out "${directory}/private.key" 3072 >/dev/null 2>&1
  openssl pkey -in "${directory}/private.key" -pubout \
    -out "${directory}/public.pem" >/dev/null 2>&1
  chmod 0440 "${directory}/private.key"
  chmod 0444 "${directory}/public.pem"
}

IFS=',' read -r -a roles <<<"${REQUESTED_ROLES}"
if [[ "${#roles[@]}" -eq 0 ]]; then
  printf 'at least one approved external role is required\n' >&2
  exit 64
fi
declare -A subjects=(
  [monitoring]='spiffe://qdl/paper/monitoring-multivenue-stable'
  [alpha-okx]='spiffe://qdl/paper/alpha-okx-stable'
  [reference-l2]='spiffe://qdl/paper/reference-l2-stable'
)
declare -A seen=()
for role in "${roles[@]}"; do
  if [[ -z "${subjects[${role}]:-}" || -n "${seen[${role}]:-}" ]]; then
    printf 'external role list is invalid: %s\n' "${role}" >&2
    exit 64
  fi
  seen["${role}"]=1
  issue_client "${role}" "${subjects[${role}]}"
  issue_jwt_key "${role}"
done

# The first PEM is the active CA trusted by existing paper clients. The second
# is additive and signs only the newly introduced external consumers.
cat "${SERVER_CA_FILE}" "${EXTERNAL_CA_CERT}" >"${OUTPUT_DIR}/client-ca-bundle.crt"
chmod 0444 "${OUTPUT_DIR}/client-ca-bundle.crt" "${EXTERNAL_CA_CERT}"
rm -f "${EXTERNAL_CA_KEY}" "${OUTPUT_DIR}/external-client-ca.srl"

printf 'phase105 external consumer trust extension generated at %s\n' "${OUTPUT_DIR}"
