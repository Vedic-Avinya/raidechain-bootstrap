#!/usr/bin/env bash
# Test whether the current credentials can call FCM HTTP v1 (IAM + scopes).
#
# On a GCE VM with ADC (no key file in env), uses the instance metadata token.
# Or: export GOOGLE_APPLICATION_CREDENTIALS=/path/to.json first, then install jq + curl.
#
# Usage:
#   ./verify-fcm-http-v1.sh PROJECT_ID
# Example:
#   ./verify-fcm-http-v1.sh ridechain-90ebd
#
# Expect: HTTP 400 with INVALID_ARGUMENT (bad token) means IAM is OK.
#         HTTP 403 PERMISSION_DENIED means fix IAM for the identity that obtained the token.

set -euo pipefail

PROJECT_ID="${1:-}"
if [[ -z "$PROJECT_ID" ]]; then
  echo "Usage: $0 <GCP_PROJECT_ID>" >&2
  exit 1
fi

if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is set — token will be for that key's service account, not the VM SA." >&2
fi

if command -v gcloud >/dev/null 2>&1; then
  # Respects GOOGLE_APPLICATION_CREDENTIALS if set (application-default uses user creds;
  # for SA key use: gcloud auth activate-service-account --key-file=...)
  TOKEN="$(gcloud auth application-default print-access-token 2>/dev/null || true)"
fi

if [[ -z "${TOKEN:-}" ]] && curl -fsS -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/id" >/dev/null 2>&1; then
  META_JSON="$(curl -fsS -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")"
  if command -v python3 >/dev/null 2>&1; then
    TOKEN="$(printf '%s' "$META_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"
  elif command -v jq >/dev/null 2>&1; then
    TOKEN="$(printf '%s' "$META_JSON" | jq -r .access_token)"
  else
    echo "Install python3 or jq to parse metadata token JSON." >&2
    exit 1
  fi
fi

if [[ -z "${TOKEN:-}" ]]; then
  echo "Could not get access token. On the VM try: gcloud auth application-default login" >&2
  echo "Or set GOOGLE_APPLICATION_CREDENTIALS and run: gcloud auth activate-service-account --key-file=\$GOOGLE_APPLICATION_CREDENTIALS" >&2
  exit 1
fi

URL="https://fcm.googleapis.com/v1/projects/${PROJECT_ID}/messages:send"
# Intentionally invalid token — we only care whether the API accepts the OAuth identity.
BODY='{"message":{"token":"__invalid_token_probe__","data":{"probe":"1"}}}'

CODE="$(curl -sS -o /tmp/fcm-probe-body.json -w "%{http_code}" \
  -X POST "$URL" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "$BODY" || true)"

echo "HTTP ${CODE}"
cat /tmp/fcm-probe-body.json 2>/dev/null || true
echo

if [[ "$CODE" == "403" ]]; then
  echo "→ Still IAM/scope denied. Fix the service account that owns this token (see bootstrap logs: credential_service_account or adc_service_account)." >&2
  exit 2
fi
if [[ "$CODE" == "400" ]] || [[ "$CODE" == "404" ]]; then
  echo "→ Auth accepted; FCM rejected the dummy token (expected). Server IAM for SendMessage is OK." >&2
  exit 0
fi
echo "→ Unexpected code; check body above." >&2
exit 3
