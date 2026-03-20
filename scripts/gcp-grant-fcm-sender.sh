#!/usr/bin/env bash
# Grant a service account permission to send FCM (HTTP v1) on a GCP project.
# Run from Cloud Shell or any machine where `gcloud` is logged in as Project Owner/Editor.
#
# Usage:
#   ./gcp-grant-fcm-sender.sh PROJECT_ID SERVICE_ACCOUNT_EMAIL
#
# Get SERVICE_ACCOUNT_EMAIL from:
#   - The "client_email" field in your Firebase / GCP JSON key on the VM, OR
#   - Bootstrap startup log: credential_service_account=... OR metadata default SA.
#
# Example:
#   ./gcp-grant-fcm-sender.sh ridechain-90ebd firebase-adminsdk-xxxxx@ridechain-90ebd.iam.gserviceaccount.com

set -euo pipefail

PROJECT_ID="${1:-}"
SA_EMAIL="${2:-}"

if [[ -z "$PROJECT_ID" || -z "$SA_EMAIL" ]]; then
  echo "Usage: $0 <PROJECT_ID> <SERVICE_ACCOUNT_EMAIL>" >&2
  exit 1
fi

if [[ "$SA_EMAIL" != *"@"* ]]; then
  echo "SERVICE_ACCOUNT_EMAIL looks invalid: $SA_EMAIL" >&2
  exit 1
fi

echo "Project:  $PROJECT_ID"
echo "Member:   serviceAccount:$SA_EMAIL"
echo ""

gcloud config set project "$PROJECT_ID"

echo "== Enabling APIs (safe if already enabled) =="
gcloud services enable fcm.googleapis.com firebase.googleapis.com --project="$PROJECT_ID"

echo ""
echo "== Granting Firebase Cloud Messaging Admin =="
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/firebasecloudmessaging.admin"

echo ""
echo "Done. Wait 1–2 minutes, then on the VM: sudo systemctl restart ridechain-bootstrap"
echo "If it still returns 403, temporarily try --role=roles/firebase.admin on the SAME email to confirm IAM is the only issue."
