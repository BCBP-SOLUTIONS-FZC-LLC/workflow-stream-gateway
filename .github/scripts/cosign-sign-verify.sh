#!/usr/bin/env bash
# Signs IMAGE_NAME@DIGEST with Cosign keyless (Sigstore OIDC) and immediately
# verifies the signature against the given certificate-identity regexp.
# Usage: cosign-sign-verify.sh <certificate-identity-regexp>
# Requires IMAGE_NAME and DIGEST in the environment.
set -euo pipefail

identity_regexp="$1"
ref="${IMAGE_NAME}@${DIGEST}"

cosign sign --yes "$ref"

cosign verify "$ref" \
  --certificate-identity-regexp "$identity_regexp" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "Pushed and signed ${ref}" >> "$GITHUB_STEP_SUMMARY"
fi
