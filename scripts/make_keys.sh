#!/usr/bin/env bash
# Creates two RSA key pairs in ~/.ssh (admin and loader) and prints each PUBLIC key as one line,
# ready to paste into sql/00_bootstrap_once.sql. Private keys never leave your machine.
set -euo pipefail
mkdir -p ~/.ssh
for name in tf loader; do
  key=~/.ssh/snowflake_${name}.p8
  if [[ -f "$key" ]]; then
    echo "keeping existing $key"
  else
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out "$key" -nocrypt
    chmod 600 "$key"
  fi
  echo
  echo "Public key for ${name} (paste this one line into the bootstrap SQL):"
  openssl rsa -in "$key" -pubout 2>/dev/null | grep -v -- '-----' | tr -d '\n'
  echo
done
