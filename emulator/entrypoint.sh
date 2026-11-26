#!/bin/sh
set -eu

PORT="${EMULATOR_PORT:-9297}"
export HOME=/home/emulator
export EMULATOR_BIND_ADDRESS="${EMULATOR_BIND_ADDRESS:-0.0.0.0}"

mkdir -p "${HOME}/.aws"

aws-emulator -p "${PORT}" &
pid=$!

cleanup() {
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}
trap cleanup TERM INT

files="${HOME}/.aws/emulator/${PORT}"
i=0
ready=0
while [ "$i" -lt 60 ]; do
  if [ -f "${files}/ca.pem" ] && [ -f "${files}/emulator_credentials" ]; then
    # Airflow (uid 50000) e Spark (uid 185) leem estes arquivos.
    # A chave so vale para esta execucao do emulador.
    chmod -R a+rX "${files}"
    if curl -sf --cacert "${files}/ca.pem" \
      "https://127.0.0.1:${PORT}/__admin/health" >/dev/null; then
      ready=1
      break
    fi
  fi
  i=$((i + 1))
  sleep 1
done

if [ "$ready" -ne 1 ]; then
  echo "AWS Emulator nao ficou pronto a tempo" >&2
  cleanup
  exit 1
fi

echo "AWS Emulator pronto em https://127.0.0.1:${PORT}"
wait "$pid"
