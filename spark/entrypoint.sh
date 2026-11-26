#!/usr/bin/env bash
set -euo pipefail

CREDS="${EMULATOR_DIR:-/aws/emulator/9297}"
PORT="${EMULATOR_PORT:-9297}"

echo "Aguardando AWS Emulator em 127.0.0.1:${PORT}..."
python3 - "$CREDS" "$PORT" <<'PY'
import os
import ssl
import sys
import time
import urllib.request

creds, port = sys.argv[1], sys.argv[2]
ca = os.path.join(creds, "ca.pem")
keys = os.path.join(creds, "emulator_credentials")
url = f"https://127.0.0.1:{port}/__admin/health"

for _ in range(60):
    if os.path.isfile(ca) and os.access(ca, os.R_OK) and os.access(keys, os.R_OK):
        try:
            ctx = ssl.create_default_context(cafile=ca)
            with urllib.request.urlopen(url, context=ctx, timeout=3) as resp:
                if resp.status == 200:
                    raise SystemExit(0)
        except SystemExit:
            raise
        except Exception as exc:
            print(f"emulador ainda indisponivel: {exc}", flush=True)
    else:
        print(f"credenciais ainda ilegíveis em {creds}", flush=True)
    time.sleep(2)

print(f"AWS Emulator nao ficou pronto. Conteudo de {creds}:", flush=True)
try:
    print("\n".join(sorted(os.listdir(creds))), flush=True)
except OSError as exc:
    print(exc, flush=True)
raise SystemExit(1)
PY

rm -f /tmp/emulator-truststore.p12
keytool -importcert -noprompt \
  -alias aws-emulator \
  -file "${CREDS}/ca.pem" \
  -keystore /tmp/emulator-truststore.p12 \
  -storetype PKCS12 \
  -storepass changeit >/dev/null

export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djavax.net.ssl.trustStore=/tmp/emulator-truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit"
export AWS_ENDPOINT_URL="https://127.0.0.1:${PORT}"
export AWS_SHARED_CREDENTIALS_FILE="${CREDS}/emulator_credentials"
export AWS_CA_BUNDLE="${CREDS}/ca.pem"
export AWS_EC2_METADATA_DISABLED=true
export AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE || true

echo "Servidor de jobs PySpark em http://127.0.0.1:8091"
exec python3 /opt/app/job_server.py
