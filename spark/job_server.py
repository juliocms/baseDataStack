#!/usr/bin/env python3
"""Recebe um estagio do medalhao e executa o spark-submit local."""

from __future__ import annotations

import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 8091
STAGES = {"bronze", "silver", "gold"}
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.rstrip("/") == "/health":
            self._send(200, b"ok")
            return
        self._send(404, b"nao encontrado")

    def do_POST(self) -> None:
        parts = [part for part in self.path.split("/") if part]
        if len(parts) != 2 or parts[0] != "jobs" or parts[1] not in STAGES:
            self._send(404, b"use POST /jobs/bronze, /jobs/silver ou /jobs/gold")
            return
        if not LOCK.acquire(blocking=False):
            self._send(409, b"ja existe um job em execucao")
            return
        stage = parts[1]
        try:
            command = [
                "/opt/spark/bin/spark-submit",
                "--master",
                "local[1]",
                "--conf",
                "spark.driver.memory=1g",
                "--conf",
                "spark.ui.enabled=false",
                "--conf",
                "spark.eventLog.enabled=true",
                "--conf",
                "spark.eventLog.dir=file:/opt/spark-events",
                "--conf",
                "spark.ui.showConsoleProgress=false",
                "/opt/app/jobs/medallion.py",
                "--stage",
                stage,
            ]
            print(f"iniciando estagio {stage}", flush=True)
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=900,
                env=os.environ.copy(),
                check=False,
            )
            output = (completed.stdout or "") + "\n" + (completed.stderr or "")
            code = 200 if completed.returncode == 0 else 500
            self._send(code, output.encode("utf-8", errors="replace"))
        except subprocess.TimeoutExpired as exc:
            output = (exc.stdout or "") + "\n" + (exc.stderr or "") + "\ntimeout"
            self._send(500, output.encode("utf-8", errors="replace"))
        finally:
            LOCK.release()

    def _send(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        print("[job-server]", fmt % args, flush=True)


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"escutando em http://{HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
