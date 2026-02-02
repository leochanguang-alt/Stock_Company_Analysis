#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class AkshareHandler(SimpleHTTPRequestHandler):
    def _set_headers(self, code=200, content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(204)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/akshare/fetch":
            self._set_headers(404)
            self.wfile.write(json.dumps({"error": "Not Found"}).encode("utf-8"))
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}

        symbol = str(payload.get("symbol", "")).strip()
        if not symbol:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": "Missing symbol"}).encode("utf-8"))
            return

        try:
            fetch_cmd = [sys.executable, os.path.join(ROOT_DIR, "scripts", "fetch_stock_data.py"), f"--symbol={symbol}"]
            upload_cmd = [sys.executable, os.path.join(ROOT_DIR, "scripts", "upload_stock_data.py"), f"--symbol={symbol}"]

            subprocess.run(fetch_cmd, cwd=ROOT_DIR, check=True, capture_output=True, text=True)
            subprocess.run(upload_cmd, cwd=ROOT_DIR, check=True, capture_output=True, text=True)

            self._set_headers(200)
            self.wfile.write(json.dumps({"message": f"下载并上传完成: {symbol}"}).encode("utf-8"))
        except subprocess.CalledProcessError as err:
            msg = err.stderr.strip() if err.stderr else (err.stdout.strip() if err.stdout else str(err))
            self._set_headers(500)
            self.wfile.write(json.dumps({"error": msg}).encode("utf-8"))


def main():
    os.chdir(ROOT_DIR)
    port = 8000
    httpd = HTTPServer(("0.0.0.0", port), AkshareHandler)
    print(f"AKShare fetch server running at http://localhost:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
