"""
Tiny no-cache static server for the PodTerminal React app.

Python's built-in `http.server` does not emit Cache-Control headers, so the
browser aggressively caches the TSX file and its in-browser Babel transpile.
During rapid dev iteration this causes stale UI after code edits.

This wrapper subclasses SimpleHTTPRequestHandler and adds `Cache-Control:
no-store, no-cache, must-revalidate` plus `Pragma: no-cache` on every
response, guaranteeing the browser re-fetches each reload.

Usage:  python bnpl-pod/web/serve_nocache.py [port]
Default port: 8765.
"""
from __future__ import annotations

import os
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class NoCacheHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    directory = os.path.dirname(os.path.abspath(__file__))
    os.chdir(directory)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), NoCacheHandler)
    print(f"no-cache static server on http://localhost:{port}/ serving {directory}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
