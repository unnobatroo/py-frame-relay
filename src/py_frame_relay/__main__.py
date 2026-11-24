import argparse
import logging
import signal
from pathlib import Path

from py_frame_relay.server import TelemetryServer


def main() -> None:
    parser = argparse.ArgumentParser(prog="py-frame-relay")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--log", type=Path, default=Path("telemetry.log"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with TelemetryServer(args.host, args.port, args.log) as server:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: server.stop())
        logging.info("listening on %s:%d", *server.server_address)
        server.serve_forever()


if __name__ == "__main__":
    main()
