#!/usr/bin/env python3
"""
DoStyle Sync Engine — main operational entry point.

Usage:
  python sync.py run-all-sources
  python sync.py run-source renuar --limit 300
  python sync.py run-incremental zara
  python sync.py sync-loop --interval 360
  python sync.py source-status
  python sync.py refresh-source-stats
  python sync.py verify-stale-products --days 7
  python sync.py retry-failures

  python sync.py monitor              — Start monitoring UI
  python sync.py monitor --port 5000
"""
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


def start_monitor():
    """Entry point for dostyle-monitor script."""
    import argparse
    parser = argparse.ArgumentParser(description="DoStyle Monitor UI")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    from monitor.app import run_monitor
    run_monitor(args.host, args.port, args.debug)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "monitor":
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--host", default="0.0.0.0")
        parser.add_argument("--port", type=int, default=5000)
        parser.add_argument("--debug", action="store_true")
        args = parser.parse_args(sys.argv[2:])
        from monitor.app import run_monitor
        run_monitor(args.host, args.port, args.debug)
    else:
        from engine.cli.sync_commands import cli
        cli(sys.argv[1:])


if __name__ == "__main__":
    main()
