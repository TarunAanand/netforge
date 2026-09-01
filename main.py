"""
NetForge :: entrypoint

Minimal CLI shell that wires up NetForge's modules. Drop this at the
project root (or as netforge/main.py) alongside a `modules/` package
containing speedtest.py.

Run with:
    python main.py speedtest
    python main.py speedtest run -d 15 -c 8
"""

import typer

from modules import speedtest  # noqa: adjust import path to your package layout

app = typer.Typer(
    name="netforge",
    help="NetForge - modular CLI network analysis and diagnostic tool.",
    no_args_is_help=True,
)

app.add_typer(speedtest.app, name="speedtest")

# Register additional modules here as you build them out, e.g.:
# from modules import ping, portscan, dns
# app.add_typer(ping.app, name="ping")
# app.add_typer(portscan.app, name="portscan")
# app.add_typer(dns.app, name="dns")

if __name__ == "__main__":
    app()