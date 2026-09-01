"""
netforge.modules.speedtest
===========================

Internet speed test module for the NetForge CLI, powered by the same
unofficial fast.com / Netflix Open Connect API that backs the Go project
at https://github.com/maaslalani/fast.

How fast.com works, in short:
    1. fast.com embeds a rotating API token inside one of its JS bundles.
       We scrape the homepage for the bundle name, fetch it, and regex the
       token out. If that ever breaks, we fall back to a token that has
       historically kept working.
    2. We call ``https://api.fast.com/netflix/speedtest/v2`` with that
       token to get back a handful of Netflix Open Connect CDN URLs
       nearest to us, plus some client/ISP metadata.
    3. We hammer those URLs with parallel GET requests (download) and/or
       PUT requests (upload) for a fixed window and measure throughput.

Wire this into the main NetForge app with:

    from netforge.modules import speedtest
    app.add_typer(speedtest.app, name="speedtest")

Usage:
    netforge speedtest                     # quick download+upload test
    netforge speedtest run -d 15 -c 6       # 15s per phase, 6 connections
    netforge speedtest run --no-upload      # download only
    netforge speedtest run --json           # machine-readable output
    netforge speedtest token                # print the current API token
"""

from __future__ import annotations

import json as jsonlib
import re
import threading
import time
from dataclasses import dataclass, asdict
from typing import Optional

import httpx
import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

FAST_COM_URL = "https://fast.com/"
FAST_API_URL = "https://api.fast.com/netflix/speedtest/v2"

# Rarely changes, kept as a last-resort fallback if token scraping fails.
FALLBACK_TOKEN = "YXNkZmFzZGxmbnNkYWZoYXNkZmhrYWxm"

SCRIPT_RE = re.compile(r"app-[a-zA-Z0-9]+\.js")
TOKEN_RE = re.compile(r'token:"(\w+)"')

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) NetForge/1.0 SpeedTest"
CHUNK_SIZE = 64 * 1024
UPLOAD_PAYLOAD_SIZE = 1 * 1024 * 1024

console = Console()

app = typer.Typer(
    name="speedtest",
    help="Test your internet download/upload speed via the fast.com (Netflix) network.",
    no_args_is_help=False,
)


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------

@dataclass
class SpeedResult:
    download_mbps: float = 0.0
    upload_mbps: float = 0.0
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0
    latency_ms: Optional[float] = None
    server_count: int = 0
    client_ip: Optional[str] = None
    isp: Optional[str] = None
    location: Optional[str] = None

    def to_json(self) -> str:
        return jsonlib.dumps(asdict(self), indent=2)


class ByteCounter:
    """Thread-safe running total of bytes transferred by worker threads."""

    __slots__ = ("_total", "_lock")

    def __init__(self) -> None:
        self._total = 0
        self._lock = threading.Lock()

    def add(self, n: int) -> None:
        with self._lock:
            self._total += n

    @property
    def total(self) -> int:
        with self._lock:
            return self._total


# --------------------------------------------------------------------------
# fast.com API plumbing
# --------------------------------------------------------------------------

def _get_token(client: httpx.Client) -> str:
    """Scrape the current API token out of fast.com's JS bundle.

    Falls back to a known-working static token if the page structure has
    changed or the network call fails, so the tool degrades gracefully
    rather than hard failing.
    """
    try:
        page = client.get(FAST_COM_URL).text
        script_match = SCRIPT_RE.search(page)
        if not script_match:
            return FALLBACK_TOKEN

        script = client.get(FAST_COM_URL + script_match.group(0)).text
        token_match = TOKEN_RE.search(script)
        if not token_match:
            return FALLBACK_TOKEN

        return token_match.group(1)
    except httpx.HTTPError:
        return FALLBACK_TOKEN


def _get_targets(
    client: httpx.Client, token: str, url_count: int, https: bool
) -> dict:
    """Ask fast.com for a batch of nearby Netflix Open Connect CDN URLs.

    Returns the raw parsed JSON payload, which typically looks like::

        {
          "client": {"ip": "...", "isp": "...", "location": {...}},
          "targets": [{"url": "...", "location": {...}}, ...]
        }

    fast.com's client/isp/location fields are undocumented and can
    disappear; callers should treat them as optional.
    """
    params = {
        "https": "true" if https else "false",
        "token": token,
        "urlCount": url_count,
    }
    resp = client.get(FAST_API_URL, params=params, timeout=15.0)
    resp.raise_for_status()
    return resp.json()


def _measure_latency(client: httpx.Client, url: str) -> Optional[float]:
    """Round-trip time (ms) of a small HEAD request to the first target,
    used as a rough proxy for latency to the nearest CDN edge."""
    try:
        start = time.perf_counter()
        client.head(url, timeout=5.0)
        return (time.perf_counter() - start) * 1000
    except httpx.HTTPError:
        return None


# --------------------------------------------------------------------------
# Transfer workers
# --------------------------------------------------------------------------

def _download_worker(
    client: httpx.Client, url: str, counter: ByteCounter, stop: threading.Event
) -> None:
    while not stop.is_set():
        try:
            with client.stream("GET", url, timeout=30.0) as resp:
                for chunk in resp.iter_bytes(chunk_size=CHUNK_SIZE):
                    counter.add(len(chunk))
                    if stop.is_set():
                        break
        except httpx.HTTPError:
            if stop.is_set():
                return
            time.sleep(0.2)


def _upload_worker(
    client: httpx.Client, url: str, counter: ByteCounter, stop: threading.Event
) -> None:
    payload = b"0" * UPLOAD_PAYLOAD_SIZE

    def body_stream():
        while not stop.is_set():
            counter.add(len(payload))
            yield payload

    while not stop.is_set():
        try:
            client.put(url, content=body_stream(), timeout=30.0)
        except httpx.HTTPError:
            if stop.is_set():
                return
            time.sleep(0.2)


def _run_phase(
    urls: list[str],
    connections: int,
    duration: float,
    worker_fn,
    label: str,
    quiet: bool,
) -> tuple[float, int]:
    """Fan `connections` worker threads out across `urls` for `duration`
    seconds, live-rendering throughput, and return (mbps, total_bytes)."""
    counter = ByteCounter()
    stop = threading.Event()
    threads: list[threading.Thread] = []

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for i in range(connections):
            url = urls[i % len(urls)]
            t = threading.Thread(
                target=worker_fn, args=(client, url, counter, stop), daemon=True
            )
            threads.append(t)
            t.start()

        start = time.monotonic()

        if quiet:
            time.sleep(duration)
        else:
            with Live(console=console, refresh_per_second=8, transient=True) as live:
                while time.monotonic() - start < duration:
                    elapsed = time.monotonic() - start
                    mbps = (counter.total * 8 / 1_000_000) / elapsed if elapsed > 0 else 0.0
                    live.update(
                        Panel(
                            f"[bold]{label}[/]  [bold green]{mbps:6.2f} Mbps[/]",
                            title="NetForge SpeedTest",
                            border_style="cyan",
                        )
                    )
                    time.sleep(0.1)

        stop.set()
        elapsed = time.monotonic() - start
        for t in threads:
            t.join(timeout=1.0)

    total_bytes = counter.total
    mbps = (total_bytes * 8 / 1_000_000) / elapsed if elapsed > 0 else 0.0
    return mbps, total_bytes


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def _execute(
    duration: float,
    connections: int,
    url_count: int,
    https: bool,
    download: bool,
    upload: bool,
    quiet: bool,
) -> SpeedResult:
    result = SpeedResult()

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        if not quiet:
            console.print("[dim]Fetching API token from fast.com...[/dim]")
        token = _get_token(client)

        if not quiet:
            console.print("[dim]Locating nearest Netflix Open Connect servers...[/dim]")
        payload = _get_targets(client, token, url_count, https)

        targets = payload.get("targets", [])
        urls = [t["url"] for t in targets if "url" in t]
        if not urls:
            raise RuntimeError("fast.com returned no usable server targets.")

        result.server_count = len(urls)

        client_info = payload.get("client", {})
        result.client_ip = client_info.get("ip")
        result.isp = client_info.get("isp")
        location = client_info.get("location") or {}
        if location:
            city = location.get("city")
            country = location.get("country")
            result.location = ", ".join(p for p in (city, country) if p)

        result.latency_ms = _measure_latency(client, urls[0])

    if download:
        mbps, total = _run_phase(
            urls, connections, duration, _download_worker, "Downloading", quiet
        )
        result.download_mbps = round(mbps, 2)
        result.bytes_downloaded = total

    if upload:
        mbps, total = _run_phase(
            urls, connections, duration, _upload_worker, "Uploading", quiet
        )
        result.upload_mbps = round(mbps, 2)
        result.bytes_uploaded = total

    return result


def _render_table(result: SpeedResult) -> None:
    table = Table(title="NetForge SpeedTest Results", show_header=False, border_style="cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    if result.latency_ms is not None:
        table.add_row("Latency", f"{result.latency_ms:.1f} ms")
    if result.download_mbps:
        table.add_row("Download", f"[bold green]{result.download_mbps:.2f} Mbps[/]")
    if result.upload_mbps:
        table.add_row("Upload", f"[bold green]{result.upload_mbps:.2f} Mbps[/]")
    if result.location:
        table.add_row("Location", result.location)
    if result.isp:
        table.add_row("ISP", result.isp)
    if result.client_ip:
        table.add_row("Client IP", result.client_ip)
    table.add_row("Servers used", str(result.server_count))

    console.print(table)


# --------------------------------------------------------------------------
# CLI commands
# --------------------------------------------------------------------------

def _run_speedtest(
    duration: float = 10.0,
    connections: int = 4,
    url_count: int = 5,
    https: bool = True,
    download: bool = True,
    upload: bool = True,
    as_json: bool = False,
) -> None:
    """Actual speedtest logic, with real Python defaults.

    Kept separate from the `run` command so both the bare `netforge
    speedtest` callback and the `run` subcommand can call it directly,
    without going through click's ctx.invoke() (which would otherwise
    hand back unresolved typer.Option sentinel objects instead of
    concrete values).
    """
    if not download and not upload:
        console.print("[red]Nothing to do: both --no-download and --no-upload were set.[/red]")
        raise typer.Exit(code=1)

    try:
        result = _execute(
            duration=duration,
            connections=connections,
            url_count=url_count,
            https=https,
            download=download,
            upload=upload,
            quiet=as_json,
        )
    except (httpx.HTTPError, RuntimeError) as exc:
        if as_json:
            console.print(jsonlib.dumps({"error": str(exc)}))
        else:
            console.print(f"[red]Speed test failed:[/red] {exc}")
        raise typer.Exit(code=1)

    if as_json:
        console.print(result.to_json())
    else:
        _render_table(result)


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    """Run `netforge speedtest` with no subcommand as a shortcut for `run`."""
    if ctx.invoked_subcommand is None:
        _run_speedtest()


@app.command()
def run(
    duration: float = typer.Option(
        10.0, "--duration", "-d", help="Seconds to measure each phase for."
    ),
    connections: int = typer.Option(
        4, "--connections", "-c", min=1, max=32, help="Parallel connections per phase."
    ),
    url_count: int = typer.Option(
        5, "--url-count", "-u", min=1, max=10, help="Number of CDN URLs to request."
    ),
    https: bool = typer.Option(True, help="Use HTTPS for the test connections."),
    download: bool = typer.Option(True, "--download/--no-download", help="Run the download phase."),
    upload: bool = typer.Option(True, "--upload/--no-upload", help="Run the upload phase."),
    as_json: bool = typer.Option(False, "--json", "-j", help="Print machine-readable JSON only."),
) -> None:
    """Run a download and/or upload speed test against fast.com's network."""
    _run_speedtest(
        duration=duration,
        connections=connections,
        url_count=url_count,
        https=https,
        download=download,
        upload=upload,
        as_json=as_json,
    )


@app.command()
def token() -> None:
    """Print the API token currently scraped from fast.com (debug utility)."""
    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        console.print(_get_token(client))


@app.command()
def servers(
    url_count: int = typer.Option(5, "--url-count", "-u", min=1, max=10),
    https: bool = typer.Option(True),
) -> None:
    """List the Netflix Open Connect CDN URLs fast.com hands back right now."""
    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        tok = _get_token(client)
        payload = _get_targets(client, tok, url_count, https)

    table = Table(title="fast.com CDN Targets", border_style="cyan")
    table.add_column("#", justify="right")
    table.add_column("URL")
    for i, target in enumerate(payload.get("targets", []), start=1):
        table.add_row(str(i), target.get("url", "?"))
    console.print(table)


if __name__ == "__main__":
    app()