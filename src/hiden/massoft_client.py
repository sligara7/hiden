"""
MASsoft sockets client (merged).

Combines the protocol-correct CRLF framing, hot-link threads, and diagnostics from the
rewrite with backward-compatible module constants and method aliases from the working
version.

MASsoft Sockets protocol rules:

- Command and response strings are CRLF terminated.
- Read a response for every command before sending the next command.
- Use dedicated sockets for hot links; once a link is established, do not send further
  commands on that socket.
- Do not close a socket while a command is in progress; the worst-case duration is
  bounded by the "-d<t>" retry window you request.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path, PureWindowsPath
from typing import Any, Callable

LOG = logging.getLogger(__name__)
CRLF = b"\r\n"
DEFAULT_CONFIG_FILE_NAME = "hiden_config.json"

# ---------------------------------------------------------------------------
# Module-level constants (backward compat for IPython workflows)
# ---------------------------------------------------------------------------

MAS_HOST = "10.66.58.225"
MAS_PORT = 5026
EXPERIMENT_DIRECTORY = r"C:\Users\08id-user\Documents\Hiden Analytical\MASsoft\11"
EXPERIMENT_DIRECTORY_ENV = "HIDEN_FilePath"
MOST_RECENT_FILE = "HIDEN_LastFile"
TEMPLATE_DICT = {
    "exp1": "HIDEN_1.exp",
    "exp2": "HIDEN_2.exp",
    "exp3": "HIDEN_3.exp",
    "exp4": "HIDEN_4.exp",
}


# ---------------------------------------------------------------------------
# Runtime config helpers
# ---------------------------------------------------------------------------


def get_runtime_config_path(config_path: str | None = None) -> Path:
    """
    Resolve runtime config path.

    Priority:
      1) explicit ``config_path``
      2) ``HIDEN_CONFIG`` environment variable
      3) ``<this module directory>/hiden_config.json``
    """
    if config_path:
        return Path(config_path).expanduser()

    env_path = os.getenv("HIDEN_CONFIG")
    if env_path:
        return Path(env_path).expanduser()

    return Path(__file__).with_name(DEFAULT_CONFIG_FILE_NAME)


def load_runtime_config(config_path: str | None = None) -> dict[str, Any]:
    """Load runtime JSON config.  Returns empty dict when file is missing."""
    path = get_runtime_config_path(config_path=config_path)
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        msg = f"Invalid JSON in runtime config: {path}"
        raise ValueError(msg) from exc

    if not isinstance(raw, dict):
        msg = f"Runtime config must be a JSON object: {path}"
        raise ValueError(msg)

    return raw


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MASsoftError(Exception):
    """Base exception for MASsoft socket client errors."""


class MASsoftDisconnected(MASsoftError, ConnectionError):
    """Raised when the remote side closes the socket or the socket is unusable."""


class MASsoftTimeout(MASsoftError, TimeoutError):
    """Raised when a read/write exceeds the requested timeout."""


class MASsoftProtocolError(MASsoftError):
    """Raised when MASsoft returns '0' (failure) or protocol framing is violated."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MASsoftConfig:
    beamline_name: str = "08IDB"
    host: str = "10.66.58.225"
    port: int = 5026

    # Directory used when ``open_experiment("file56.exp")`` is called.
    experiment_directory: str = (
        r"C:\Users\08id-user\Documents\Hiden Analytical\MASsoft\11"
    )

    # Default MASsoft "-d<t>" retry window for commands that support it.
    retry_s: int = 15

    # IMPORTANT: must be >= retry_s (+ margin), otherwise you can time out locally
    # while MASsoft continues retrying and later sends a "late" response that
    # desynchronizes the stream.
    command_timeout_s: float = 20.0

    # Per-recv timeout used by link reader threads.
    link_chunk_timeout_s: float = 1.0

    # Group multiple CRLF lines that arrive in a short burst into one callback.
    link_burst_gap_s: float = 0.10

    enable_keepalive: bool = True

    @classmethod
    def from_runtime_config(cls, config_path: str | None = None) -> MASsoftConfig:
        """Build config from runtime JSON."""
        raw = load_runtime_config(config_path=config_path)
        massoft = raw.get("massoft", {})
        if not isinstance(massoft, dict):
            msg = "runtime config key 'massoft' must be an object"
            raise ValueError(msg)

        def _get(key: str, default: Any) -> Any:
            if key in massoft:
                return massoft[key]
            return raw.get(key, default)

        return cls(
            beamline_name=str(raw.get("beamline_name", cls.beamline_name)),
            host=str(_get("host", cls.host)),
            port=int(_get("port", cls.port)),
            experiment_directory=str(
                _get("experiment_directory", cls.experiment_directory)
            ),
            retry_s=int(_get("retry_s", cls.retry_s)),
            command_timeout_s=float(_get("command_timeout_s", cls.command_timeout_s)),
            link_chunk_timeout_s=float(
                _get("link_chunk_timeout_s", cls.link_chunk_timeout_s)
            ),
            link_burst_gap_s=float(_get("link_burst_gap_s", cls.link_burst_gap_s)),
            enable_keepalive=bool(_get("enable_keepalive", cls.enable_keepalive)),
        )


# ---------------------------------------------------------------------------
# Low-level CRLF framed socket
# ---------------------------------------------------------------------------


class _CRLFSocket:
    """
    A single TCP connection to MASsoft with CRLF framed reads.

    - ``request(...)`` is serialized by a lock so command/response pairs cannot
      interleave.
    - For hot-links, call ``send(...)`` once, then only call ``read_line(...)``
      from one thread.
    """

    def __init__(self, host: str, port: int, *, name: str, timeout_s: float):
        self.host = host
        self.port = port
        self.name = name
        self._timeout_s = float(timeout_s)

        self._sock: socket.socket | None = None
        self._buf = bytearray()
        self._req_lock = threading.Lock()

    def connect(self, *, enable_keepalive: bool = True) -> None:
        self.close()

        sock = socket.create_connection((self.host, self.port), timeout=self._timeout_s)
        sock.settimeout(self._timeout_s)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if enable_keepalive:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        self._sock = sock
        self._buf.clear()

        # Best-effort: consume the greeting if it arrives quickly.
        try:
            _ = self.read_line(timeout_s=1.0)
        except MASsoftTimeout:
            pass
        except MASsoftDisconnected:
            raise

        LOG.info("%s connected to %s:%d", self.name, self.host, self.port)

    def close(self) -> None:
        sock = self._sock
        self._sock = None
        self._buf.clear()
        if sock is None:
            return

        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            sock.close()
        LOG.info("%s closed", self.name)

    def is_connected(self) -> bool:
        return self._sock is not None

    def send(self, command: str, *, retry_s: int | None = None) -> None:
        if self._sock is None:
            msg = f"{self.name}: not connected"
            raise MASsoftDisconnected(msg)

        cmd = command.strip()

        # Append -d<t> only if requested and not already present.
        if retry_s is not None and " -d" not in cmd:
            cmd = f"{cmd} -d{int(retry_s)}"

        wire = (cmd + "\r\n").encode("utf-8")
        try:
            self._sock.sendall(wire)
        except OSError as exc:
            msg = f"{self.name}: send failed: {exc!r}"
            raise MASsoftDisconnected(msg) from exc

    def read_line(self, *, timeout_s: float | None = None) -> str:
        if self._sock is None:
            msg = f"{self.name}: not connected"
            raise MASsoftDisconnected(msg)

        sock = self._sock
        old_timeout = sock.gettimeout()
        if timeout_s is not None:
            sock.settimeout(float(timeout_s))

        try:
            while True:
                idx = self._buf.find(CRLF)
                if idx != -1:
                    raw = bytes(self._buf[:idx])
                    del self._buf[: idx + 2]
                    return raw.decode("utf-8", errors="replace")

                try:
                    chunk = sock.recv(4096)
                except socket.timeout as exc:
                    msg = f"{self.name}: read timed out"
                    raise MASsoftTimeout(msg) from exc
                except OSError as exc:
                    msg = f"{self.name}: read failed: {exc!r}"
                    raise MASsoftDisconnected(
                        msg
                    ) from exc

                if chunk == b"":
                    msg = f"{self.name}: remote closed connection"
                    raise MASsoftDisconnected(msg)

                self._buf.extend(chunk)
        finally:
            if timeout_s is not None:
                sock.settimeout(old_timeout)

    def request(
        self,
        command: str,
        *,
        retry_s: int | None = None,
        timeout_s: float | None = None,
    ) -> str:
        """Send one command and read one CRLF-terminated response line."""
        with self._req_lock:
            self.send(command, retry_s=retry_s)
            return self.read_line(timeout_s=timeout_s)

    # -------------------------------------------------------------------
    # Compat methods (working-version API)
    # -------------------------------------------------------------------

    def send_command(self, command: str, expect_response: bool = True) -> str:
        """Compat: send command with ``-d20`` retry and return response.

        Returns ``""`` on timeout.  Raises on disconnect so callers can
        trigger reconnection logic.
        """
        if not expect_response:
            self.send(command, retry_s=20)
            return ""
        try:
            return self.request(command, retry_s=20, timeout_s=25.0)
        except MASsoftTimeout:
            LOG.warning("%s: timeout for: %s", self.name, command.strip())
            return ""

    def receive(self) -> str:
        """Compat: read one CRLF line, return ``""`` on timeout/disconnect."""
        try:
            return self.read_line()
        except (MASsoftTimeout, MASsoftDisconnected):
            return ""


# ---------------------------------------------------------------------------
# Hot-link subscription helper
# ---------------------------------------------------------------------------


class MASsoftHotlink:
    """
    A dedicated listening socket reading a MASsoft ``-l<Item>`` hot link.

    The callback is invoked with a *burst* of one or more lines.  Bursts are
    separated by idle gaps > ``burst_gap_s``.
    """

    def __init__(
        self,
        sock: _CRLFSocket,
        *,
        chunk_timeout_s: float,
        burst_gap_s: float,
        on_burst: Callable[[list[str]], None],
        name: str,
    ):
        self._sock = sock
        self._chunk_timeout_s = float(chunk_timeout_s)
        self._burst_gap_s = float(burst_gap_s)
        self._on_burst = on_burst
        self._name = name

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        t = threading.Thread(target=self._run, name=self._name, daemon=True)
        self._thread = t
        t.start()

    def stop(self, *, join_timeout_s: float = 2.0) -> None:
        self._stop.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=float(join_timeout_s))
        self._thread = None

    def _run(self) -> None:
        pending: list[str] = []
        last_rx: float | None = None

        def _emit_pending() -> None:
            nonlocal pending, last_rx
            if not pending:
                return
            try:
                self._on_burst(pending)
            except Exception:
                LOG.exception("%s callback error", self._name)
            pending = []
            last_rx = None

        while not self._stop.is_set():
            try:
                line = self._sock.read_line(timeout_s=self._chunk_timeout_s)
                s = line.strip()
                if s:
                    now = time.monotonic()
                    if (
                        pending
                        and last_rx is not None
                        and (now - last_rx) >= self._burst_gap_s
                    ):
                        _emit_pending()
                    pending.append(s)
                    last_rx = now
            except MASsoftTimeout:
                if pending and last_rx is not None and (time.monotonic() - last_rx) >= self._burst_gap_s:
                    _emit_pending()
            except MASsoftDisconnected as exc:
                LOG.warning("%s disconnected: %s", self._name, exc)
                break
            except Exception:
                LOG.exception("%s reader error", self._name)
                break

            if pending and last_rx is not None and self._burst_gap_s <= 0:
                _emit_pending()

        # Flush any remaining lines
        if pending:
            _emit_pending()


# ---------------------------------------------------------------------------
# High-level MASsoft client
# ---------------------------------------------------------------------------


class MASsoftClient:
    """
    High-level MASsoft client.

    Accepts multiple constructor styles for backward compatibility::

        MASsoftClient()                        # load from config file
        MASsoftClient(cfg)                     # MASsoftConfig instance
        MASsoftClient(host="...", port=5026)   # working-version keyword style
    """

    def __init__(
        self,
        cfg: MASsoftConfig | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        config_path: str | None = None,
    ):
        if isinstance(cfg, MASsoftConfig):
            self.cfg = cfg
        elif cfg is None:
            base = MASsoftConfig.from_runtime_config(config_path=config_path)
            if host is not None or port is not None:
                overrides: dict[str, Any] = {}
                if host is not None:
                    overrides["host"] = host
                if port is not None:
                    overrides["port"] = port
                self.cfg = replace(base, **overrides)
            else:
                self.cfg = base
        else:
            msg = f"Expected MASsoftConfig, None, or keyword host=/port=; got {type(cfg)}"
            raise TypeError(
                msg
            )

        # Use self.cfg (not cfg) -- fixes rewrite bug where cfg could be None.
        self.command = _CRLFSocket(
            self.cfg.host,
            self.cfg.port,
            name="MASsoftCommand",
            timeout_s=self.cfg.command_timeout_s,
        )
        self.status_sock = _CRLFSocket(
            self.cfg.host,
            self.cfg.port,
            name="MASsoftStatus",
            timeout_s=self.cfg.link_chunk_timeout_s,
        )
        self.data_sock = _CRLFSocket(
            self.cfg.host,
            self.cfg.port,
            name="MASsoftData",
            timeout_s=self.cfg.link_chunk_timeout_s,
        )

        self.current_file: str | None = None
        self._command_assoc_file: str | None = None
        self._status_assoc_file: str | None = None
        self._data_assoc_file: str | None = None
        self._assoc_fallback_logged: set[str] = set()

        self._status_link: MASsoftHotlink | None = None
        self._data_link: MASsoftHotlink | None = None

        self._latest_status_lock = threading.Lock()
        self._latest_status: str | None = None
        self._latest_status_ts = 0.0

        self._latest_row_lock = threading.Lock()
        self._latest_row: list[float] | None = None
        self._latest_row_ts = 0.0
        self._latest_raw_row: str | None = None
        self._latest_raw_row_ts = 0.0

        self._last_error_lock = threading.Lock()
        self._last_error: str | None = None

    # -------------------------------------------------------------------
    # Socket attribute aliases (working-version names)
    # -------------------------------------------------------------------

    @property
    def command_socket(self) -> _CRLFSocket:
        return self.command

    @property
    def status_socket(self) -> _CRLFSocket:
        return self.status_sock

    @property
    def data_socket(self) -> _CRLFSocket:
        return self.data_sock

    # -------------------------------------------------------------------
    # Connection lifecycle
    # -------------------------------------------------------------------

    def connect(self) -> None:
        """Connect all sockets (command + link sockets)."""
        self.command.connect(enable_keepalive=self.cfg.enable_keepalive)
        self.status_sock.connect(enable_keepalive=self.cfg.enable_keepalive)
        self.data_sock.connect(enable_keepalive=self.cfg.enable_keepalive)

    def disconnect(self) -> None:
        """Stop hot-links and close sockets."""
        self.stop_links()
        self.command.close()
        self.status_sock.close()
        self.data_sock.close()
        self.current_file = None
        self._command_assoc_file = None
        self._status_assoc_file = None
        self._data_assoc_file = None
        self._assoc_fallback_logged.clear()

    def initialize(self) -> None:
        """Compat: connect all sockets.  Delegates to ``connect()``."""
        self.connect()

    def shutdown(self) -> None:
        """Compat: close all sockets.  Delegates to ``disconnect()``."""
        self.disconnect()

    # -------------------------------------------------------------------
    # File open / association
    # -------------------------------------------------------------------

    def _resolve_path(self, file_name_or_path: str) -> str:
        if (
            file_name_or_path.startswith(("%", "\\\\")) or ":" in file_name_or_path
        ):
            return file_name_or_path
        return str(PureWindowsPath(self.cfg.experiment_directory) / file_name_or_path)

    def open_experiment(
        self, file_name_or_path: str | None = None, *, retry_s: int | None = None
    ) -> str:
        """
        Open/associate an experiment file on all sockets.

        When *file_name_or_path* is ``None``, queries MASsoft for the current
        filename (like the working version).  Lists/tuples are unpacked for
        backward compatibility.
        """
        if retry_s is None:
            retry_s = self.cfg.retry_s

        if file_name_or_path is None:
            path = self.query_filename(retry_s=retry_s, update_current=True)
        else:
            if isinstance(file_name_or_path, (list, tuple)):
                file_name_or_path = file_name_or_path[0]
            path = self._resolve_path(file_name_or_path)

        r = self.command.request(
            f'-f"{path}"',
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = f"MASsoft failed to open/associate: {path}"
            raise MASsoftProtocolError(msg)
        self._command_assoc_file = path

        for s in (self.status_sock, self.data_sock):
            rr = s.request(
                f'-f"{path}"',
                retry_s=retry_s,
                timeout_s=self.cfg.command_timeout_s,
            ).strip()
            if rr == "0":
                msg = f"MASsoft failed to associate {s.name} with: {path}"
                raise MASsoftProtocolError(
                    msg
                )
            if s is self.status_sock:
                self._status_assoc_file = path
            elif s is self.data_sock:
                self._data_assoc_file = path

        self.current_file = path
        return path

    # Compat alias used by the IOC (cap2.py)
    open_experiment_commands = open_experiment

    def open_experiment_data(self, file_name: str | None = None) -> str:
        """Compat: associate an experiment file on the data socket only."""
        if file_name is None:
            path = self.query_filename_data()
        else:
            if isinstance(file_name, (list, tuple)):
                file_name = file_name[0]
            path = self._resolve_path(file_name)
        r = self.data_sock.request(
            f'-f"{path}"',
            retry_s=self.cfg.retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = f"Failed to associate data socket: {path}"
            raise MASsoftProtocolError(msg)
        self._data_assoc_file = path
        self.current_file = path
        return path

    def open_experiment_status(self, file_name: str | None = None) -> str:
        """Compat: associate an experiment file on the status socket only."""
        if file_name is None:
            path = self.query_filename(update_current=False)
        else:
            if isinstance(file_name, (list, tuple)):
                file_name = file_name[0]
            path = self._resolve_path(file_name)
        r = self.status_sock.request(
            f'-f"{path}"',
            retry_s=self.cfg.retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = f"Failed to associate status socket: {path}"
            raise MASsoftProtocolError(msg)
        self._status_assoc_file = path
        self.current_file = path
        return path

    def query_filename(
        self, *, retry_s: int | None = None, update_current: bool = True
    ) -> str:
        """Return the filename currently associated with the command socket."""
        if retry_s is None:
            retry_s = self.cfg.retry_s

        path = self.command.request(
            "-xFilename",
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if path == "0" or not path:
            msg = "MASsoft refused -xFilename (returned 0/empty)"
            raise MASsoftProtocolError(msg)

        path = path.strip().strip('"')
        if not path:
            msg = "MASsoft returned an empty filename for -xFilename"
            raise MASsoftProtocolError(
                msg
            )

        if update_current:
            self.current_file = path

        return path

    def query_filename_data(self) -> str:
        """Compat: return the filename currently associated with the data socket."""
        resp = self.data_sock.request(
            "-xFilename",
            retry_s=self.cfg.retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if resp in ("0", ""):
            msg = "Failed to retrieve filename from data socket"
            raise MASsoftProtocolError(msg)
        return resp.strip('"')

    def _associate_socket_with_active_file(
        self,
        sock: _CRLFSocket,
        *,
        retry_s: int | None = None,
    ) -> str:
        """Associate a socket with the currently active experiment file."""
        if retry_s is None:
            retry_s = self.cfg.retry_s

        if sock is self.command:
            fallback_assoc = self._command_assoc_file
        elif sock is self.status_sock:
            fallback_assoc = self._status_assoc_file
        elif sock is self.data_sock:
            fallback_assoc = self._data_assoc_file
        else:
            fallback_assoc = None

        active_file = self.current_file
        try:
            active_file = self.query_filename(retry_s=retry_s, update_current=True)
        except Exception:
            if not active_file:
                raise

        rr = sock.request(
            f'-f"{active_file}"',
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if rr != "0":
            if sock is self.command:
                self._command_assoc_file = active_file
            elif sock is self.status_sock:
                self._status_assoc_file = active_file
            elif sock is self.data_sock:
                self._data_assoc_file = active_file
            self._assoc_fallback_logged.discard(sock.name)
            return active_file

        if fallback_assoc:
            msg = (
                f"{sock.name} association to active file failed; "
                f"keeping existing association: {fallback_assoc}"
            )
            if sock.name not in self._assoc_fallback_logged:
                LOG.info(msg)
                self._assoc_fallback_logged.add(sock.name)
            return fallback_assoc

        msg = f"MASsoft failed to associate {sock.name} with: {active_file}"
        raise MASsoftProtocolError(
            msg
        )

    # -------------------------------------------------------------------
    # Execute (-x*) commands
    # -------------------------------------------------------------------

    def x_status(self, *, retry_s: int | None = None) -> str:
        """Return current MSIU status via ``-xStatus``."""
        if retry_s is None:
            retry_s = self.cfg.retry_s
        return self.command.request(
            "-xStatus",
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()

    def x_go(
        self,
        *,
        filename: str | None = None,
        od: bool = True,
        ot: bool = True,
        retry_s: int | None = None,
    ) -> None:
        """Start the experiment via ``-xGo``."""
        if retry_s is None:
            retry_s = self.cfg.retry_s

        o_flags = ""
        if od or ot:
            o_flags = "-O" + ("d" if od else "") + ("t" if ot else "")

        parts = ["-xGo"]
        if filename:
            parts.append(filename)
        if o_flags:
            parts.append(o_flags)

        cmd = " ".join(parts)
        r = self.command.request(
            cmd,
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = "MASsoft refused -xGo (returned 0)"
            raise MASsoftProtocolError(msg)

        with contextlib.suppress(Exception):
            self.query_filename(retry_s=retry_s, update_current=True)

    def x_abort(self, *, retry_s: int | None = None) -> None:
        """Abort acquisition via ``-xAbort``."""
        if retry_s is None:
            retry_s = self.cfg.retry_s
        r = self.command.request(
            "-xAbort",
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = "MASsoft refused -xAbort (returned 0)"
            raise MASsoftProtocolError(msg)

    def x_close(self, *, retry_s: int | None = None) -> None:
        """Close the experiment via ``-xClose``."""
        if retry_s is None:
            retry_s = self.cfg.retry_s

        r = self.command.request(
            "-xClose",
            retry_s=retry_s,
            timeout_s=self.cfg.command_timeout_s,
        ).strip()
        if r == "0":
            msg = "MASsoft refused -xClose (returned 0)"
            raise MASsoftProtocolError(msg)

        self.stop_links()
        self.current_file = None
        self._command_assoc_file = None
        self._status_assoc_file = None
        self._data_assoc_file = None
        self._assoc_fallback_logged.clear()

    def safe_abort_and_wait(self, *, timeout_s: float = 30.0) -> str:
        """Abort and wait until status becomes Stopped*."""
        self.x_abort()

        deadline = time.monotonic() + float(timeout_s)
        while time.monotonic() < deadline:
            st = self.get_latest_status()
            if st and st.lower().startswith("stopped"):
                return st
            try:
                st2 = self.x_status()
                if st2 and st2.lower().startswith("stopped"):
                    self._set_latest_status(st2)
                    return st2
            except Exception:
                pass
            time.sleep(0.2)

        msg = f"Timed out waiting for stopped status after abort ({timeout_s}s)"
        raise MASsoftTimeout(
            msg
        )

    def safe_abort_and_close(
        self,
        *,
        abort_timeout_s: float = 30.0,
        close_retry_s: int | None = None,
        reconnect: bool = False,
    ) -> str:
        """Safely stop (if running), then close the experiment file."""
        if close_retry_s is None:
            close_retry_s = self.cfg.retry_s

        st = self.get_latest_status()
        if not st:
            try:
                st = self.x_status()
            except Exception:
                st = None

        def _is_running(s: str) -> bool:
            ss = s.strip().lower()
            return (
                ss.startswith(("starting", "scanning", "stopping")) or ss == "degas"
            )

        final_status = st or ""

        if st and _is_running(st):
            final_status = self.safe_abort_and_wait(timeout_s=abort_timeout_s)
        elif st is None:
            with contextlib.suppress(Exception):
                final_status = self.safe_abort_and_wait(timeout_s=abort_timeout_s)

        try:
            self.x_close(retry_s=close_retry_s)
        finally:
            with contextlib.suppress(Exception):
                self.disconnect()

            if reconnect:
                self.connect()

        return final_status or (self.get_latest_status() or "")

    # -------------------------------------------------------------------
    # Compat execute methods (working-version API)
    # -------------------------------------------------------------------

    def run_experiment(self, mode: str = "-Odt") -> None:
        """Compat: start the experiment.  Delegates to ``x_go()``."""
        od = "d" in mode if mode else False
        ot = "t" in mode if mode else False
        self.x_go(od=od, ot=ot)

    def abort_experiment(self) -> None:
        """Compat: abort the experiment.  Delegates to ``x_abort()``."""
        self.x_abort()

    def close_experiment(self) -> None:
        """Compat: close the experiment.  Delegates to ``x_close()``."""
        self.x_close()

    # -------------------------------------------------------------------
    # Links (-l*) for status/data
    # -------------------------------------------------------------------

    def start_status_link(self, *, view: int = 1) -> None:
        """Start a ``-lStatus`` hot-link on the dedicated status socket."""
        if self.current_file is None:
            msg = "No experiment is associated. Call open_experiment(...) first."
            raise RuntimeError(
                msg
            )

        self._associate_socket_with_active_file(
            self.status_sock, retry_s=self.cfg.retry_s
        )

        self.status_sock.send(f"-lStatus -v{int(view)}", retry_s=self.cfg.retry_s)

        def _on(lines: list[str]) -> None:
            for line in lines:
                if line == "0":
                    self._set_last_error(
                        "Status link returned '0' (view missing or MASsoft busy)"
                    )
                    continue
                self._set_latest_status(line)

        self._status_link = MASsoftHotlink(
            self.status_sock,
            chunk_timeout_s=self.cfg.link_chunk_timeout_s,
            burst_gap_s=self.cfg.link_burst_gap_s,
            on_burst=_on,
            name="MASsoftStatusHotlink",
        )
        self._status_link.start()

    def start_data_link(
        self,
        *,
        view: int = 1,
        mid_cycles: int = 1,
        include_time: bool = False,
        include_ms: bool = False,
    ) -> None:
        """Start a ``-lData`` hot-link on the dedicated data socket."""
        if self.current_file is None:
            msg = "No experiment is associated. Call open_experiment(...) first."
            raise RuntimeError(
                msg
            )

        self._associate_socket_with_active_file(
            self.data_sock, retry_s=self.cfg.retry_s
        )

        c = max(1, int(mid_cycles))
        t = 1 if include_time else 0
        m = 1 if include_ms else 0

        cmd = f"-lData -v{int(view)}"
        if c != 1 or t != 0 or m != 0:
            cmd += f" -c{c} -t{t} -m{m}"
        self.data_sock.send(cmd, retry_s=self.cfg.retry_s)

        drop = (1 if include_time else 0) + (1 if include_ms else 0)

        def _to_float(tok: str) -> float | None:
            try:
                return float(tok)
            except Exception:
                return None

        def _on(lines: list[str]) -> None:
            last_row: list[float] | None = None
            last_raw_line: str | None = None
            for line in lines:
                if not line:
                    continue
                if line == "0":
                    self._set_last_error(
                        "Data link returned '0' (view missing or MASsoft busy)"
                    )
                    continue
                last_raw_line = line
                parts = line.split()

                start = 0
                if parts and ":" in parts[0]:
                    start = 1
                start += drop
                if start >= len(parts):
                    continue

                numeric: list[float] = []
                for p in parts[start:]:
                    v = _to_float(p)
                    if v is None:
                        continue
                    numeric.append(v)

                if not numeric:
                    self._set_last_error(f"Unparseable MID data line: {line!r}")
                    continue

                if not include_ms and len(numeric) >= 2:
                    first_raw = parts[start]
                    second_raw = parts[start + 1] if (start + 1) < len(parts) else ""
                    first_is_intish = first_raw.isdigit()
                    second_is_scan_like = ("e" in second_raw.lower()) or (
                        "." in second_raw
                    )
                    if first_is_intish and second_is_scan_like:
                        numeric = numeric[1:]

                if not numeric:
                    continue
                last_row = numeric

            if last_row is not None:
                with self._latest_row_lock:
                    self._latest_row = last_row
                    self._latest_row_ts = time.monotonic()
            if last_raw_line is not None:
                with self._latest_row_lock:
                    self._latest_raw_row = last_raw_line
                    self._latest_raw_row_ts = time.monotonic()

        self._data_link = MASsoftHotlink(
            self.data_sock,
            chunk_timeout_s=self.cfg.link_chunk_timeout_s,
            burst_gap_s=0.0,
            on_burst=_on,
            name="MASsoftDataHotlink",
        )
        self._data_link.start()

    def stop_links(self) -> None:
        """Stop status/data hot-links (does not close sockets)."""
        if self._status_link is not None:
            self._status_link.stop()
            self._status_link = None
        if self._data_link is not None:
            self._data_link.stop()
            self._data_link = None

    # -------------------------------------------------------------------
    # One-shot metadata
    # -------------------------------------------------------------------

    def fetch_legends(self, *, view: int = 1) -> list[str]:
        """Fetch ``-lLegends`` once on a temporary socket."""
        if self.current_file is None:
            msg = "No experiment is associated. Call open_experiment(...) first."
            raise RuntimeError(
                msg
            )

        active_file = self.query_filename(update_current=True)

        tmp = _CRLFSocket(
            self.cfg.host,
            self.cfg.port,
            name="MASsoftLegendsTmp",
            timeout_s=self.cfg.command_timeout_s,
        )
        try:
            tmp.connect(enable_keepalive=self.cfg.enable_keepalive)
            r = tmp.request(
                f'-f"{active_file}"',
                retry_s=self.cfg.retry_s,
                timeout_s=self.cfg.command_timeout_s,
            ).strip()
            if r == "0":
                msg = "Failed to associate temp legends socket with experiment"
                raise MASsoftProtocolError(
                    msg
                )

            line = tmp.request(
                f"-lLegends -v{int(view)}",
                retry_s=self.cfg.retry_s,
                timeout_s=self.cfg.command_timeout_s,
            ).strip()
            if line == "0":
                msg = "MASsoft refused -lLegends (returned 0)"
                raise MASsoftProtocolError(msg)

            if "\t" in line:
                return [p.strip().strip('"') for p in line.split("\t") if p.strip()]
            return [line]
        finally:
            tmp.close()

    def get_legends(self, view: int = 1) -> tuple[list[str], str]:
        """Compat: return ``(legend_list, path)`` tuple."""
        path = self.query_filename(update_current=True)
        legends = self.fetch_legends(view=view)
        return legends, path

    # -------------------------------------------------------------------
    # Readback helpers (thread-safe accessors)
    # -------------------------------------------------------------------

    def _set_latest_status(self, status: str) -> None:
        with self._latest_status_lock:
            self._latest_status = status
            self._latest_status_ts = time.monotonic()

    def get_latest_status(self) -> str | None:
        with self._latest_status_lock:
            return self._latest_status

    def get_latest_status_timestamp(self) -> float:
        with self._latest_status_lock:
            return self._latest_status_ts

    def get_latest_row(self) -> list[float] | None:
        with self._latest_row_lock:
            return list(self._latest_row) if self._latest_row is not None else None

    def get_latest_row_timestamp(self) -> float:
        with self._latest_row_lock:
            return self._latest_row_ts

    def get_latest_raw_line(self) -> str | None:
        with self._latest_row_lock:
            return self._latest_raw_row

    def get_latest_raw_line_timestamp(self) -> float:
        with self._latest_row_lock:
            return self._latest_raw_row_ts

    def _set_last_error(self, msg: str) -> None:
        with self._last_error_lock:
            self._last_error = msg

    def get_last_error(self) -> str | None:
        with self._last_error_lock:
            return self._last_error
