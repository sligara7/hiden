"""
Caproto IOC for Hiden RGA via MASsoft sockets (merged).

Combines the working version's reliability patterns (eager connect, polling
fallback with reconnect retry) with the rewrite's protocol-correct hot-link
threads and diagnostic PVs.

PV names use the working-version convention: RunExp, AbortExp, CloseExp.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
import time

from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run

from .massoft_client import MASsoftClient, load_runtime_config

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)
logging.getLogger("caproto").propagate = False
logging.getLogger("caproto.ctx").propagate = False
LOG = logging.getLogger(__name__)

# Runtime config for IOC defaults
_RUNTIME_CFG = load_runtime_config()
_IOC_CFG = _RUNTIME_CFG.get("ioc", {}) if isinstance(_RUNTIME_CFG, dict) else {}
if not isinstance(_IOC_CFG, dict):
    _IOC_CFG = {}


def _ioc_default(name: str, default):
    val = _IOC_CFG.get(name, default)
    return default if val is None else val


class RGAIOC(PVGroup):
    # -----------------------------------------------------------------
    # Control PVs (working-version names)
    # -----------------------------------------------------------------

    open_exp = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:OpenExp",
        value=0,
        dtype=int,
        doc="Write 1 to connect + open/associate the experiment + fetch legends",
    )

    experiment = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:ExpName",
        value=str(_ioc_default("default_experiment", "file56.exp")),
        dtype=str,
        max_length=256,
        doc="Experiment file name (relative to MASsoft experiment directory) or full path",
    )

    acquire = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:Acquire",
        value=0,
        dtype=int,
        doc="Start/stop the acquisition loop",
    )

    run_exp = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:RunExp",
        value=0,
        dtype=int,
        doc="Write 1 to start the experiment",
    )

    abort_exp = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:AbortExp",
        value=0,
        dtype=int,
        doc="Write 1 to abort the running experiment",
    )

    close_exp = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:CloseExp",
        value=0,
        dtype=int,
        doc="Write 1 to close the experiment file",
    )

    # -----------------------------------------------------------------
    # Configuration PVs (from rewrite)
    # -----------------------------------------------------------------

    view = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:View",
        value=max(1, int(_ioc_default("default_view", 1))),
        dtype=int,
        doc="MASsoft view number (used for -lStatus/-lData/-lLegends)",
    )

    go_od = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:GoOD",
        value=1 if int(_ioc_default("default_go_od", 1)) else 0,
        dtype=int,
        doc="When 1, include 'd' in -O flags (create date directory).",
    )

    go_ot = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:GoOT",
        value=1 if int(_ioc_default("default_go_ot", 1)) else 0,
        dtype=int,
        doc="When 1, include 't' in -O flags (time-based filename).",
    )

    go_filename = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:GoFilename",
        value=str(_ioc_default("default_go_filename", "")),
        dtype=str,
        max_length=256,
        doc="Optional filename argument to -xGo. If blank, MASsoft uses its defaults.",
    )

    # -----------------------------------------------------------------
    # Diagnostic PVs (from rewrite)
    # -----------------------------------------------------------------

    connected = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:Connected",
        value=0,
        dtype=int,
        doc="1 if client has connected sockets and associated a file; 0 otherwise.",
    )

    status = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:Status",
        value="Disconnected",
        dtype=str,
        max_length=32,
        doc="Last MASsoft status received from the status hot-link.",
    )

    last_error = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:LastError",
        value="",
        dtype=str,
        max_length=256,
        doc="Last client-side socket/protocol error for diagnostics.",
    )

    data_age = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:DataAge",
        value=-1.0,
        dtype=float,
        doc="Seconds since last data row received. -1 means unknown.",
    )

    status_age = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:StatusAge",
        value=-1.0,
        dtype=float,
        doc="Seconds since last status update received. -1 means unknown.",
    )

    # -----------------------------------------------------------------
    # MID-I readbacks (1-10)
    # -----------------------------------------------------------------

    for idx in range(1, 11):
        locals()[f"mid{idx}"] = pvproperty(
            name=f"XF:08IDB-SE{{{{RGA:1}}}}P:MID{idx}-I",
            value=0.0,
            dtype=float,
            doc=f"RGA MID{idx} intensity",
        )
    del idx

    # -----------------------------------------------------------------
    # Mass readbacks (1-10)
    # -----------------------------------------------------------------

    for idx in range(1, 11):
        locals()[f"mass{idx}"] = pvproperty(
            name=f"XF:08IDB-VA{{{{RGA:1}}}}Mass:MID{idx}",
            value=0.0,
            dtype=float,
            doc=f"MID{idx} mass value",
        )
    del idx

    # -----------------------------------------------------------------
    # Constructor (eager connect)
    # -----------------------------------------------------------------

    def __init__(self, *args, mas_host=None, mas_port=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = MASsoftClient(host=mas_host, port=mas_port)
        self.client.connect()
        self._running = False
        self._acq_task: asyncio.Task | None = None
        self._links_started = False
        self._mass_vals: list[float] = []
        self._last_pub_status_ts = 0.0
        self._last_pub_row_ts = 0.0

    # -----------------------------------------------------------------
    # PV put handlers
    # -----------------------------------------------------------------

    @open_exp.putter
    async def open_exp(self, instance, value):
        """Connect if needed, open/associate experiment, fetch legends."""
        if not int(value):
            return value

        fname = self.experiment.value
        if isinstance(fname, (list, tuple)):
            fname = fname[0]
        fname = (fname or "").strip() or "file56.exp"
        view = max(1, int(self.view.value or 1))

        LOG.info("Opening experiment: %s (view=%d)", fname, view)

        try:
            if not self.client.command.is_connected():
                await asyncio.to_thread(self.client.connect)

            await asyncio.to_thread(self.client.open_experiment, fname)

            legends = await asyncio.to_thread(self.client.fetch_legends, view=view)
            masses: list[float] = []
            for item in legends:
                if "mass" in item.lower():
                    try:
                        masses.append(float(item.split()[-1]))
                    except Exception:
                        continue
            self._mass_vals = masses[:10]

            for i, m in enumerate(self._mass_vals, start=1):
                await getattr(self, f"mass{i}").write(m)

            await self.connected.write(1)
            await self.status.write("Connected")
        except Exception as exc:
            LOG.exception("OpenExp failed")
            await self.connected.write(0)
            await self.status.write("Error")
            await self.last_error.write(repr(exc))

        return 0

    @run_exp.putter
    async def run_exp(self, instance, value):
        """Write 1 to start the experiment."""
        if int(value):
            try:
                await asyncio.to_thread(self.client.run_experiment)
            except Exception as exc:
                LOG.exception("RunExp failed")
                await self.last_error.write(repr(exc))
        return 0

    @abort_exp.putter
    async def abort_exp(self, instance, value):
        """Write 1 to abort the running experiment."""
        if int(value):
            try:
                await asyncio.to_thread(self.client.abort_experiment)
            except Exception as exc:
                LOG.exception("AbortExp failed")
                await self.last_error.write(repr(exc))
        return 0

    @close_exp.putter
    async def close_exp(self, instance, value):
        """Write 1 to close the experiment file."""
        if not int(value):
            return 0

        try:
            self._running = False
            if self._acq_task is not None:
                self._acq_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._acq_task
                self._acq_task = None

            if self._links_started:
                self.client.stop_links()
                self._links_started = False

            await asyncio.to_thread(self.client.close_experiment)
            await self.connected.write(0)
            await self.status.write("Disconnected")
        except Exception as exc:
            LOG.exception("CloseExp failed")
            await self.last_error.write(repr(exc))

        return 0

    @acquire.putter
    async def acquire(self, instance, value):
        """Start/stop acquisition loop."""
        want = bool(int(value))

        if want and not self._running:
            LOG.info("Starting acquisition loop")
            self._running = True
            self._acq_task = asyncio.create_task(self._acquire_loop())
        elif not want and self._running:
            LOG.info("Stopping acquisition loop")
            self._running = False
            if self._acq_task is not None:
                self._acq_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._acq_task
                self._acq_task = None

        return value

    # -----------------------------------------------------------------
    # Acquisition loops
    # -----------------------------------------------------------------

    async def _acquire_loop(self):
        """Hot-link with polling fallback."""
        try:
            view = max(1, int(self.view.value or 1))

            # Fetch legends and populate mass PVs
            legends, path = await asyncio.to_thread(self.client.get_legends, view)
            mass_vals = [float(h.split()[-1]) for h in legends if "mass" in h.lower()][
                :10
            ]
            self._mass_vals = mass_vals
            for i, m in enumerate(mass_vals, start=1):
                await getattr(self, f"mass{i}").write(m)

            # Try hot-link threads first
            try:
                await asyncio.to_thread(self.client.start_status_link, view=view)
                await asyncio.to_thread(self.client.start_data_link, view=view)
                self._links_started = True
                LOG.info("Hot-links started, using hotlink update loop")
                await self._hotlink_update_loop()
            except Exception as exc:
                LOG.warning("Hot-links failed (%s), falling back to polling", exc)
                self._links_started = False
                await self._polling_acquire_loop(view, mass_vals, path)

        except asyncio.CancelledError:
            LOG.info("Acquisition loop cancelled")
        except Exception as exc:
            LOG.error("Error in acquisition loop: %s", exc)
            with contextlib.suppress(Exception):
                await self.last_error.write(repr(exc))

    async def _hotlink_update_loop(self):
        """Read thread-safe accessors at ~1 Hz and update PVs."""
        while self._running:
            now = time.monotonic()

            # Status
            st = self.client.get_latest_status()
            st_ts = self.client.get_latest_status_timestamp()
            if st and st_ts > self._last_pub_status_ts:
                await self.status.write(st)
                self._last_pub_status_ts = st_ts
            if st_ts > 0:
                await self.status_age.write(max(0.0, now - st_ts))
            else:
                await self.status_age.write(-1.0)

            # Data
            row = self.client.get_latest_row()
            row_ts = self.client.get_latest_row_timestamp()
            if row and row_ts > self._last_pub_row_ts:
                for i, val in enumerate(row[:10], start=1):
                    await getattr(self, f"mid{i}").write(val)
                self._last_pub_row_ts = row_ts
            if row_ts > 0:
                await self.data_age.write(max(0.0, now - row_ts))
            else:
                await self.data_age.write(-1.0)

            # Error
            err = self.client.get_last_error()
            if err:
                await self.last_error.write(err)

            await asyncio.sleep(1.0)

    async def _polling_acquire_loop(self, view, mass_vals, path):
        """Working-version direct -lData polling with 5-attempt reconnect retry."""
        # Associate data socket with the experiment
        try:
            await asyncio.to_thread(self.client.open_experiment_data, path)
        except Exception as exc:
            LOG.error("Failed to associate data socket: %s", exc)
            return

        while self._running:
            try:
                raw_data = await asyncio.to_thread(
                    self.client.data_socket.send_command,
                    f"-lData -v{view}",
                )
                if raw_data and raw_data != "0":
                    values = raw_data.split()[2:]  # Skip time columns
                    if len(values) >= len(mass_vals):
                        for i, val in enumerate(values[: len(mass_vals)], start=1):
                            await getattr(self, f"mid{i}").write(float(val))
            except Exception as exc:
                LOG.error("Socket error during polling: %s", exc)
                self.client.data_socket.close()
                for attempt in range(5):
                    try:
                        await asyncio.to_thread(self.client.data_socket.connect)
                        LOG.info("Reconnected data socket")
                        break
                    except Exception as reconnect_err:
                        LOG.error(
                            "Reconnect attempt %d failed: %s",
                            attempt + 1,
                            reconnect_err,
                        )
                        await asyncio.sleep(5)
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    # Parse MASsoft connection arguments
    parser = argparse.ArgumentParser(description="RGA MASsoft IOC")
    parser.add_argument(
        "--mas-host",
        default=None,
        help="MASsoft host address (overrides config file)",
    )
    parser.add_argument(
        "--mas-port",
        type=int,
        default=None,
        help="MASsoft port number (overrides config file)",
    )
    args, remaining = parser.parse_known_args(sys.argv[1:])

    # Let caproto parse its own arguments from remaining args
    sys.argv = [sys.argv[0], *remaining]
    ioc_opts, run_opts = ioc_arg_parser(
        default_prefix="",
        desc="RGA MASsoft IOC",
    )

    ioc = RGAIOC(mas_host=args.mas_host, mas_port=args.mas_port, **ioc_opts)
    run(ioc.pvdb, **run_opts)
