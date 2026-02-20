from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run

from .massoft_client import MASsoftClient

logging.basicConfig(level=logging.INFO)


class RGAIOC(PVGroup):
    # -- Control / Configuration PVs --
    open_exp = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:OpenExp",
        value=0,
        doc="Write 1 to open the experiment file",
        dtype=int,
    )

    experiment = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:ExpName",
        value="file56.exp",
        dtype=str,
        max_length=64,
        doc="Name of the .exp file in MASsoft folder",
    )

    acquire = pvproperty(
        name="XF:08IDB-SE{{RGA:1}}:Acquire",
        value=0,
        doc="Start/stop the acquisition loop",
        dtype=int,
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

    # -- MID-I readback PVs (1-10) --
    for idx in range(1, 11):
        locals()[f"mid{idx}"] = pvproperty(
            name=f"XF:08IDB-SE{{{{RGA:1}}}}P:MID{idx}-I",
            value=0.0,
            doc=f"RGA reading for MID{idx}",
            dtype=float,
        )
    del idx

    # -- Mass PVs (1-10) --
    for idx in range(1, 11):
        locals()[f"mass{idx}"] = pvproperty(
            name=f"XF:08IDB-VA{{{{RGA:1}}}}Mass:MID{idx}",
            value=0.0,
            doc=f"RGA mass for MID{idx}",
            dtype=float,
        )
    del idx

    def __init__(self, *args, mas_host="10.66.58.225", mas_port=5026, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = MASsoftClient(host=mas_host, port=mas_port)
        self.client.initialize()
        self._running = False
        self._acq_task = None

    @open_exp.putter
    async def open_exp(self, instance, value):
        """Open the experiment file when PV is set to 1."""
        if int(value):
            fname = self.experiment.value
            if isinstance(fname, (list, tuple)):
                fname = fname[0]
            logging.info(f"Opening experiment: {fname}")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.open_experiment_commands, fname)
        return value

    @acquire.putter
    async def acquire(self, instance, value):
        """Start/stop acquisition loop on 'acquire' PV change."""
        want = bool(int(value))

        if want and not self._running:
            logging.info("Starting acquisition loop")
            self._running = True
            self._acq_task = asyncio.create_task(self._acquire_loop())
        elif not want and self._running:
            logging.info("Stopping acquisition loop")
            self._running = False
            if self._acq_task:
                self._acq_task.cancel()
        return value

    async def _acquire_loop(self):
        """1 Hz loop: pull headers, then data, and update PVs."""
        try:
            loop = asyncio.get_running_loop()

            # 1) Get and parse legends -> mass PVs
            headers, path = await loop.run_in_executor(
                None, self.client.get_legends, 1
            )
            mass_vals = [
                float(h.split()[-1]) for h in headers if "mass" in h.lower()
            ][:10]
            logging.info(f"Parsed masses: {mass_vals}")
            for idx, m in enumerate(mass_vals, start=1):
                await getattr(self, f"mass{idx}").write(m)

            # 2) Associate the data socket with the experiment file
            await loop.run_in_executor(None, self.client.open_experiment_data, path)

            # 3) Main data loop
            while self._running:
                try:
                    raw_data = await loop.run_in_executor(
                        None, self.client.data_socket.send_command, "-lData -v1"
                    )
                    if raw_data != "0":
                        lines = raw_data.strip().split("\r\n")
                        for line in lines:
                            if line.strip() == "0":
                                continue
                            values = line.split()[2:]
                            if len(values) >= len(mass_vals):
                                for idx, val in enumerate(values[:len(mass_vals)], start=1):
                                    await getattr(self, f"mid{idx}").write(float(val))
                except Exception as e:
                    logging.error(f"Socket error during acquisition: {e}")
                    self.client.data_socket.close()
                    # Retry reconnection up to 5 times
                    for attempt in range(5):
                        try:
                            self.client.data_socket.connect()
                            logging.info("Reconnected data socket")
                            break
                        except Exception as reconnect_err:
                            logging.error(
                                f"Reconnect attempt {attempt + 1} failed: {reconnect_err}"
                            )
                            await asyncio.sleep(5)
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logging.info("Acquisition loop cancelled")
        except Exception as ex:
            logging.error(f"Error in acquisition loop: {ex}")

    @run_exp.putter
    async def run_exp(self, instance, value):
        """Write 1 to start the experiment."""
        if int(value):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.run_experiment)
        return value

    @abort_exp.putter
    async def abort_exp(self, instance, value):
        """Write 1 to abort the running experiment."""
        if int(value):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.abort_experiment)
        return value

    @close_exp.putter
    async def close_exp(self, instance, value):
        """Write 1 to close the experiment file."""
        if int(value):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.close_experiment)
        return value


if __name__ == "__main__":
    # Parse MASsoft connection arguments
    parser = argparse.ArgumentParser(description="RGA MASsoft IOC")
    parser.add_argument(
        "--mas-host",
        default="10.66.58.225",
        help="MASsoft host address (default: 10.66.58.225)",
    )
    parser.add_argument(
        "--mas-port", type=int, default=5026, help="MASsoft port number (default: 5026)"
    )
    args, remaining = parser.parse_known_args(sys.argv[1:])

    # Let caproto parse its own arguments from remaining args
    sys.argv = [sys.argv[0], *remaining]
    ioc_opts, run_opts = ioc_arg_parser(
        default_prefix="",  # PV names include the full prefix already
        desc="RGA MASsoft IOC",
    )

    ioc = RGAIOC(mas_host=args.mas_host, mas_port=args.mas_port, **ioc_opts)
    run(ioc.pvdb, **run_opts)
