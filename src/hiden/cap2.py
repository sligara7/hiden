import argparse
import asyncio
import logging
import sys

from caproto import ChannelType
from caproto.server import PVGroup, pvproperty, ioc_arg_parser, run

from .massoft_client import MASsoftClient

logging.basicConfig(level=logging.INFO)

class RGAIOC(PVGroup):
    # — Control / Configuration PVs —
    open_exp = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:OpenExp',
        value=0, dtype=int,
        doc='Write 1 to open the experiment file'
    )

    experiment_name = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:ExpName',
        value='file1.exp',
        dtype=ChannelType.STRING, max_length=64,
        doc='Name of the .exp file in MASsoft folder'
    )

    acquire = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:Acquire',
        value=0, dtype=int,
        doc='Start/stop the acquisition loop',
    )

    run_exp = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:RunExp',
        value=0, dtype=int,
        doc='Write 1 to start the experiment'
    )

    abort_exp = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:AbortExp',
        value=0, dtype=int,
        doc='Write 1 to abort the running experiment'
    )

    close_exp = pvproperty(
        name='XF:08IDB-SE{{RGA:1}}:CloseExp',
        value=0, dtype=int,
        doc='Write 1 to close the experiment file'
    )

    # — MID-I & Mass PVs (1–10) —
    for idx in range(1, 11):
        locals()[f'mid{idx}'] = pvproperty(
            name=f'XF:08IDB-SE{{{{RGA:1}}}}P:MID{idx}-I',
            value=0.0, dtype=float,
            doc=f'RGA reading for MID{idx}',
        )
        locals()[f'mass{idx}'] = pvproperty(
            name=f'XF:08IDB-VA{{{{RGA:1}}}}Mass:MID{idx}',
            value=0.0, dtype=float,
            doc=f'RGA mass for MID{idx}',
        )
    del idx

    def __init__(self, *args, mas_host='10.66.58.225', mas_port=5026, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = MASsoftClient(host=mas_host, port=mas_port)
        self.client.initialize()
        self._running = False
        self._task = None
        self._mass_vals = []  # store legends

    @open_exp.putter
    async def open_exp(self, instance, value):
        want = bool(int(value))
        if want:
            fn = self.experiment_name.value
            if isinstance(fn, (list, tuple)):
                fn = fn[0]
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None,
                self.client.open_experiment_commands, fn
            )
        return value

    @experiment_name.putter
    async def experiment_name(self, instance, value):
        return value

    @acquire.putter
    async def acquire(self, instance, value):
        """Triggered when someone writes to the START PV."""
        want_acquire = bool(int(value))
        if want_acquire and not self._running:
            logging.info("Starting acquisition loop")
            self._running = True
            # spawn background task
            self._task = asyncio.create_task(self._acquire_loop())
        elif not want_acquire and self._running:
            logging.info("Stopping acquisition loop")
            self._running = False
            if self._task:
                self._task.cancel()
        return value

    async def _acquire_loop(self):
        """Read all channels once per second until stopped."""
        try:
            # Parses RGA data headers into PVs
            loop = asyncio.get_running_loop()
            headers, path = await loop.run_in_executor(None, self.client.get_legends, 1)
            mass_values = [
                float(h.split()[-1])
                for h in headers
                if 'mass' in h.lower()
            ]
            # print('Mass values: {}'.format(mass_values))
            for idx, mass_val in enumerate(mass_values[:10], start=1):
                # print(f'Mass {idx}: {mass_val}')
                pv = getattr(self, f'mass{idx}')
                await pv.write(mass_val)
                logging.debug(f"Wrote {mass_val:.2f} to {pv.name}")

            await loop.run_in_executor(None, self.client.open_experiment_data, path)
            while self._running:
                print(self._running)
                # print('Trying......')
                try:
                    raw_data = await loop.run_in_executor(None, self.client.data_socket.send_command, "-lData -v1")
                    if raw_data != '0':
                        lines = raw_data.strip().split('\r\n')
                        # print(f'Lines: {lines}')
                        for line in lines:
                            if line.strip() == '0':
                                # print("Ignoring first line with '0'.")
                                continue
                            values = line.split()[2:]
                            # print(f'Values: {values}')
                            # print(f'Length of values: {len(values)}')
                            # print('Length of mass values: {}'.format(len(mass_values)))
                            if len(values) == len(mass_values):
                                # print('updating PVs')
                                for idx, val in enumerate(values):
                                    # print(f'Index {idx}: {val}')
                                    pv = getattr(self, f'mid{idx+1}')
                                    print(pv)
                                    await pv.write(float(val))
                                    logging.debug(f'Wrote {val} to {pv.name}')
                except Exception as e:
                    logging.error(f"Socket error during acquisition: {e}")
                    self.client.data_socket.close()
                    # Retry reconnection up to N times
                    for attempt in range(5):
                        try:
                            self.client.data_socket.connect()
                            logging.info("Reconnected data socket")
                            break  # success — back to the while loop
                        except Exception as reconnect_err:
                            logging.error(f"Reconnect attempt {attempt+1} failed:\n{reconnect_err}")
                            await asyncio.sleep(5)
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            logging.info("Acquisition loop cancelled")
            return

    @run_exp.putter
    async def run_exp(self, instance, value):
        want = bool(int(value))
        if want:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.run_experiment)

        return value

    @abort_exp.putter
    async def abort_exp(self, instance, value):
        """Write 1 to abort the running experiment, always resets to 0."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.client.shutdown)
        await loop.run_in_executor(None, self.client.initialize)
        await loop.run_in_executor(None, self.client.command_socket.send_command, '-f"%HIDEN_LastFile%"')
        path = await loop.run_in_executor(None, self.client.command_socket.send_command, '-xFilename')
        # print(f'Aborting experiment: {path}')
        want = bool(int(value))
        if want:
            await loop.run_in_executor(None, self.client.abort_experiment)
        return value

    @close_exp.putter
    async def close_exp(self, instance, value):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.client.command_socket.send_command, '-f"%HIDEN_LastFile%"')
        want = bool(int(value))
        if want:
            await loop.run_in_executor(None, self.client.close_experiment)
        return value


if __name__ == '__main__':
    # Parse MASsoft connection arguments
    parser = argparse.ArgumentParser(description='RGA MASsoft IOC')
    parser.add_argument('--mas-host', default='10.66.58.225',
                        help='MASsoft host address (default: 10.66.58.225)')
    parser.add_argument('--mas-port', type=int, default=5026,
                        help='MASsoft port number (default: 5026)')
    args, remaining = parser.parse_known_args(sys.argv[1:])
    
    # Let caproto parse its own arguments from remaining args
    sys.argv = [sys.argv[0]] + remaining
    ioc_opts, run_opts = ioc_arg_parser(
        default_prefix='',  # PV names include the {{RGA:1}} macro literally
        desc='RGA MASsoft IOC'
    )
    
    ioc = RGAIOC(mas_host=args.mas_host, mas_port=args.mas_port, **ioc_opts)
    run(ioc.pvdb, **run_opts)
