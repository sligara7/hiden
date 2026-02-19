from pathlib import PureWindowsPath
import socket
import time
import logging

# Logger setup
# All logging flows through procServ's log management, which handles rotation and permissions correctly
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# System Configuration
MAS_HOST = '10.66.58.225'
MAS_PORT = 5026
EXPERIMENT_DIRECTORY = r"C:\Users\08id-user\Documents\Hiden Analytical\MASsoft\11"
EXPERIMENT_DIRECTORY_ENV = "%HIDEN_FilePath%" # Environment variable name for the experiment directory
MOST_RECENT_FILE = "%HIDEN_LastFile%" # This environment variable name already includes the path
TIME_PERSISTANCE = 20 # Time in seconds for the messages to keep trying waiting for success
MESSAGE_TERMINATOR = "\r\n"

class MASsoftSocket:
    def __init__(self, host, port, name="GenericSocket", timeout=20):
        self.host = host
        self.port = port
        self.name = name
        self.timeout = timeout
        self.sock = None

    def connect(self):
        """Establish or re-establish the socket connection."""
        if self.sock:
            try:
                # Test current socket
                self.sock.sendall(b'')
                return
            except Exception:
                self.close()
        self.sock = socket.create_connection((self.host, self.port))
        self.sock.settimeout(self.timeout)
        logging.info(f"{self.name} connected to {self.host}:{self.port}")
        try:
            _ = self.sock.recv(4096)
        except socket.timeout:
            pass

    def send_command(self, command, expect_response=True):
        if not self.sock:
            raise RuntimeError(f"{self.name} not connected.")
        # Append retry delay and CRLF
        message = command.strip() + f' -d{TIME_PERSISTANCE}{MESSAGE_TERMINATOR}'
        self.sock.sendall(message.encode('utf-8'))
        if expect_response:
            try:
                chunks = []
                while True:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    if len(chunk) < 4096:
                        break
                resp = b''.join(chunks).decode('utf-8').strip()
                return resp
            except socket.timeout:
                logging.warning(f"{self.name} response timeout for:\n        {message.strip()}")
                return ''
        return ''

    def receive(self):
        if not self.sock:
            raise RuntimeError(f"{self.name} not connected.")
        try:
            chunks = []
            while True:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                if len(chunk) < 4096:
                    break
            resp = b''.join(chunks).decode('utf-8').strip()
            return resp
        except socket.timeout:
            return ''

    def close(self):
        if self.sock:
            self.sock.close()
            logging.info(f"{self.name} closed.")

class MASsoftClient:
    def __init__(self, host=MAS_HOST, port=MAS_PORT):
        self.command_socket = MASsoftSocket(host, port, name="CommandSocket")
        self.status_socket  = MASsoftSocket(host, port, name="StatusSocket")
        self.data_socket    = MASsoftSocket(host, port, name="DataSocket")
        self.current_file = MOST_RECENT_FILE

    def initialize(self):
        """Connect all sockets."""
        self.command_socket.connect()
        self.status_socket.connect()
        self.data_socket.connect()

    def open_experiment_commands(self, file_name=None):
        """Open and associate an experiment file.
        If file_name is None, query MASsoft for the current filename."""
        # 1) Determine the filename string
        if file_name is None:
            full_path = self.query_filename()
        else:
            # Coerce list or tuple into a single string
            if isinstance(file_name, (list, tuple)):
                file_name = file_name[0]
            # Build a pure-Windows path
            full_path = str(PureWindowsPath(EXPERIMENT_DIRECTORY) / file_name)

        # 2) Send to MASsoft
        resp = self.command_socket.send_command(f'-f"{full_path}"')
        if resp =='0':
            raise RuntimeError(f"Failed to open experiment file: {full_path}")

        # 3) Remember it for future operations
        self.current_file = full_path
        return full_path

    def open_experiment_data(self, file_name=None):
        """Open and associate an experiment file.
        If file_name is None, query MASsoft for the current filename."""
        # 1) Determine the filename string
        if file_name is None:
            full_path = self.query_filename_data()
        else:
            # Coerce list or tuple into a single string
            if isinstance(file_name, (list, tuple)):
                file_name = file_name[0]
            # Build a pure-Windows path
            full_path = str(PureWindowsPath(EXPERIMENT_DIRECTORY) / file_name)

        # 2) Send to MASsoft
        resp = self.data_socket.send_command(f'-f"{full_path}"')
        if resp =='0':
            raise RuntimeError(f"Failed to open experiment file: {full_path}")

        # 3) Remember it for future operations
        self.current_file = full_path
        return full_path

    def open_experiment_status(self, file_name=None):
        """Open and associate an experiment file.
        If file_name is None, query MASsoft for the current filename."""
        # 1) Determine the filename string
        if file_name is None:
            full_path = self.query_filename()
        else:
            # Coerce list or tuple into a single string
            if isinstance(file_name, (list, tuple)):
                file_name = file_name[0]
            # Build a pure-Windows path
            full_path = str(PureWindowsPath(EXPERIMENT_DIRECTORY) / file_name)

        # 2) Send to MASsoft
        resp = self.status_socket.send_command(f'-f"{full_path}"')
        if resp =='0':
            raise RuntimeError(f"Failed to open experiment file: {full_path}")

        # 3) Remember it for future operations
        self.current_file = full_path
        return full_path

    def run_experiment(self, new_file_name = None, mode = "-Odt"):
        """Start the experiment."""        
        resp = self.command_socket.send_command(f'-xGo {mode}')
        if resp == '0':
            raise RuntimeError("Experiment failed to start.")
        if not resp:
            logging.warning("Assuming experiment started despite no response.")

    def associate_status_link(self, view=1):
        """Set up a hot-link for status updates."""
        if not self.current_file:
            raise RuntimeError("No file opened.")
        self.open_experiment_status()
        self.status_socket.send_command(f'-lStatus -v{view}')

    def monitor_until_stopped(self, timeout=120):
        """
        Listen for status updates until a 'Stopped...' status arrives.
        Requires associate_status_link() to be called first.
        """
        if not self.current_file:
            raise RuntimeError("No file opened.")
        self.associate_status_link()
        start = time.time()
        while time.time() - start < timeout:
            status = self.status_socket.receive()
            if status:
                logging.info(f"Status: {status}")
                if status.lower().startswith('stopped'):
                    return True
            time.sleep(1)
        raise TimeoutError(f"Did not stop within {timeout}s.")

    def get_data(self, view=1):
        """Retrieve scan data via a new data socket."""
        if not self.current_file:
            raise RuntimeError("No file opened.")
        headers = self.get_legends(view=view)
        data = []
        while True:
            raw_data = self.data_socket.send_command(f"-lData -v{view}")
            if raw_data != '0':
                lines = raw_data.strip().split('\r\n')
                # print(f'Lines: {lines}')
                for line in lines:
                    if line.strip() == '0':
                        # print("Ignoring first line with '0'.")
                        continue
                    values = line.split()
                    # print(f'Values: {values}')
                    if len(values) < len(headers):
                        # print(f"Line skipped due to insufficient values: {line.strip()}")
                        continue
                    data.append(values)
                    # print(f"Data appended: {values}")
            time.sleep(1)            


    def get_legends(self, view=1):
        """Retrieve column legends via a temporary socket."""
        path = self.command_socket.send_command("-xFilename")
        time.sleep(1)
        path = self.command_socket.send_command("-xFilename")
        self.command_socket.send_command(f'-f"{path}"')
        try:
            while True:
                raw_data = self.command_socket.send_command(f"-lLegends -v{view}")
                if raw_data != '0':
                    legend = raw_data.replace("\r\n", "\t").split("\t")
                    break
                else:
                    time.sleep(1)                
        except KeyboardInterrupt:
            print("Done.")
        return legend, path

    def get_legends_data(self, view=1):
        """Retrieve column legends data via a temporary socket."""
        path = self.data_socket.send_command("-xFilename")
        time.sleep(1)
        path = self.data_socket.send_command("-xFilename")
        self.command_socket.send_command(f'-f"{path}"')
        try:
            while True:
                raw_data = self.command_socket.send_command(f"-lLegends -v{view}")
                if raw_data != '0':
                    legend = raw_data.replace("\r\n", "\t").split("\t")
                    break
                else:
                    time.sleep(1)
        except KeyboardInterrupt:
            print("Done.")
        return legend, path

    def query_filename(self):
        """Return the filename currently associated with the command socket."""
        resp = self.command_socket.send_command('-xFilename')
        if resp == '0':
            raise RuntimeError("Failed querying filename.")
        return resp

    def query_filename_data(self):
        """Return the filename currently associated with the command socket."""
        resp = self.data_socket.send_command('-xFilename')
        if resp == '0':
            raise RuntimeError("Failed querying filename.")
        return resp

    def close_experiment(self):
        """Close the experiment file."""
        resp = self.command_socket.send_command('-xClose')
        if resp == '0':
            raise RuntimeError("Close failed.")
        
    def abort_experiment(self):
        """Abort the experiment."""
        resp = self.command_socket.send_command('-xAbort')
        if resp == '0':
            raise RuntimeError("Abort failed.")

    def shutdown(self):
        """Close all sockets."""
        self.command_socket.close()
        self.status_socket.close()
        self.data_socket.close()

# Example IPython Usage:
# from massoft_client import MASsoftClient
# client = MASsoftClient(); client.initialize()
# client.open_experiment('file56.exp')
# client.run_experiment(); client.associate_status_link()
# client.monitor_until_stopped(timeout=300)
# print(client.query_filename())
# data = client.get_data(); legends = client.get_legends()
# client.close_experiment(); client.shutdown()
