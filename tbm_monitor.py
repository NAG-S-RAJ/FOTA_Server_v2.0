"""Live log window, SGW traffic capture and save-on-exit for the TBM scripts (Linux: tcpdump, Windows: Wireshark)."""

import datetime
import getpass
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

try:
    # websocket-client (pip install websocket-client)
    from websocket import WebSocketApp as _ClientWebSocketApp
except ImportError:
    _ClientWebSocketApp = None

if os.name == "nt":
    from proxy_auth import enable_windows_proxy_auth
else:
    def enable_windows_proxy_auth(report=None):
        return None
from sgw_decoder import decode_frame, parse_hex_lines, parse_tshark_hex_lines

IS_WINDOWS = os.name == "nt"
SGW_MAC = os.getenv("SGW_MAC", "ec:fa:03:00:cb:11")
CAPTURE_INTERFACE = os.getenv("CAPTURE_INTERFACE", "Ethernet" if IS_WINDOWS else "eth0")
WIRESHARK_DIR = os.getenv("WIRESHARK_DIR", r"C:\Program Files\Wireshark")
TSHARK_HEX_LINE = re.compile(r"^[0-9a-fA-F]{4,}  [0-9a-fA-F]{2}")
# Same capture as: tcpdump -i eth0 -nn -e -s 0 'ether host <SGW_MAC> and not ether proto 0x88f7'
CAPTURE_FILTER = f"ether host {SGW_MAC} and not ether proto 0x88f7"
SGW_UPLOAD_INTERVAL_SECONDS = 2
SGW_UPLOAD_MAX_LINES = 20
TBM_UPLOAD_MAX_LINES = 50
# TX/RX are not forwarded: the server already logs every message it exchanges with the TBM.
FORWARDED_SOURCES = ("TBM", "CAPTURE")
WINDOW_MAX_LINES = 5000


class _SyncWebSocketApp:
    """websocket-client style app built on the 'websockets' package, used when websocket-client is missing."""

    def __init__(self, url, on_open=None, on_message=None, on_error=None, on_close=None):
        self.url = url
        self.on_open, self.on_message, self.on_error, self.on_close = on_open, on_message, on_error, on_close
        self._connection = None
        self._send_lock = threading.Lock()

    def _callback(self, callback, *args):
        if callback is None:
            return
        try:
            callback(self, *args)
        except Exception as error:
            if callback is not self.on_error and self.on_error is not None:
                self.on_error(self, error)

    def send(self, data):
        connection = self._connection
        if connection is None:
            raise ConnectionError("WebSocket is not connected")
        with self._send_lock:
            connection.send(data)

    def close(self):
        connection = self._connection
        if connection is not None:
            connection.close()

    def run_forever(self, ping_interval=20, ping_timeout=10, **_):
        from websockets.sync.client import connect

        connection = None
        try:
            connection = connect(self.url, ping_interval=ping_interval, ping_timeout=ping_timeout, open_timeout=15)
            self._connection = connection
            self._callback(self.on_open)
            for message in connection:
                self._callback(self.on_message, message)
        except Exception as error:
            self._callback(self.on_error, error)
        finally:
            self._connection = None
            if connection is not None:
                connection.close()
            self._callback(self.on_close, getattr(connection, "close_code", None),
                           getattr(connection, "close_reason", None))


WebSocketApp = _ClientWebSocketApp or _SyncWebSocketApp


class _LogStream:
    """Mirrors print() output into the monitor while still writing to the terminal."""

    def __init__(self, monitor, original):
        self.monitor = monitor
        self.original = original
        self.buffer = threading.local()

    def write(self, text):
        self.original.write(text)
        pending = getattr(self.buffer, "text", "") + text
        *lines, self.buffer.text = pending.split("\n")
        for line in lines:
            if line.strip():
                self.monitor.log("TBM", line, echo=False)
        return len(text)

    def flush(self):
        self.original.flush()

    def __getattr__(self, name):
        return getattr(self.original, name)


class TbmMonitor:
    def __init__(self, name):
        self.name = name
        self.vin = ""
        self.lines = []
        self.view_queue = queue.Queue()
        self.lock = threading.Lock()
        self.sgw_pending = []
        self.sgw_dropped = 0
        self.sgw_packets = 0
        self.tbm_pending = []
        self.tbm_dropped = 0
        self.ws = None
        self.server_connected = False
        self.approved = False
        self.stop_event = threading.Event()
        self.capture_file = None
        self.capture_processes = []
        self.started_at = datetime.datetime.now()
        self._terminal = sys.stdout
        sys.stdout = _LogStream(self, sys.stdout)
        try:
            enable_windows_proxy_auth(report=lambda message: self.log("TBM", message))
        except Exception as error:
            self.log("TBM", f"ERROR Windows proxy sign-on unavailable: {error}")

    # ------------------------------------------------------------------ logging

    def log(self, source, text, echo=True):
        text = text if isinstance(text, str) else repr(text)
        line = f"{datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]} [{source}] {text.rstrip()}"
        with self.lock:
            self.lines.append(line)
            # Lines logged before approval are kept and forwarded once the server accepts this TBM.
            if source in FORWARDED_SOURCES:
                if len(self.tbm_pending) < 1000:
                    self.tbm_pending.append(line[:400])
                else:
                    self.tbm_dropped += 1
        self.view_queue.put((source, line))
        if echo:
            self._terminal.write(line + "\n")

    # ---------------------------------------------------------------- websocket

    def create_websocket(self, url, on_open, on_message, on_error, on_close):
        monitor = self

        class MonitoredWebSocketApp(WebSocketApp):
            def send(self, data, *args, **kwargs):
                monitor.log("TX -> SERVER", data)
                return super().send(data, *args, **kwargs)

            def send_quiet(self, data):
                return super().send(data)

        def opened(ws):
            monitor.ws = ws
            monitor.server_connected = True
            on_open(ws)

        def received(ws, message):
            monitor.log("RX <- SERVER", message)
            try:
                message_type = json.loads(message).get("type")
            except (ValueError, AttributeError):
                message_type = None
            if message_type == "approved":
                monitor.approved = True
            elif message_type in ("rejected", "auth_failed", "already_connected"):
                monitor.approved = False
            on_message(ws, message)

        def closed(ws, code, message):
            if monitor.ws is ws:
                monitor.ws = None
            monitor.server_connected = False
            monitor.approved = False
            on_close(ws, code, message)

        return MonitoredWebSocketApp(url, on_open=opened, on_message=received,
                                     on_error=on_error, on_close=closed)

    def _send_capture_status(self, status, detail):
        ws = self.ws
        if ws is None:
            return
        try:
            ws.send_quiet(json.dumps({"type": "sgw_capture", "vin": self.vin, "status": status, "detail": detail}))
        except Exception:
            pass

    def _take_batch(self, pending_name, dropped_name, limit):
        pending = getattr(self, pending_name)
        batch = pending[:limit]
        dropped = getattr(self, dropped_name)
        setattr(self, pending_name, pending[limit:])
        setattr(self, dropped_name, 0)
        return batch, dropped

    def _upload_logs(self):
        while not self.stop_event.wait(SGW_UPLOAD_INTERVAL_SECONDS):
            ws = self.ws
            if ws is None or not self.approved:
                continue
            with self.lock:
                tbm_batch, tbm_dropped = self._take_batch("tbm_pending", "tbm_dropped", TBM_UPLOAD_MAX_LINES)
                sgw_batch = self.sgw_pending[:SGW_UPLOAD_MAX_LINES]
                sgw_dropped = self.sgw_dropped + len(self.sgw_pending) - len(sgw_batch)
                self.sgw_pending = []
                self.sgw_dropped = 0
            messages = []
            if tbm_batch or tbm_dropped:
                messages.append({"type": "tbm_log", "vin": self.vin, "lines": tbm_batch, "dropped": tbm_dropped})
            if sgw_batch or sgw_dropped:
                messages.append({"type": "sgw_traffic", "vin": self.vin, "lines": sgw_batch, "dropped": sgw_dropped})
            for message in messages:
                try:
                    ws.send_quiet(json.dumps(message))
                except Exception as error:
                    if message["type"] == "tbm_log":
                        with self.lock:
                            self.tbm_pending[:0] = tbm_batch
                    self._terminal.write(f"Unable to forward logs to server: {error}\n")
                    break

    # ------------------------------------------------------------------ capture

    def start_capture(self):
        if IS_WINDOWS:
            self._start_wireshark_capture()
        else:
            self._start_tcpdump_capture()

    def _start_capture_threads(self, reader, decoder):
        packet_lines = queue.Queue()
        threading.Thread(target=self._pump_lines, args=(reader.stdout, packet_lines), daemon=True).start()
        threading.Thread(target=decoder, args=(packet_lines,), daemon=True).start()
        for process in self.capture_processes:
            threading.Thread(target=self._read_capture_errors, args=(process,), daemon=True).start()
        self.log("CAPTURE", f"Started on {CAPTURE_INTERFACE} filter='{CAPTURE_FILTER}' file={self.capture_file}")
        self._send_capture_status("started", f"interface={CAPTURE_INTERFACE} sgw_mac={SGW_MAC}")

    @staticmethod
    def _wireshark_tool(name):
        found = shutil.which(name)
        if found:
            return found
        candidate = os.path.join(WIRESHARK_DIR, name + ".exe")
        return candidate if os.path.exists(candidate) else None

    def _start_wireshark_capture(self):
        dumpcap, tshark = self._wireshark_tool("dumpcap"), self._wireshark_tool("tshark")
        if not dumpcap or not tshark:
            self.log("CAPTURE", "ERROR Wireshark (dumpcap/tshark) not found. Install Wireshark with Npcap, "
                                "or set WIRESHARK_DIR to its folder")
            return
        self.capture_file = os.path.join(
            tempfile.gettempdir(), f"tbm_capture_{self.started_at.strftime('%Y%m%d_%H%M%S')}.pcapng")
        # A separate process group lets stop_capture send Ctrl+Break so the capture file is closed cleanly.
        group = subprocess.CREATE_NEW_PROCESS_GROUP
        writer = subprocess.Popen([dumpcap, "-i", CAPTURE_INTERFACE, "-f", CAPTURE_FILTER, "-q", "-w", self.capture_file],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, creationflags=group)
        reader = subprocess.Popen([tshark, "-i", CAPTURE_INTERFACE, "-f", CAPTURE_FILTER, "-l", "-n", "-x"],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
                                  creationflags=group)
        self.capture_processes = [writer, reader]
        self._start_capture_threads(reader, self._decode_tshark_packets)

    def _start_tcpdump_capture(self):
        if not shutil.which("tcpdump"):
            self.log("CAPTURE", "ERROR tcpdump not installed. Install with: sudo apt install tcpdump")
            return
        prefix = [] if os.geteuid() == 0 else ["sudo", "-n"]
        user = [] if os.geteuid() == 0 else ["-Z", getpass.getuser()]
        base = prefix + ["tcpdump", "-i", CAPTURE_INTERFACE, "-nn", "-e", "-s", "0"] + user
        self.capture_file = os.path.join(
            tempfile.gettempdir(), f"tbm_capture_{self.started_at.strftime('%Y%m%d_%H%M%S')}.pcap")
        writer = subprocess.Popen(base + ["-U", "-w", self.capture_file, CAPTURE_FILTER],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        reader = subprocess.Popen(base + ["-l", "-xx", CAPTURE_FILTER],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.capture_processes = [writer, reader]
        self._start_capture_threads(reader, self._decode_packets)

    @staticmethod
    def _pump_lines(stream, target_queue):
        for line in stream:
            target_queue.put(line.rstrip("\n"))

    def _decode_packets(self, packet_lines):
        """Groups tcpdump -xx output (header line + hex lines) into packets and decodes each one."""
        header, hex_lines = None, []
        while True:
            try:
                line = packet_lines.get(timeout=0.3)
            except queue.Empty:
                line = None
            is_hex = line is not None and line.startswith(("\t", " ")) and "0x" in line[:12]
            if header is not None and not is_hex:
                self._record_packet(header, hex_lines)
                header, hex_lines = None, []
            if line is None:
                continue
            if is_hex:
                hex_lines.append(line)
            elif line.strip():
                header = line.strip()

    def _decode_tshark_packets(self, packet_lines):
        """Groups tshark -x output (summary line, hex dump lines, blank line) into packets and decodes each one."""
        hex_lines = []
        while True:
            try:
                line = packet_lines.get(timeout=0.3)
            except queue.Empty:
                line = None
            if line is not None and TSHARK_HEX_LINE.match(line):
                hex_lines.append(line)
                continue
            if hex_lines:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S.%f")
                self._record_frame(timestamp, lambda: parse_tshark_hex_lines(hex_lines), "")
                hex_lines = []

    def _record_packet(self, header, hex_lines):
        timestamp = header.split(" ", 1)[0]
        self._record_frame(timestamp, lambda: parse_hex_lines(hex_lines) if hex_lines else None, header)

    def _record_frame(self, timestamp, frame_bytes, fallback):
        try:
            frame = frame_bytes()
            decoded = decode_frame(frame, SGW_MAC) if frame else fallback
        except Exception as error:
            decoded = f"(decode error {error}) {fallback}"
        text = f"{timestamp} {decoded}"
        self.sgw_packets += 1
        self.log("SGW", text, echo=False)
        with self.lock:
            if len(self.sgw_pending) < 500:
                self.sgw_pending.append(text[:400])
            else:
                self.sgw_dropped += 1

    def _read_capture_errors(self, process):
        for message in process.stderr:
            message = message.strip()
            if not message:
                continue
            if "password is required" in message:
                message += " -> run 'sudo visudo' and allow tcpdump without password, or start the TBM script with sudo"
            self.log("CAPTURE", message)
        if process.wait() not in (0, -signal.SIGINT) and not self.stop_event.is_set():
            self.log("CAPTURE", f"ERROR tcpdump exited with code {process.returncode}")
            self._send_capture_status("failed", f"tcpdump exit code {process.returncode}")

    def stop_capture(self):
        for process in self.capture_processes:
            if process.poll() is None:
                try:
                    process.send_signal(signal.CTRL_BREAK_EVENT if IS_WINDOWS else signal.SIGINT)
                except PermissionError:
                    subprocess.run(["sudo", "-n", "kill", "-INT", str(process.pid)], check=False)
                except OSError:
                    process.terminate()
        for process in self.capture_processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if self.capture_processes:
            self.log("CAPTURE", f"Stopped. SGW packets seen: {self.sgw_packets}")
            self._send_capture_status("stopped", f"packets={self.sgw_packets}")

    # --------------------------------------------------------------------- save

    def default_file_name(self):
        return f"TBM_{self.vin or self.name}_{self.started_at.strftime('%Y%m%d_%H%M%S')}.pcapng"

    def save(self, target):
        target = os.path.abspath(os.path.expanduser(target))
        if not target.lower().endswith((".pcapng", ".pcap")):
            target += ".pcapng"
        os.makedirs(os.path.dirname(target), exist_ok=True)
        saved = []
        if self.capture_file and os.path.exists(self.capture_file):
            editcap = shutil.which("editcap") or (self._wireshark_tool("editcap") if IS_WINDOWS else None)
            source_format = os.path.splitext(self.capture_file)[1].lower()
            target_format = os.path.splitext(target)[1].lower()
            if source_format != target_format and editcap:
                subprocess.run([editcap, "-F", target_format.lstrip("."), self.capture_file, target], check=True)
            else:
                shutil.copyfile(self.capture_file, target)
                if source_format != target_format:
                    self.log("CAPTURE", f"editcap not found; capture saved in {source_format.lstrip('.')} format")
            saved.append(target)
        log_file = os.path.splitext(target)[0] + ".log"
        with self.lock, open(log_file, "w", encoding="utf-8") as file:
            file.write("\n".join(self.lines) + "\n")
        saved.append(log_file)
        return saved

    def discard(self):
        if self.capture_file and os.path.exists(self.capture_file):
            os.remove(self.capture_file)

    # ---------------------------------------------------------------------- run

    def run(self, connection_loop):
        """Starts capture and the server connection, then shows the log window until the TBM is stopped."""
        self.start_capture()
        threading.Thread(target=self._upload_logs, daemon=True).start()
        threading.Thread(target=connection_loop, daemon=True).start()
        stop_requested = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop_requested.set())
        signal.signal(signal.SIGTERM, lambda *_: stop_requested.set())
        if IS_WINDOWS or os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
            try:
                self._run_window(stop_requested)
                return
            except ImportError:
                self.log("TBM", "tkinter not available (sudo apt install python3-tk); using terminal mode")
        self._run_terminal(stop_requested)

    def _shutdown_connection(self):
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        self.stop_capture()

    def _run_terminal(self, stop_requested):
        self.log("TBM", "Running without a display. Press Ctrl+C to stop the TBM and save the capture.")
        stop_requested.wait()
        self._shutdown_connection()
        name = input(f"\nSave capture as (empty = {self.default_file_name()}, '-' = discard): ").strip()
        if name == "-":
            self.discard()
            print("Capture discarded.")
        else:
            for path in self.save(name or self.default_file_name()):
                print("Saved:", path)
        os._exit(0)

    def _run_window(self, stop_requested):
        import tkinter as tk
        from tkinter import filedialog, messagebox, scrolledtext

        root = tk.Tk()
        root.title(f"TBM Monitor - {self.vin or self.name}")
        root.geometry("1100x650")
        root.configure(bg="#111415")

        toolbar = tk.Frame(root, bg="#191d1f")
        toolbar.pack(fill="x")
        status = tk.Label(toolbar, bg="#191d1f", fg="#edf0ef", anchor="w",
                          font=("Segoe UI" if IS_WINDOWS else "DejaVu Sans", 10))
        status.pack(side="left", padx=10, pady=6, fill="x", expand=True)
        autoscroll = tk.BooleanVar(value=True)
        tk.Checkbutton(toolbar, text="Auto-scroll", variable=autoscroll, bg="#191d1f", fg="#edf0ef",
                       selectcolor="#111415", activebackground="#191d1f").pack(side="right", padx=6)
        filters = {}
        for source in ("TX -> SERVER", "RX <- SERVER", "SGW", "CAPTURE", "TBM"):
            filters[source] = tk.BooleanVar(value=True)
            tk.Checkbutton(toolbar, text=source, variable=filters[source], bg="#191d1f", fg="#edf0ef",
                           selectcolor="#111415", activebackground="#191d1f").pack(side="right")

        view = scrolledtext.ScrolledText(root, bg="#111415", fg="#edf0ef", insertbackground="#edf0ef",
                                         font=("Consolas" if IS_WINDOWS else "DejaVu Sans Mono", 9), wrap="none")
        view.pack(fill="both", expand=True)
        colors = {"TX -> SERVER": "#78dbc5", "RX <- SERVER": "#8ab4f8", "SGW": "#eed29a",
                  "CAPTURE": "#c9a0f0", "TBM": "#cad7d2"}
        for source, color in colors.items():
            view.tag_configure(source, foreground=color)
        view.tag_configure("ERROR", foreground="#efb5a9")

        def stop():
            if self.stop_event.is_set():
                return
            stop_button.configure(state="disabled", text="Stopping...")
            root.update_idletasks()
            self._shutdown_connection()
            while True:
                target = filedialog.asksaveasfilename(
                    parent=root, title="Save TBM capture", initialfile=self.default_file_name(),
                    defaultextension=".pcapng", filetypes=[("pcapng capture", "*.pcapng"), ("pcap capture", "*.pcap")])
                if target:
                    try:
                        saved = self.save(target)
                        messagebox.showinfo("Capture saved", "\n".join(saved), parent=root)
                        break
                    except Exception as error:
                        messagebox.showerror("Save failed", str(error), parent=root)
                elif messagebox.askyesno("Discard capture", "Discard the capture and logs?", parent=root):
                    self.discard()
                    break
            root.destroy()
            os._exit(0)

        stop_button = tk.Button(toolbar, text="Stop TBM & Save", command=stop, bg="#78dbc5", fg="#10251f",
                                activebackground="#a0ebd8", relief="flat", padx=12)
        stop_button.pack(side="right", padx=10, pady=4)
        root.protocol("WM_DELETE_WINDOW", stop)

        def poll():
            if stop_requested.is_set():
                stop()
                return
            for _ in range(500):
                try:
                    source, line = self.view_queue.get_nowait()
                except queue.Empty:
                    break
                if not filters.get(source, tk.BooleanVar(value=True)).get():
                    continue
                tags = (source, "ERROR") if "ERROR" in line or "failed" in line.lower() else (source,)
                view.insert("end", line + "\n", tags)
            excess = int(view.index("end-1c").split(".")[0]) - WINDOW_MAX_LINES
            if excess > 0:
                view.delete("1.0", f"{excess}.0")
            if autoscroll.get():
                view.see("end")
            capture = "running" if any(p.poll() is None for p in self.capture_processes) else "stopped"
            server = "approved (forwarding logs)" if self.approved else "connected" if self.server_connected else "disconnected"
            status.configure(text=f"VIN {self.vin or '-'}  |  Server {server}"
                                  f"  |  Capture {capture} on {CAPTURE_INTERFACE} ({SGW_MAC})  |  SGW packets {self.sgw_packets}")
            root.after(200, poll)

        poll()
        root.mainloop()
