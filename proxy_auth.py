"""Local proxy that adds Windows single sign-on (Negotiate/NTLM via SSPI) to a corporate HTTP proxy.

Python's requests/websockets can only do Basic proxy auth, so on networks where the proxy answers
407 with "Proxy-Authenticate: Negotiate/NTLM" they fail. This module listens on 127.0.0.1, accepts
plain CONNECT requests from those libraries, and opens the tunnel through the corporate proxy using
the logged-in Windows account (same as curl --proxy-negotiate -U :).
"""

import base64
import ctypes
import os
import socket
import socketserver
import threading
import urllib.parse
import urllib.request
from ctypes import wintypes

SEC_E_OK = 0
SEC_I_CONTINUE_NEEDED = 0x00090312
SECPKG_CRED_OUTBOUND = 2
SECURITY_NATIVE_DREP = 0x10
SECBUFFER_TOKEN = 2
SECBUFFER_VERSION = 0
ISC_REQ_CONNECTION = 0x00000800
MAX_TOKEN_SIZE = 64 * 1024


class SecHandle(ctypes.Structure):
    _fields_ = [("dwLower", ctypes.c_void_p), ("dwUpper", ctypes.c_void_p)]


class SecBuffer(ctypes.Structure):
    _fields_ = [("cbBuffer", wintypes.ULONG), ("BufferType", wintypes.ULONG), ("pvBuffer", ctypes.c_void_p)]


class SecBufferDesc(ctypes.Structure):
    _fields_ = [("ulVersion", wintypes.ULONG), ("cBuffers", wintypes.ULONG), ("pBuffers", ctypes.POINTER(SecBuffer))]


class _SspiClient:
    """One authentication exchange with the proxy using the current Windows logon."""

    def __init__(self, package, target):
        self.secur32 = ctypes.WinDLL("secur32")
        self.target = target
        self.credentials = SecHandle()
        self.context = SecHandle()
        self.has_context = False
        expiry = ctypes.c_longlong()
        status = self.secur32.AcquireCredentialsHandleW(
            None, ctypes.c_wchar_p(package), SECPKG_CRED_OUTBOUND, None, None, None, None,
            ctypes.byref(self.credentials), ctypes.byref(expiry))
        if status != SEC_E_OK:
            raise OSError(f"AcquireCredentialsHandle({package}) failed: 0x{status & 0xFFFFFFFF:08X}")

    def step(self, server_token=None):
        output = ctypes.create_string_buffer(MAX_TOKEN_SIZE)
        out_buffer = SecBuffer(MAX_TOKEN_SIZE, SECBUFFER_TOKEN, ctypes.cast(output, ctypes.c_void_p))
        out_desc = SecBufferDesc(SECBUFFER_VERSION, 1, ctypes.pointer(out_buffer))
        in_desc_pointer = None
        if server_token:
            token_buffer = ctypes.create_string_buffer(server_token, len(server_token))
            in_buffer = SecBuffer(len(server_token), SECBUFFER_TOKEN, ctypes.cast(token_buffer, ctypes.c_void_p))
            in_desc = SecBufferDesc(SECBUFFER_VERSION, 1, ctypes.pointer(in_buffer))
            in_desc_pointer = ctypes.byref(in_desc)
        attributes, expiry = wintypes.ULONG(), ctypes.c_longlong()
        status = self.secur32.InitializeSecurityContextW(
            ctypes.byref(self.credentials), ctypes.byref(self.context) if self.has_context else None,
            ctypes.c_wchar_p(self.target), ISC_REQ_CONNECTION, 0, SECURITY_NATIVE_DREP, in_desc_pointer, 0,
            ctypes.byref(self.context), ctypes.byref(out_desc), ctypes.byref(attributes), ctypes.byref(expiry))
        if status not in (SEC_E_OK, SEC_I_CONTINUE_NEEDED):
            raise OSError(f"InitializeSecurityContext failed: 0x{status & 0xFFFFFFFF:08X}")
        self.has_context = True
        return output.raw[:out_buffer.cbBuffer]

    def close(self):
        if self.has_context:
            self.secur32.DeleteSecurityContext(ctypes.byref(self.context))
        self.secur32.FreeCredentialsHandle(ctypes.byref(self.credentials))


def _read_response(sock):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Proxy closed the connection")
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    headers = [line.split(":", 1) for line in lines[1:] if ":" in line]
    length = next((int(value) for name, value in headers if name.strip().lower() == "content-length"), 0)
    while len(body) < length:
        chunk = sock.recv(min(65536, length - len(body)))
        if not chunk:
            break
        body += chunk
    challenges = [value.strip() for name, value in headers if name.strip().lower() == "proxy-authenticate"]
    return status, challenges


def open_tunnel(proxy_host, proxy_port, host, port, timeout=20):
    """Returns a socket tunnelled to host:port through the proxy, authenticating with SSPI when asked."""
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    request = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: keep-alive\r\n"
    try:
        sock.sendall((request + "\r\n").encode())
        status, challenges = _read_response(sock)
        if status == 200:
            sock.settimeout(None)
            return sock
        if status != 407:
            raise ConnectionError(f"Proxy answered HTTP {status}")
        offered = [challenge.split(" ", 1)[0] for challenge in challenges]
        package = "Negotiate" if "Negotiate" in offered else "NTLM" if "NTLM" in offered else None
        if package is None:
            raise ConnectionError(f"Proxy requires unsupported authentication: {', '.join(offered)}")
        client = _SspiClient(package, f"HTTP/{proxy_host}")
        try:
            token = client.step()
            for _ in range(3):
                header = f"Proxy-Authorization: {package} {base64.b64encode(token).decode()}\r\n"
                sock.sendall((request + header + "\r\n").encode())
                status, challenges = _read_response(sock)
                if status == 200:
                    sock.settimeout(None)
                    return sock
                server_token = next((challenge.split(" ", 1)[1] for challenge in challenges
                                     if challenge.startswith(package + " ")), None)
                if status != 407 or not server_token:
                    raise ConnectionError(f"Proxy authentication failed (HTTP {status})")
                token = client.step(base64.b64decode(server_token))
            raise ConnectionError("Proxy authentication did not complete")
        finally:
            client.close()
    except Exception:
        sock.close()
        raise


def _pipe(source, destination):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            destination.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (source, destination):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class _TunnelHandler(socketserver.BaseRequestHandler):
    def handle(self):
        client = self.request
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = client.recv(4096)
            if not chunk:
                return
            data += chunk
        request_line = data.split(b"\r\n", 1)[0].decode("latin-1")
        method, target, _ = (request_line.split(" ") + ["", ""])[:3]
        if method.upper() != "CONNECT":
            client.sendall(b"HTTP/1.1 501 Only CONNECT is supported\r\nContent-Length: 0\r\n\r\n")
            return
        host, _, port = target.rpartition(":")
        try:
            if urllib.request.proxy_bypass(host):
                upstream = socket.create_connection((host, int(port)), timeout=20)
                upstream.settimeout(None)
            else:
                upstream = open_tunnel(self.server.proxy_host, self.server.proxy_port, host, int(port))
        except Exception as error:
            self.server.report(f"Proxy tunnel to {target} failed: {error}")
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            return
        client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        threading.Thread(target=_pipe, args=(upstream, client), daemon=True).start()
        _pipe(client, upstream)


class _LocalProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def enable_windows_proxy_auth(report=print):
    """Starts the local SSPI proxy and points this process's HTTP(S)_PROXY at it. Returns its URL or None."""
    if os.name != "nt":
        return None
    configured = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or \
        os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    if not configured:
        return None
    parsed = urllib.parse.urlparse(configured if "://" in configured else "http://" + configured)
    if parsed.hostname in ("127.0.0.1", "localhost"):
        return None
    server = _LocalProxy(("127.0.0.1", 0), _TunnelHandler)
    server.proxy_host, server.proxy_port = parsed.hostname, parsed.port or 8080
    server.report = report
    threading.Thread(target=server.serve_forever, daemon=True).start()
    local = f"http://127.0.0.1:{server.server_address[1]}"
    for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        os.environ[name] = local
    report(f"Windows proxy sign-on enabled: {local} -> {parsed.hostname}:{server.proxy_port} (Negotiate/NTLM)")
    return local
