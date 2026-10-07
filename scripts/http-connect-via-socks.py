+#!/usr/bin/env python3
import argparse
import selectors
import socket
import socketserver
import struct
from urllib.parse import urlsplit


def recv_exact(sock, n):
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise OSError("unexpected EOF")
        out.extend(chunk)
    return bytes(out)


def connect_socks5(upstream_host, upstream_port, target_host, target_port):
    sock = socket.create_connection((upstream_host, upstream_port), timeout=15)
    sock.sendall(b"\x05\x01\x00")
    if recv_exact(sock, 2) != b"\x05\x00":
        raise OSError("SOCKS5 negotiation failed")

    try:
        ip = socket.inet_pton(socket.AF_INET, target_host)
        addr = b"\x01" + ip
    except OSError:
        try:
            ip = socket.inet_pton(socket.AF_INET6, target_host)
            addr = b"\x04" + ip
        except OSError:
            host = target_host.encode("idna")
            if len(host) > 255:
                raise OSError("hostname too long")
            addr = b"\x03" + bytes([len(host)]) + host

    sock.sendall(b"\x05\x01\x00" + addr + struct.pack("!H", target_port))
    header = recv_exact(sock, 4)
    if header[0] != 5 or header[1] != 0:
        raise OSError(f"SOCKS5 CONNECT failed, code={header[1]}")
    if header[3] == 1:
        recv_exact(sock, 4)
    elif header[3] == 3:
        recv_exact(sock, recv_exact(sock, 1)[0])
    elif header[3] == 4:
        recv_exact(sock, 16)
    else:
        raise OSError("invalid SOCKS5 reply")
    recv_exact(sock, 2)
    sock.settimeout(None)
    return sock


def relay(left, right):
    selector = selectors.DefaultSelector()
    left.setblocking(False)
    right.setblocking(False)
    selector.register(left, selectors.EVENT_READ, right)
    selector.register(right, selectors.EVENT_READ, left)
    try:
        while selector.get_map():
            for key, _ in selector.select(timeout=60):
                src = key.fileobj
                dst = key.data
                try:
                    data = src.recv(65536)
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    data = b""
                if not data:
                    try:
                        selector.unregister(src)
                    except Exception:
                        pass
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    continue
                view = memoryview(data)
                while view:
                    try:
                        sent = dst.send(view)
                        view = view[sent:]
                    except (BlockingIOError, InterruptedError):
                        continue
    finally:
        selector.close()


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        client = self.request
        client.settimeout(15)
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = client.recv(4096)
            if not chunk:
                return
            data.extend(chunk)
            if len(data) > 65536:
                return

        head, extra = bytes(data).split(b"\r\n\r\n", 1)
        parts = head.split(b"\r\n", 1)[0].decode("latin1").split()
        if len(parts) != 3 or parts[0].upper() != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n")
            return

        target = parts[1]
        if target.startswith("["):
            end = target.rfind("]")
            host = target[1:end]
            port = int(target[end + 2 :])
        else:
            host, port_text = target.rsplit(":", 1)
            port = int(port_text)

        upstream = connect_socks5(
            self.server.upstream_host,
            self.server.upstream_port,
            host,
            port,
        )
        try:
            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            if extra:
                upstream.sendall(extra)
            client.settimeout(None)
            relay(client, upstream)
        finally:
            upstream.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    args = parser.parse_args()

    upstream = urlsplit(args.upstream)
    if upstream.scheme not in ("socks5", "socks5h") or not upstream.hostname or not upstream.port:
        raise SystemExit("upstream must be socks5://host:port or socks5h://host:port")

    with Server((args.listen_host, args.listen_port), Handler) as server:
        server.upstream_host = upstream.hostname
        server.upstream_port = upstream.port
        server.serve_forever()


if __name__ == "__main__":
    main()

