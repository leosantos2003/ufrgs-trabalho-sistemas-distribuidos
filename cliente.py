#!/usr/bin/env python3
"""Cliente stop-and-wait do serviço UDP de soma."""

from __future__ import annotations

import argparse
import os
import queue
import socket
import sys
import threading
import time
from datetime import datetime

from protocol import (
    ACK,
    DISCOVER_ACK,
    EXIT,
    MAX_UINT64,
    ProtocolError,
    pack_discover,
    pack_exit,
    pack_request,
    unpack_ack,
)

DISCOVERY_RETRY_SECONDS = 0.5
# Valor validado nos testes de rede doméstica e de laboratório. O enunciado
# permite configurar o timeout conforme o RTT da rede local.
REQUEST_TIMEOUT_SECONDS = 0.100
STOP = object()


class Console:
    """Serializa a saída em uma thread separada da leitura do teclado."""

    def __init__(self) -> None:
        self.messages: queue.Queue[str | object] = queue.Queue()
        self.thread = threading.Thread(target=self._write, daemon=True)
        self.thread.start()

    def log(self, text: str) -> None:
        self.messages.put(text)

    def close(self) -> None:
        self.messages.put(STOP)
        self.thread.join()

    def _write(self) -> None:
        while True:
            message = self.messages.get()
            try:
                if message is STOP:
                    return
                print(message, flush=True)
            finally:
                self.messages.task_done()


class SumClient:
    def __init__(self, port: int, console: Console) -> None:
        self.port = port
        self.console = console
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.socket.bind(("", 0))
        self.server: tuple[str, int] | None = None

        self.values: queue.Queue[int | object] = queue.Queue()
        self.reader = threading.Thread(target=self._read_stdin, daemon=True)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _read_stdin(self) -> None:
        """Le valores sem prompt, como exigido pela interface do trabalho."""
        try:
            for raw in sys.stdin:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    value = int(raw, 10)
                except ValueError:
                    self.console.log(f"{self._timestamp()} valor_inválido {raw!r}")
                    continue
                if not 0 < value <= MAX_UINT64:
                    self.console.log(f"{self._timestamp()} valor_inválido {value}")
                    continue
                self.values.put(value)
        finally:
            self.values.put(STOP)

    def discover(self) -> None:
        # O endereço pode ser sobrescrito apenas para testes em loopback. O
        # comportamento padrão é o broadcast IPv4 pedido pela especificação.
        broadcast = os.environ.get("DISCOVERY_BROADCAST", "255.255.255.255")
        destination = (broadcast, self.port)
        while self.server is None:
            self.socket.sendto(pack_discover(), destination)
            deadline = time.monotonic() + DISCOVERY_RETRY_SECONDS
            while time.monotonic() < deadline:
                self.socket.settimeout(max(0.0, deadline - time.monotonic()))
                try:
                    data, address = self.socket.recvfrom(65535)
                except socket.timeout:
                    break
                if data == DISCOVER_ACK:
                    self.server = (address[0], self.port)
                    self.console.log(f"{self._timestamp()} server_addr {address[0]}")
                    return

    def _send_until_acknowledged(self, request_id: int, value: int) -> None:
        assert self.server is not None
        first_attempt = True
        packet = pack_request(request_id, value)
        while True:
            action = "SEND" if first_attempt else "RESEND"
            self.console.log(
                f"{self._timestamp()} server {self.server[0]} {action} "
                f"id_req {request_id} value {value}"
            )
            self.socket.sendto(packet, self.server)
            first_attempt = False
            deadline = time.monotonic() + REQUEST_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                self.socket.settimeout(max(0.0, deadline - time.monotonic()))
                try:
                    data, address = self.socket.recvfrom(65535)
                except socket.timeout:
                    break
                if address != self.server or data[:1] != ACK:
                    continue
                try:
                    acknowledgement = unpack_ack(data)
                except ProtocolError:
                    continue
                if acknowledgement.request_id != request_id:
                    # ACK antigo ou a confirmação de uma lacuna: a mesma
                    # requisição continua pendente e será retransmitida.
                    continue
                self.console.log(
                    f"{self._timestamp()} server {self.server[0]} id_req {request_id} "
                    f"value {value} num_reqs {acknowledgement.num_reqs} "
                    f"total_sum {acknowledgement.total_sum}"
                )
                return
            self.console.log(
                f"{self._timestamp()} server {self.server[0]} TIMEOUT "
                f"id_req {request_id} value {value}"
            )

    def run(self) -> None:
        self.discover()
        self.reader.start()
        request_id = 1
        while True:
            value = self.values.get()
            if value is STOP:
                return
            self._send_until_acknowledged(request_id, value)
            request_id += 1

    def close(self) -> None:
        if self.server is not None:
            try:
                self.socket.sendto(pack_exit(), self.server)
            except OSError:
                pass
        self.socket.close()


def parse_port() -> int:
    parser = argparse.ArgumentParser(description="Cliente UDP de soma confiável")
    parser.add_argument("port", type=int, help="porta UDP do servidor (1-65535)")
    port = parser.parse_args().port
    if not 1 <= port <= 65535:
        parser.error("a porta deve estar entre 1 e 65535")
    return port


def main() -> None:
    console = Console()
    client = SumClient(parse_port(), console)
    try:
        client.run()
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        console.close()


if __name__ == "__main__":
    main()
