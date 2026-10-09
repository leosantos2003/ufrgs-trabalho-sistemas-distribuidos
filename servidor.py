#!/usr/bin/env python3
"""Servidor UDP confiável para a agregação de números inteiros."""

from __future__ import annotations

import argparse
import socket
from dataclasses import dataclass
from datetime import datetime

from protocol import (
    ACK,
    DISCOVER,
    EXIT,
    MAX_UINT64,
    REQUEST,
    ProtocolError,
    pack_ack,
    pack_discover_ack,
    unpack_request,
)


@dataclass
class ClientState:
    """Última resposta confirmada para um cliente identificado pelo IP."""

    last_req: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0


class SumServer:
    def __init__(self, port: int) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", port))
        self.clients: dict[str, ClientState] = {}
        self.num_reqs = 0
        self.total_sum = 0

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _send_ack(self, state: ClientState, address: tuple[str, int]) -> None:
        self.socket.sendto(
            pack_ack(state.last_req, state.last_num_reqs, state.last_total_sum), address
        )

    def _handle_discovery(self, address: tuple[str, int]) -> None:
        # A tabela é indexada por IP, conforme pedido no enunciado. A porta é
        # usada somente para responder ao datagrama que acabou de chegar.
        self.clients.setdefault(address[0], ClientState())
        self.socket.sendto(pack_discover_ack(), address)

    def _handle_request(self, data: bytes, address: tuple[str, int]) -> None:
        try:
            request = unpack_request(data)
        except ProtocolError:
            return

        state = self.clients.get(address[0])
        if state is None:
            # Uma requisição só é válida após a descoberta; não a processar
            # evita criar estado parcial por pacotes espúrios.
            return

        expected = state.last_req + 1
        if request.request_id == expected and request.value > 0:
            # O loop de recepção é sequencial. Portanto, estas atualizações
            # formam uma seção crítica atômica para a tabela e o acumulador.
            # A entrada é limitada a uint64; o overflow é tratado como erro de
            # protocolo, em vez de permitir que Python esconda a violação.
            if self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - request.value:
                return
            self.num_reqs += 1
            self.total_sum += request.value
            state.last_req = request.request_id
            state.last_num_reqs = self.num_reqs
            state.last_total_sum = self.total_sum
            print(
                f"{self._timestamp()} client {address[0]} id_req {request.request_id} "
                f"value {request.value} num_reqs {self.num_reqs} total_sum {self.total_sum}",
                flush=True,
            )
        elif request.request_id <= state.last_req:
            print(
                f"{self._timestamp()} client {address[0]} DUP!! id_req {request.request_id} "
                f"value {request.value} num_reqs {state.last_num_reqs} "
                f"total_sum {state.last_total_sum}",
                flush=True,
            )
        else:
            print(
                f"{self._timestamp()} client {address[0]} OUT_OF_ORDER id_req "
                f"{request.request_id} expected {expected}",
                flush=True,
            )

        # Em duplicatas e mensagens fora de ordem, este ACK contém a última
        # requisição efetivamente processada, orientando o cliente a reenviar.
        self._send_ack(state, address)

    def serve_forever(self) -> None:
        print(f"{self._timestamp()} num_reqs 0 total_sum 0", flush=True)
        while True:
            data, address = self.socket.recvfrom(65535)
            message_type = data[:1]
            if message_type == DISCOVER and len(data) == 1:
                self._handle_discovery(address)
            elif message_type == REQUEST:
                self._handle_request(data, address)
            elif message_type == EXIT and len(data) == 1:
                # A saída não altera o acumulador; somente remove o participante
                # que explicitamente deixou o serviço.
                self.clients.pop(address[0], None)
                continue


def parse_port() -> int:
    parser = argparse.ArgumentParser(description="Servidor UDP de soma confiável")
    parser.add_argument("port", type=int, help="porta UDP (1-65535)")
    port = parser.parse_args().port
    if not 1 <= port <= 65535:
        parser.error("a porta deve estar entre 1 e 65535")
    return port


def main() -> None:
    server = SumServer(parse_port())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.socket.close()


if __name__ == "__main__":
    main()
