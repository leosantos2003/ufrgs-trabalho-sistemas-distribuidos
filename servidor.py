#!/usr/bin/env python3
"""Servidor UDP confiável para a agregação de números inteiros.

Dois tipos de servidor:
- FrontEnd (Servidor.FE): atende clientes, repassa requisições às réplicas.
- Replica  (Servidor.RM): processa a soma, monitora heartbeat, participa de eleição.
"""

from __future__ import annotations

import argparse
import socket
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from protocol import (
    ACK,
    ALIVE,
    DISCOVER,
    EXIT,
    MAX_UINT64,
    REQUEST,
    ProtocolError,
    pack_ack,
    pack_discover_ack,
    unpack_request,
)

HEARTBEAT = 0.1


class Servidor(Enum):
    FE = "Front End"
    RM = "Replica"


@dataclass
class ClientState:
    """Última resposta confirmada para um cliente identificado pelo IP.

    O estado é um retrato do ACK da última requisição aceita. Ele permite que o
    servidor responda corretamente a uma retransmissão, mesmo se outros
    clientes já tiverem modificado a soma global depois daquela operação.
    """

    # Maior ID processado para esse endereço IP.
    last_req: int = 0
    # Quantidade global de requisições existente quando ``last_req`` foi aceito.
    last_num_reqs: int = 0
    # Soma global existente quando ``last_req`` foi aceito.
    last_total_sum: int = 0


class SumServer(ABC):
    """Socket, bind e utilitários compartilhados por FrontEnd e Replica."""

    def __init__(self, port: int, peers: list[str]) -> None:
        # AF_INET e SOCK_DGRAM selecionam UDP sobre IPv4, conforme o enunciado.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Ajuda a reiniciar o processo logo após encerrá-lo, quando o sistema
        # operacional ainda mantém informações de associação da porta.
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # O endereço vazio significa "todas as interfaces IPv4". Dessa forma,
        # o servidor recebe tanto broadcasts de descoberta quanto unicast.
        self.socket.bind(("", port))

        self.port = port
        # lista de IPs de todos servidores ativos (anel completo)
        self.peers = peers
        # FE como primeiro
        self.fe_ip = peers[0]
        # réplicas ativas
        self.rm_ips = peers[1:] 

    @staticmethod
    def _timestamp() -> str:
        """Produz a data no formato exigido pelas mensagens da interface."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @abstractmethod
    def serve_forever(self) -> None:
        ...


class FrontEnd(SumServer):
    """Ponto de entrada dos clientes."""

    def _handle_discovery(self, address: tuple[str, int]) -> None:
        """Registra um participante novo e responde ao broadcast em unicast."""
        # A tabela é indexada por IP, conforme pedido no enunciado. A porta é
        # usada somente para responder ao datagrama que acabou de chegar.
        self.clients.setdefault(address[0], ClientState())
        self.socket.sendto(pack_discover_ack(), address)

    def __init__(self, port, peers):
        super().__init__(port, peers)

        # A chave é o endereço IP porque o enunciado define o participante por
        # esse campo. A porta UDP serve somente para responder ao datagrama.
        self.clients: dict[str, ClientState] = {}
        self.num_reqs = 0
        self.total_sum = 0

        
    def _send_ack(self, state: ClientState, address: tuple[str, int]) -> None:
            """Envia o ACK que corresponde à última operação aceita do cliente.

            Este método é usado após uma requisição nova, duplicada ou fora de
            ordem. Nos dois últimos casos, ele não usa o acumulador global atual:
            usa o retrato salvo em ``state``.
            """
            self.socket.sendto(
                pack_ack(state.last_req, state.last_num_reqs, state.last_total_sum), address
            )

    
    def _handle_request(self, data: bytes, address: tuple[str, int]) -> None:
        """Recebe requisição do cliente e repassa às réplicas."""
        try:
            # Rejeita datagramas com tipo, tamanho ou formato incorretos antes
            # de acessar os campos internos.
            request = unpack_request(data)
        except ProtocolError:
            return
        
        state = self.clients.get(address[0])
        if state is None:
            # Uma requisição só é válida após a descoberta; não a processar
            # evita criar estado parcial por pacotes espúrios.
            return

        # Como os IDs começam em 1, um cliente recém-descoberto espera ID 1.
        expected = state.last_req + 1
        if request.request_id == expected and request.value > 0:
            # O loop de recepção é sequencial. Portanto, estas atualizações
            # formam uma seção crítica atômica para a tabela e o acumulador.
            # A entrada é limitada a uint64; o overflow é tratado como erro de
            # protocolo, em vez de permitir que Python esconda a violação.
            # A expressão de subtração detecta overflow antes de executar a
            # soma. Sem essa checagem, um inteiro arbitrário de Python poderia
            # ultrapassar silenciosamente o intervalo definido pelo protocolo.
            if self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - request.value:
                return
            # Esta é a única região do programa que modifica o estado global.
            self.num_reqs += 1

            ## ACHO QUE AQUI ENTRA A SUBSTITUIÇÃO PARA A CHAMADA DE TRATAMENTO DOS RM E QUÓRUM
            """self.total_sum += request.value""" 
            
            state.last_req = request.request_id
            state.last_num_reqs = self.num_reqs
            state.last_total_sum = self.total_sum
            print(
                f"{self._timestamp()} client {address[0]} id_req {request.request_id} "
                f"value {request.value} num_reqs {self.num_reqs} total_sum {self.total_sum}",
                flush=True,
            )
        elif request.request_id <= state.last_req:
            # O servidor já aplicou esse ID. A retransmissão acontece, por
            # exemplo, quando o ACK original se perde na rede.
            print(
                f"{self._timestamp()} client {address[0]} DUP!! id_req {request.request_id} "
                f"value {request.value} num_reqs {state.last_num_reqs} "
                f"total_sum {state.last_total_sum}",
                flush=True,
            )
        else:
            # Um ID maior indica que falta ao menos uma requisição anterior.
            # O ACK enviado abaixo informa ao cliente até onde o servidor foi.
            print(
                f"{self._timestamp()} client {address[0]} OUT_OF_ORDER id_req "
                f"{request.request_id} expected {expected}",
                flush=True,
            )

        # Em duplicatas e mensagens fora de ordem, este ACK contém a última
        # requisição efetivamente processada, orientando o cliente a reenviar.
        self._send_ack(state, address)


    def _send_alive(self) -> None:
        """Envia sinal ALIVE periódico para as réplicas monitorarem o FE."""
        while True:
            for ip in self.rm_ips:
                self.socket.sendto(ALIVE, (ip, self.port))
            time.sleep(HEARTBEAT)

    def serve_forever(self) -> None:
        """Loop sequencial que recebe os datagramas UDP"""
        # mantem heartbeat em thread separada para não bloquear a função
        threading.Thread(target=self._send_alive, daemon=True).start()
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


class Replica(SumServer):
    """Processa requisições recebidas do FE e monitora o heartbeat."""

    def __init__(self, port, peers):
        super().__init__(port, peers)

        self.num_reqs = 0
        self.total_sum = 0
        self.last_alive = time.monotonic()
    
    ## ATUALIZAR LÓGICA PARA TRATAMENTO DE SOMA
    def _handle_request(self, data: bytes, address: tuple[str, int]) -> None:
        """Processa uma requisição vinda do FE."""  
        try:
            # Rejeita datagramas com tipo, tamanho ou formato incorretos antes
            # de acessar os campos internos.
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

        self._send_ack(state, address)

    def _monitor_heartbeat(self) -> None:
        """Detecta ausência de ALIVE consecutivos do FE e dispara eleição."""
        missed = 0
        last_seen = self.last_alive
        while True:
            time.sleep(HEARTBEAT)
            if self.last_alive == last_seen:
                missed += 1
                if missed == 3:
                    # FE não mandou ALIVE no tempo esperado
                    self._handle_election()
                    missed = 0
            else:
                missed = 0
                last_seen = self.last_alive

    def _handle_election(self) -> None:
        """Participa da eleição por anel para escolher novo FE."""
        ...

    def serve_forever(self) -> None:
        # mantem monitor do heartbeat em thread separada para não bloquear a função
        threading.Thread(target=self._monitor_heartbeat, daemon=True).start()
        while True:
            data, address = self.socket.recvfrom(65535)
            message_type = data[:1]
            if message_type == REQUEST:
                self._handle_request(data, address)
            elif message_type == ALIVE and len(data) == 1:
                self.last_alive = time.monotonic()




def parse_args() -> tuple[int, Servidor]:
    """Lê porta e tipo do servidor (FE ou RM) da linha de comando."""
    parser = argparse.ArgumentParser(description="Servidor UDP de soma confiável")
    parser.add_argument("port", type=int, help="porta UDP (1-65535)")
    parser.add_argument("tipo", choices=["FE", "RM"], help="tipo do servidor")
    parser.add_argument("peers", nargs="+", help="IPs dos nós que compõem o anel (em ordem de vizinhança)")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("a porta deve estar entre 1 e 65535")
    return args.port, Servidor[args.tipo], args.peers


def main() -> None:
    """Inicializa o servidor e garante o fechamento do socket no encerramento."""
    port, tipo, peers = parse_args()
    server: SumServer = FrontEnd(port, peers) if tipo == Servidor.FE else Replica(port, peers)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.socket.close()


if __name__ == "__main__":
    main()
