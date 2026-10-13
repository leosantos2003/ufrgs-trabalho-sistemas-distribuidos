#!/usr/bin/env python3
"""Cliente stop-and-wait do serviço UDP de soma.

O cliente usa um único socket para descoberta, envio de requisições e recepção
de ACKs. A confiabilidade é construída na aplicação: há apenas uma requisição
pendente por vez, e ela é retransmitida até receber o ACK com o mesmo ID.
"""

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

## O cliente repete broadcast de descoberta duas vezes por segundo enquanto o
## servidor ainda não respondeu.
DISCOVERY_RETRY_SECONDS = 0.5
# Valor validado nos testes de rede doméstica e de laboratório. O enunciado
# permite configurar o timeout conforme o RTT da rede local.
REQUEST_TIMEOUT_SECONDS = 0.100
# Marcador exclusivo colocado na fila quando a entrada padrão termina. Usar um
# objeto, e não uma string ou número, impede confusão com valores legítimos.
STOP = object()


class Console:
    """Serializa a saída em uma thread separada da leitura do teclado.

    A fila preserva a ordem das mensagens entregues a ``log``. Assim, a thread
    de entrada e a thread principal não escrevem na tela ao mesmo tempo.
    """

    def __init__(self) -> None:
        # A mesma fila aceita textos normais e o marcador STOP de encerramento.
        self.messages: queue.Queue[str | object] = queue.Queue()
        self.thread = threading.Thread(target=self._write, daemon=True)
        self.thread.start()

    def log(self, text: str) -> None:
        """Agenda uma linha para a thread responsável pela saída padrão."""
        self.messages.put(text)

    def close(self) -> None:
        """Solicita encerramento ordenado e espera a última linha ser escrita."""
        self.messages.put(STOP)
        self.thread.join()

    def _write(self) -> None:
        """Consome a fila de mensagens até receber o marcador STOP."""
        while True:
            message = self.messages.get()
            try:
                if message is STOP:
                    return
                print(message, flush=True)
            finally:
                self.messages.task_done()


class SumClient:
    """Coordena descoberta, entrada, retransmissão e recebimento de ACKs."""

    def __init__(self, port: int, console: Console) -> None:
        self.port = port
        self.console = console
        # O socket permanece o mesmo durante toda a sessão. Isso preserva a
        # porta de origem usada pelo servidor para devolver ACKs.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # A porta 0 pede ao sistema operacional uma porta UDP efêmera livre.
        self.socket.bind(("", 0))
        # Após a descoberta, armazena-se o par IP/porta aceito como servidor.
        self.server: tuple[str, int] | None = None

        # A thread de entrada coloca inteiros nesta fila; a thread principal os
        # processa em ordem e nunca deixa mais de uma requisição em trânsito.
        self.values: queue.Queue[int | object] = queue.Queue()
        self.reader = threading.Thread(target=self._read_stdin, daemon=True)

    @staticmethod
    def _timestamp() -> str:
        """Produz a data no formato exigido pela interface do cliente."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _read_stdin(self) -> None:
        """Lê valores sem prompt e os transfere para a fila thread-safe.

        A função não realiza operações de rede. Isso deixa a thread principal
        livre para esperar ACKs e fazer retransmissões no tempo correto.
        """
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
            # EOF (Ctrl+D) e qualquer saída da função usam o mesmo mecanismo
            # para avisar a thread principal que não há mais valores a enviar.
            self.values.put(STOP)

    def discover(self) -> None:
        """Localiza o servidor por broadcast e guarda seu endereço unicast."""
        # O endereço pode ser sobrescrito apenas para testes em loopback. O
        # comportamento padrão é o broadcast IPv4 pedido pela especificação.
        broadcast = os.environ.get("DISCOVERY_BROADCAST", "255.255.255.255")
        destination = (broadcast, self.port)
        while self.server is None:
            # Se o datagrama broadcast se perder, o laço tenta novamente após
            # o prazo de descoberta, sem criar entradas duplicadas no servidor.
            self.socket.sendto(pack_discover(), destination)
            deadline = time.monotonic() + DISCOVERY_RETRY_SECONDS
            while time.monotonic() < deadline:
                self.socket.settimeout(max(0.0, deadline - time.monotonic()))
                try:
                    data, address = self.socket.recvfrom(65535)
                except socket.timeout:
                    break
                if data == DISCOVER_ACK:
                    # A resposta deve sair da porta configurada no servidor.
                    # O IP de origem é a informação descoberta pelo cliente.
                    self.server = (address[0], self.port)
                    self.console.log(f"{self._timestamp()} server_addr {address[0]}")
                    return

    def _send_until_acknowledged(self, request_id: int, value: int) -> None:
        """Executa stop-and-wait para uma única requisição.

        A função só retorna quando recebe um ACK com o mesmo identificador.
        Logo, ``run`` não incrementa o ID nem consome o próximo valor antes da
        confirmação da operação atual.
        """
        assert self.server is not None
        first_attempt = True
        # O pacote é criado uma vez e reutilizado em todos os reenvios. Isso
        # garante que uma retransmissão tenha exatamente o mesmo ID e valor.
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
                    # Descarta tráfego que não veio do servidor aceito ou que
                    # não é uma confirmação. Isso evita avançar por acidente.
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
        """Descobre o servidor e consome a entrada até EOF ou interrupção."""
        self.discover()
        # A entrada só começa após conhecer o servidor, como pede a interface.
        self.reader.start()
        request_id = 1
        while True:
            value = self.values.get()
            if value is STOP:
                return
            # Esta chamada bloqueia até o ACK correto, mantendo o protocolo
            # stop-and-wait e a numeração sequencial por cliente.
            self._send_until_acknowledged(request_id, value)
            request_id += 1

    def close(self) -> None:
        """Notifica a saída quando possível e libera o descritor do socket."""
        if self.server is not None:
            try:
                # EXIT não altera a soma; ele permite ao servidor remover a
                # entrada do participante que encerrou normalmente a sessão.
                self.socket.sendto(pack_exit(), self.server)
            except OSError:
                pass
        self.socket.close()


def parse_port() -> int:
    """Lê e valida a porta do servidor fornecida na linha de comando."""
    parser = argparse.ArgumentParser(description="Cliente UDP de soma confiável")
    parser.add_argument("port", type=int, help="porta UDP do servidor (1-65535)")
    port = parser.parse_args().port
    if not 1 <= port <= 65535:
        parser.error("a porta deve estar entre 1 e 65535")
    return port


def main() -> None:
    """Cria os componentes do cliente e trata Ctrl+C sem traceback."""
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
