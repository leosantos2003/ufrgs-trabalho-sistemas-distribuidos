#!/usr/bin/env python3
"""Servidor UDP confiável para a agregação de números inteiros.

Dois tipos de servidor:
- FrontEnd (Servidor.FE): atende clientes, repassa requisições às réplicas.
- Replica  (Servidor.RM): processa a soma, monitora heartbeat, participa de eleição.
"""

from __future__ import annotations

import argparse
import json
import select
import socket
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from protocol import (
    ACK,
    ALIVE,
    COORDINATOR,
    DISCOVER,
    ELECTION,
    EXIT,
    MAX_UINT64,
    REPLICATE,
    REPLICA_ACK,
    REQUEST,
    RM_JOIN,
    RM_JOIN_ACK,
    RM_HEARTBEAT,
    RM_HEARTBEAT_ACK,
    RM_SNAPSHOT,
    RM_SNAPSHOT_ACK,
    RM_TOPOLOGY,
    RM_LEAVE,
    RING_ACK,
    SERVER_DISCOVER,
    SERVER_DISCOVER_ACK,
    ProtocolError,
    pack_ack,
    pack_discover_ack,
    pack_replica_ack,
    pack_replicate,
    pack_ring_ack,
    pack_ring_message,
    unpack_replica_ack,
    unpack_replicate,
    unpack_ring_ack,
    unpack_ring_message,
    unpack_request,
    pack_rm_snapshot,
    unpack_rm_snapshot,
    broadcast_destinations,
)

# Redes Wi-Fi e hotspots podem introduzir jitter de algumas centenas de
# milissegundos em UDP. A cadência abaixo evita interpretar uma pequena pausa
# como queda, mas ainda detecta uma falha efetiva em poucos segundos.
HEARTBEAT = 0.5

MULTICAST_GROUP = "239.0.0.1"
MULTICAST_TTL = 1
MULTICAST_PORT = 6571
REPLICATION_TIMEOUT = 0.05
REPLICATION_CHECK_INTERVAL = 0.01
RM_HEARTBEAT_INTERVAL = HEARTBEAT
# Hotspots podem interromper UDP para todos os clientes por alguns segundos
# (por reconfiguração de rádio, power-save ou retransmissão). Como o trabalho
# não impõe um prazo máximo para detectar falhas, a suspeita só começa após
# dez segundos sem sinal; três ciclos posteriores confirmam a remoção.
RM_HEARTBEAT_TIMEOUT = 10.0
FE_HEARTBEAT_TIMEOUT = 10.0
RM_HEARTBEAT_FAILURES = 3
RING_ACK_TIMEOUT = 0.15
RING_ELECTION_TIMEOUT = 0.30
# Quando vários RMs detectam a falha no mesmo tick, cada um pode colocar um
# token no anel.  Esperar esta janela antes da proclamação deixa os tokens se
# encontrarem e impede um candidato menor de se proclamar antes de ver o token
# do maior participante vivo.
RING_ELECTION_SETTLE = 0.5
# A promoção troca a função do processo, mas mantém o mesmo socket. Esta
# margem cobre a troca do laço de recepção e vários períodos de heartbeat
# antes de qualquer RM poder suspeitar do novo FE.
LEADER_STABILIZATION = 1.0


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

    def __init__(self, port: int, own_ip: str) -> None:
        # AF_INET e SOCK_DGRAM selecionam UDP sobre IPv4, conforme o enunciado.
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Ajuda a reiniciar o processo logo após encerrá-lo, quando o sistema
        # operacional ainda mantém informações de associação da porta.
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Um servidor real precisa receber tanto unicast quanto broadcast de
        # descoberta. Dois sockets na mesma porta (um em ``own_ip`` e outro em
        # ``0.0.0.0``) parecem uma boa separação, mas em algumas pilhas Wi-Fi
        # os datagramas passam a alternar entre eles. Isso faz um heartbeat
        # aparecer no tcpdump e, ainda assim, não chegar ao laço que o trata.
        # Portanto, em execução normal há um único socket em todas as
        # interfaces. O IP do nó continua sendo ``own_ip``; o bind não define
        # sua identidade no protocolo.
        #
        # A exceção são os testes locais que usam vários IPv4 127.x na mesma
        # máquina e mesma porta. Neles o bind específico é necessário para que
        # o kernel não entregue um datagrama ao processo errado.
        bind_ip = own_ip if own_ip.startswith("127.") else ""
        self.socket.bind((bind_ip, port))
        self.discovery_socket: socket.socket | None = None

        # Heartbeats não precisam usar a porta conhecida do serviço: a
        # identidade do FE/RM é o IPv4 de origem, já validado pelos
        # receptores. Usar uma porta efêmera evita o caso ambíguo de UDP em
        # que origem e destino são ambos ``porta`` (por exemplo, 4000), que
        # apresentou perdas intermitentes em alguns pontos de acesso Wi-Fi.
        # A porta principal continua sendo a única porta de serviço exposta.
        self.heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.heartbeat_socket.bind((own_ip, 0))

        self.port = port
        self.own_ip = own_ip

    def recv_packet(self, timeout: float | None = None) -> tuple[bytes, tuple[str, int]]:
        """Recebe um datagrama do socket único do servidor."""
        readable, _, _ = select.select([self.socket], [], [], timeout)
        if not readable:
            raise socket.timeout()
        return readable[0].recvfrom(65535)

    @staticmethod
    def get_local_ip() -> str:
        """Obtém o IP utilizado pela rota de rede, sem argumento do usuário."""
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("8.8.8.8", 80))
            return probe.getsockname()[0]
        except OSError:
            return socket.gethostbyname(socket.gethostname())
        finally:
            probe.close()

    @staticmethod
    def _timestamp() -> str:
        """Produz a data no formato exigido pelas mensagens da interface."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @abstractmethod
    def serve_forever(self) -> None:
        ...


class FrontEnd(SumServer):
    """Ponto de entrada dos clientes."""
    
    def __init__(
        self,
        port: int,
        own_ip: str,
        *,
        server_socket: socket.socket | None = None,
        discovery_socket: socket.socket | None = None,
        heartbeat_socket: socket.socket | None = None,
        initial_num_reqs: int = 0,
        initial_total_sum: int = 0,
        initial_next_seq: int = 1,
        initial_clients: dict[str, ClientState] | None = None,
        initial_rms: set[str] | None = None,
        allow_local_commit: bool = False,
    ) -> None:
        if server_socket is None:
            super().__init__(port, own_ip)
        else:
            # Promoção de um RM: preserva o descritor já associado ao IP/porta
            # do vencedor, sem abrir uma segunda associação UDP.
            self.socket = server_socket
            self.socket.settimeout(None)
            self.discovery_socket = discovery_socket
            if heartbeat_socket is None:
                raise ValueError("a promoção exige o socket de heartbeat")
            self.heartbeat_socket = heartbeat_socket
            self.port = port
            self.own_ip = own_ip

        self.fe_ip = own_ip
        self.rm_ips: set[str] = set(initial_rms or set())
        # Somente um FE que foi promovido de um RM pode atender sozinho. Um
        # FE inicial sem réplicas ainda deve aguardar a formação do quorum.
        self.allow_local_commit = allow_local_commit
        self.rm_last_seen: dict[str, float] = {}
        self.rm_sync_pending: set[str] = set()

        # A chave é o endereço IP porque o enunciado define o participante por
        # esse campo. A porta UDP serve somente para responder ao datagrama.
        self.clients: dict[str, ClientState] = dict(initial_clients or {})
        self.num_reqs = initial_num_reqs
        self.total_sum = initial_total_sum
        
        # Controle do sequenciador global e da agregacao de quorum dos RMs
        self.coord_lock = threading.Lock()
        self.next_seq = initial_next_seq
        self.pending_quorums: dict[int, dict] = {}
        self.pending_clients: dict[tuple[str, int], int] = {}
        self.request_executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="fe-request")
        # Permite encerrar as threads do FE que sofreu uma queda simulada, sem
        # deixá-las agir sobre sockets já fechados.
        self.stop_event = threading.Event()

        # Próxima operação que pode ser consolidada no estado do FE.
        self.next_commit_seq = initial_next_seq

        # Socket usado exclusivamente para disseminar operacoes aos RMs.
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        self.multicast_socket.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_IF,
            socket.inet_aton(own_ip),
        )
        self.multicast_socket.bind((own_ip, 0))

    @classmethod
    def from_replica(cls, replica: "Replica") -> "FrontEnd":
        """Promove uma réplica mantendo o estado replicado e o socket UDP."""
        with replica.state_lock:
            clients = {
                ip: ClientState(
                    last_req=state.last_req,
                    last_num_reqs=state.last_num_reqs,
                    last_total_sum=state.last_total_sum,
                )
                for ip, state in replica.client_states.items()
            }
            num_reqs = replica.num_reqs
            total_sum = replica.total_sum
            next_seq = replica.expected_seq
            remaining_rms = set(replica.rm_ips)
            remaining_rms.discard(replica.own_ip)
        return cls(
            replica.port,
            replica.own_ip,
            server_socket=replica.socket,
            discovery_socket=replica.discovery_socket,
            heartbeat_socket=replica.heartbeat_socket,
            initial_num_reqs=num_reqs,
            initial_total_sum=total_sum,
            initial_next_seq=next_seq,
            initial_clients=clients,
            initial_rms=remaining_rms,
            allow_local_commit=True,
        )

    def _handle_discovery(self, address: tuple[str, int]) -> None:
            """Registra um participante novo e responde ao broadcast em unicast."""
            # A tabela eh indexada por IP, conforme pedido no enunciado. A porta eh
            # usada somente para responder ao datagrama que acabou de chegar.
            # Acesso protegido por coord_lock para concorrencia com _handle_request.
            with self.coord_lock:
                self.clients.setdefault(address[0], ClientState())
            self.socket.sendto(pack_discover_ack(), address)

    def _handle_server_discovery(self, address: tuple[str, int]) -> None:
        """Responde a descoberta de outro servidor identificando o FE atual."""
        self.socket.sendto(SERVER_DISCOVER_ACK, address)

    def _send_topology(self) -> None:
        """Propaga a topologia atual aos RMs ativos."""
        with self.coord_lock:
            topology = {
                "fe_ip": self.fe_ip,
                "rm_ips": sorted(self.rm_ips),
                "port": self.port,
            }
            active_rms = set(self.rm_ips)
        packet = RM_TOPOLOGY + json.dumps(topology, separators=(",", ":")).encode("utf-8")
        for rm_ip in active_rms:
            try:
                self.socket.sendto(packet, (rm_ip, self.port))
            except OSError:
                # Em uma queda abrupta o descritor pode ser fechado enquanto a
                # thread de monitoração ainda finaliza seu ciclo atual.
                return

    def _handle_rm_join(self, address: tuple[str, int]) -> None:
        """Sincroniza um RM antes de adicioná-lo ao conjunto ativo."""
        rm_ip = address[0]
        with self.coord_lock:
            
            if rm_ip in self.rm_ips:
                self.rm_last_seen[rm_ip] = time.monotonic()
            else:
                self.rm_sync_pending.add(rm_ip)
                
            num_reqs = self.num_reqs
            total_sum = self.total_sum
            next_seq = self.next_commit_seq

        print(
            f"{self._timestamp()} FE RM_JOIN_SYNC rm {rm_ip} "
            f"num_reqs {num_reqs} total_sum {total_sum} next_seq {next_seq}",
            flush=True,
        )
        self.socket.sendto(
            pack_rm_snapshot(
                num_reqs,
                total_sum,
                next_seq,
                {
                    ip: (state.last_req, state.last_num_reqs, state.last_total_sum)
                    for ip, state in self.clients.items()
                },
            ),
            (rm_ip, self.port),
        )

    def _handle_rm_snapshot_ack(self, data: bytes, address: tuple[str, int]) -> None:
        """Confirma que o RM aplicou o snapshot e o ativa no quorum."""
        if len(data) != 1 or data[:1] != RM_SNAPSHOT_ACK:
            return
        rm_ip = address[0]
        with self.coord_lock:
            self.rm_sync_pending.discard(rm_ip)
            self.rm_ips.add(rm_ip)
            self.rm_last_seen[rm_ip] = time.monotonic()
            active_count = len(self.rm_ips)

        print(
            f"{self._timestamp()} FE RM_JOIN rm {rm_ip} "
            f"active_rms {active_count}",
            flush=True,
        )
        self._send_topology()

    def _handle_rm_heartbeat(self, address: tuple[str, int]) -> None:
        rm_ip = address[0]
        with self.coord_lock:
            if rm_ip not in self.rm_ips:
                return
            self.rm_last_seen[rm_ip] = time.monotonic()
        try:
            self.socket.sendto(RM_HEARTBEAT_ACK, (rm_ip, self.port))
        except OSError:
            # Um FE encerrado abruptamente não deve gerar traceback enquanto
            # os RMs ainda concluem a detecção de sua queda.
            return

    def _handle_rm_leave(self, address: tuple[str, int]) -> None:
        rm_ip = address[0]
        self.handle_rm_failure(rm_ip)
        self._send_topology()

    def _ack_ring_control(self, data: bytes, address: tuple[str, int]) -> None:
        """Confirma tokens tardios após uma promoção.

        Mais de um RM pode detectar a queda do FE no mesmo intervalo e iniciar
        rodadas concorrentes. Depois de promovido, o novo FE não participa de
        outra rodada, mas precisa confirmar tokens remanescentes para que um
        RM não o considere um sucessor inativo e eleja um segundo líder.
        """
        try:
            message = unpack_ring_message(data)
            acknowledgement = pack_ring_ack(message.election_id, self.own_ip)
            self.socket.sendto(acknowledgement, address)
        except (OSError, ProtocolError):
            return

    @staticmethod
    def _calc_quorum(total_rms: int) -> int:
            if total_rms <= 0:
                return 0
            return (total_rms // 2) + 1
        
    def _send_ack(self, state: ClientState, address: tuple[str, int]) -> None:
            """Envia o ACK que corresponde à última operação aceita do cliente.

            Este método é usado após uma requisição nova, duplicada ou fora de
            ordem. Nos dois últimos casos, ele não usa o acumulador global atual:
            usa o retrato salvo em ``state``.
            """
            self.socket.sendto(pack_ack(state.last_req, state.last_num_reqs, state.last_total_sum), address)
    
    def _dispatch_packet(self, packet: bytes) -> None:
        """Dissemina uma operação sequenciada pelo grupo multicast."""

        self.multicast_socket.sendto(
            packet,
            (MULTICAST_GROUP, MULTICAST_PORT),
        )

    def _find_quorum_result(self,entry: dict) -> tuple[tuple[int, int], int] | None:
        """Retorna o resultado de maior concordancia quando atingir o quorum."""

        quorum_needed = entry["quorum_needed"]

        if quorum_needed <= 0:
            return None

        agreement: dict[tuple[int, int], int] = {}

        for vote in entry["votes"].values():
            agreement[vote] = agreement.get(vote, 0) + 1

        if not agreement:
            return None

        best_result, best_count = max(agreement.items(), key=lambda item: item[1])

        if best_count < quorum_needed:
            return None

        return best_result, best_count

    def _commit_entry_locked(self,seq_id: int,entry: dict,best_result: tuple[int, int],best_count: int) -> None:
        """Conclui uma operacao. Deve ser chamado somente enquanto coord_lock estiver adquirido."""

        if entry["completed"]:
            return

        entry["completed"] = True

        client_addr: tuple[str, int] = entry["client_addr"]
        request_id: int = entry["request_id"]
        value: int = entry["value"]
        quorum_needed: int = entry["quorum_needed"]

        request_key = (client_addr[0],request_id)

        self.pending_clients.pop(request_key, None)

        agreed_reqs, agreed_sum = best_result

        state = self.clients.get(client_addr[0])

        if state is None:
            state = ClientState()
            self.clients[client_addr[0]] = state

        self.num_reqs = agreed_reqs
        self.total_sum = agreed_sum

        state.last_req = request_id
        state.last_num_reqs = agreed_reqs
        state.last_total_sum = agreed_sum

        print(
            f"{self._timestamp()} FE QUORUM "
            f"seq_id {seq_id} "
            f"client {client_addr[0]} "
            f"id_req {request_id} "
            f"value {value} "
            f"quorum {best_count}/{quorum_needed} "
            f"(expected_rms {len(entry['expected_rms'])}) "
            f"num_reqs {agreed_reqs} "
            f"total_sum {agreed_sum}",
            flush=True,
        )

        # Mantem uma janela limitada de operacoes finalizadas.
        if seq_id > 128:
            old_seq = seq_id - 128
            old_entry = self.pending_quorums.get(old_seq)

            if old_entry is not None and old_entry["completed"]:
                self.pending_quorums.pop(old_seq, None)

        self._send_ack(state, client_addr)
        self.next_commit_seq += 1

    def _mark_quorum_ready_locked(self,seq_id: int,entry: dict,best_result: tuple[int, int],best_count: int) -> None:
        """Registra que a operacao atingiu quorum, sem consolida-la fora de ordem."""

        if entry["completed"] or entry["quorum_ready"]:
            return

        entry["quorum_ready"] = True
        entry["quorum_result"] = best_result
        entry["quorum_count"] = best_count

        print(
            f"{self._timestamp()} FE QUORUM_READY "
            f"seq_id {seq_id} "
            f"votes {best_count}/{entry['quorum_needed']}",
            flush=True,
        )

    def _commit_ready_entries_locked(self) -> None:
        """Consolida operacoes prontas seguindo estritamente a ordem dos seq_id."""

        while True:
            entry = self.pending_quorums.get(self.next_commit_seq)

            if entry is None:
                return
            if entry["completed"]:
                self.next_commit_seq += 1
                continue
            if not entry["quorum_ready"]:
                return

            best_result = entry["quorum_result"]
            best_count = entry["quorum_count"]

            if best_result is None:
                return

            seq_id = self.next_commit_seq
            self._commit_entry_locked(seq_id,entry,best_result,best_count)    

    def _monitor_pending_quorums(self) -> None:
        """Retransmite operacoes multicast que ainda nao atingiram quorum."""

        while not self.stop_event.wait(REPLICATION_CHECK_INTERVAL):

            now = time.monotonic()
            packets_to_retry: list[tuple[int, bytes, int]] = []

            with self.coord_lock:
                for seq_id, entry in self.pending_quorums.items():
                    if entry["completed"]:
                        continue
                    if entry["quorum_ready"]:
                        continue
                    if entry["quorum_needed"] <= 0:
                        continue

                    elapsed = now - entry["last_send"]

                    if elapsed < REPLICATION_TIMEOUT:
                        continue

                    entry["last_send"] = now
                    entry["retry_count"] += 1

                    packets_to_retry.append(
                        (
                            seq_id,
                            entry["packet"],
                            entry["retry_count"],
                        )
                    )

            # O envio eh feito fora do lock.
            for seq_id, packet, retry_count in packets_to_retry:
                print(
                    f"{self._timestamp()} FE REPLICATION_RETRY "
                    f"seq_id {seq_id} "
                    f"attempt {retry_count}",
                    flush=True,
                )

                self._dispatch_packet(packet)
    
    def _handle_replica_ack(self,data: bytes,address: tuple[str, int]) -> None:
        """Registra o voto de um RM e conclui a operacao ao atingir quorum."""

        try:
            ack = unpack_replica_ack(data)
        except ProtocolError:
            return

        with self.coord_lock:
            entry = self.pending_quorums.get(ack.seq_id)

            if entry is None or entry["completed"]:
                return

            rm_key = address[0]

            if rm_key not in entry["expected_rms"]:
                print(
                    f"{self._timestamp()} FE INVALID_RM_ACK "
                    f"seq_id {ack.seq_id} "
                    f"sender {rm_key}",
                    flush=True,
                )
                return

            # Um novo ACK do mesmo RM substitui o voto anterior,
            # mas nunca e contado duas vezes.
            entry["votes"][rm_key] = (ack.num_reqs, ack.total_sum)

            quorum_result = self._find_quorum_result(entry)

            if quorum_result is None:
                return

            best_result, best_count = quorum_result

            self._mark_quorum_ready_locked(ack.seq_id, entry, best_result, best_count)
            self._commit_ready_entries_locked()

    def handle_rm_failure(self, rm_ip: str) -> None:
        """Remove um RM inativo e reavalia os quoruns pendentes."""

        with self.coord_lock:
            if rm_ip not in self.rm_ips:
                return

            self.rm_ips.discard(rm_ip)
            self.rm_last_seen.pop(rm_ip, None)

            print(
                f"{self._timestamp()} FE RM_FAILURE "
                f"rm {rm_ip} "
                f"active_rms {len(self.rm_ips)}",
                flush=True,
            )

            for seq_id, entry in list(self.pending_quorums.items()):
                
                if entry["completed"]:
                    continue

                if rm_ip not in entry["expected_rms"]:
                    continue

                entry["expected_rms"].discard(rm_ip)
                entry["votes"].pop(rm_ip, None)
                entry["quorum_ready"] = False
                entry["quorum_result"] = None
                entry["quorum_count"] = 0

                active_count = len(entry["expected_rms"])

                if active_count == 0:
                    entry["quorum_needed"] = 0

                    print(
                        f"{self._timestamp()} FE QUORUM_UNAVAILABLE "
                        f"seq_id {seq_id}",
                        flush=True,
                    )
                    continue

                entry["quorum_needed"] = self._calc_quorum(active_count)

                print(
                    f"{self._timestamp()} FE QUORUM_UPDATED "
                    f"seq_id {seq_id} "
                    f"removed_rm {rm_ip} "
                    f"expected_rms {active_count} "
                    f"quorum {entry['quorum_needed']}",
                    flush=True)
                
                quorum_result = self._find_quorum_result(entry)

                if quorum_result is None:
                    continue

                best_result, best_count = quorum_result

                print(
                    f"{self._timestamp()} "
                    f"FE QUORUM_REACHED_AFTER_FAILURE "
                    f"seq_id {seq_id}",
                    flush=True,
                )

                self._mark_quorum_ready_locked(
                    seq_id,
                    entry,
                    best_result,
                    best_count,
                )

                self._commit_ready_entries_locked()


    def _handle_request(self, data: bytes, address: tuple[str, int]) -> None:
        """Recebe requisicao do cliente, atribui seq_id e repassa as replicas."""
        try:
            # Rejeita datagramas com tipo, tamanho ou formato incorretos antes
            # de acessar os campos internos.
            request = unpack_request(data)
        except ProtocolError:
            return
        
        client_ip = address[0]
        request_key = (client_ip, request.request_id)
        immediate_ack: tuple[ClientState, str] | None = None
        
        # Estado e acumuladores ficam sob coord_lock; I/O ocorre fora do lock.
        with self.coord_lock:
            
            state = self.clients.get(address[0])
            if state is None:
                # Uma requisicao so eh valida apos a descoberta; nao a processar
                return

            # Como os IDs comecam em 1, um cliente recem-descoberto espera ID 1.
            expected = state.last_req + 1
            if request.request_id == expected and request.value > 0:
                
                request_key = (address[0], request.request_id)
                existing_seq = self.pending_clients.get(request_key)

                if existing_seq is not None:
                    entry = self.pending_quorums.get(existing_seq)

                    if entry is None or entry["completed"]:
                        return

                    if entry["quorum_ready"]:
                        print(
                            f"{self._timestamp()} FE WAITING_COMMIT "
                            f"seq_id {existing_seq} "
                            f"client {address[0]} "
                            f"id_req {request.request_id}",
                            flush=True,
                        )
                        return

                    # O mesmo request_id nao pode aparecer com outro valor.
                    if entry["value"] != request.value:
                        print(
                            f"{self._timestamp()} FE CONFLICT "
                            f"client {address[0]} "
                            f"id_req {request.request_id} "
                            f"original_value {entry['value']} "
                            f"received_value {request.value}",
                            flush=True,
                        )
                        return

                    seq_id = existing_seq
                    packet = entry["packet"]
                    entry["last_send"] = time.monotonic()
                    entry["retry_count"] += 1
                    is_retry = True

                else:
                    if (self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - request.value):
                        return

                    if not self.rm_ips:
                        if not self.allow_local_commit:
                            print(
                                f"{self._timestamp()} FE NO_REPLICAS "
                                f"client {address[0]} "
                                f"id_req {request.request_id}",
                                flush=True,
                            )
                            return

                        # Depois da queda do FE, o RM vencedor pode ser o
                        # único servidor sobrevivente. O seu estado local já
                        # contém somente operações que haviam sido
                        # confirmadas antes da promoção; portanto, sem
                        # operações pendentes, ele pode continuar o serviço
                        # localmente até outra réplica entrar no grupo.
                        #
                        # Se houver uma operação pendente mais antiga, não é
                        # seguro pular sua posição na ordem global.
                        if self.next_commit_seq != self.next_seq:
                            print(
                                f"{self._timestamp()} FE NO_QUORUM_PENDING "
                                f"client {address[0]} "
                                f"id_req {request.request_id}",
                                flush=True,
                            )
                            return

                        seq_id = self.next_seq
                        self.next_seq += 1
                        self.next_commit_seq += 1
                        self.num_reqs += 1
                        self.total_sum += request.value
                        state.last_req = request.request_id
                        state.last_num_reqs = self.num_reqs
                        state.last_total_sum = self.total_sum

                        ack_snapshot = ClientState(
                            last_req=state.last_req,
                            last_num_reqs=state.last_num_reqs,
                            last_total_sum=state.last_total_sum,
                        )
                        log_msg = (
                            f"{self._timestamp()} FE LOCAL_COMMIT "
                            f"seq_id {seq_id} "
                            f"client {client_ip} "
                            f"id_req {request.request_id} "
                            f"value {request.value} "
                            f"num_reqs {self.num_reqs} "
                            f"total_sum {self.total_sum}"
                        )
                        immediate_ack = (ack_snapshot, log_msg)

                    else:
                        seq_id = self.next_seq
                        self.next_seq += 1

                        packet = pack_replicate(
                            seq_id,
                            request.value,
                            client_ip,
                            request.request_id,
                        )

                        self.pending_clients[request_key] = seq_id
                        expected_rms = set(self.rm_ips)
                        quorum_needed = self._calc_quorum(len(expected_rms))

                        self.pending_quorums[seq_id] = {
                            "client_addr": address,
                            "request_id": request.request_id,
                            "value": request.value,
                            "packet": packet,
                            "expected_rms": expected_rms,
                            "quorum_needed": quorum_needed,
                            "votes": {},
                            "completed": False,
                            "quorum_ready": False,
                            "quorum_result": None,
                            "quorum_count": 0,
                            "last_send": time.monotonic(),
                            "retry_count": 0
                        }
                        is_retry = False

                        action = "RETRY" if is_retry else "DISPATCH"
                        log_msg = (
                            f"{self._timestamp()} FE {action} "
                            f"seq_id {seq_id} "
                            f"client {client_ip} "
                            f"id_req {request.request_id} "
                            f"value {request.value}"
                        )
            
            elif request.request_id <= state.last_req:
                # O servidor ja aplicou esse ID. A retransmissao acontece, por
                # exemplo, quando o ACK original se perde na rede.
                
                ack_snapshot = ClientState(
                    last_req=state.last_req,
                    last_num_reqs=state.last_num_reqs,
                    last_total_sum=state.last_total_sum,
                )
                
                log_msg = (
                    f"{self._timestamp()} client {address[0]} DUP!! id_req {request.request_id} "
                    f"value {request.value} num_reqs {ack_snapshot.last_num_reqs} "
                    f"total_sum {ack_snapshot.last_total_sum}"
                )
                immediate_ack = (ack_snapshot, log_msg)
            
            else:
                # Um ID maior indica que falta ao menos uma requisicao anterior.
                # O ACK enviado abaixo informa ao cliente ate onde o servidor foi.
                
                ack_snapshot = ClientState(
                    last_req=state.last_req,
                    last_num_reqs=state.last_num_reqs,
                    last_total_sum=state.last_total_sum,
                )
                
                log_msg = (
                    f"{self._timestamp()} client {address[0]} OUT_OF_ORDER id_req "
                    f"{request.request_id} expected {expected}"
                )
                immediate_ack = (ack_snapshot, log_msg)

        if immediate_ack is not None:
            ack_snapshot, log_msg = immediate_ack
            print(log_msg, flush=True)
            self._send_ack(ack_snapshot, address)
            return

        print(log_msg, flush=True)
        self._dispatch_packet(packet)


    def _send_alive(self) -> None:
        """Envia sinal ALIVE periódico para as réplicas monitorarem o FE."""
        while not self.stop_event.is_set():
            with self.coord_lock:
                active_rms = set(self.rm_ips)
            for ip in active_rms:
                try:
                    self.heartbeat_socket.sendto(ALIVE, (ip, self.port))
                except OSError:
                    return
            self.stop_event.wait(HEARTBEAT)

    def _monitor_rm_failures(self) -> None:
        """Remove RMs que não enviaram heartbeat três vezes seguidas."""
        missed: dict[str, int] = {}
        while not self.stop_event.wait(RM_HEARTBEAT_INTERVAL):
            now = time.monotonic()
            with self.coord_lock:
                monitored = set(self.rm_ips)
            for rm_ip in monitored:
                with self.coord_lock:
                    last_seen = self.rm_last_seen.get(rm_ip)
                if last_seen is None:
                    continue
                if now - last_seen > RM_HEARTBEAT_TIMEOUT:
                    missed[rm_ip] = missed.get(rm_ip, 0) + 1
                    print(
                        f"{self._timestamp()} FE RM_HEARTBEAT_MISS "
                        f"rm {rm_ip} missed {missed[rm_ip]}",
                        flush=True,
                    )
                else:
                    missed[rm_ip] = 0
                if missed.get(rm_ip, 0) >= RM_HEARTBEAT_FAILURES:
                    missed.pop(rm_ip, None)
                    print(
                        f"{self._timestamp()} FE RM_FAILURE "
                        f"detected rm {rm_ip}",
                        flush=True,
                    )
                    self.handle_rm_failure(rm_ip)
                    self._send_topology()

    def serve_forever(self) -> None:
        """Loop sequencial que recebe os datagramas UDP"""
        # mantem heartbeat em thread separada para não bloquear a função
        threading.Thread(target=self._send_alive, daemon=True).start()
        threading.Thread(target=self._monitor_pending_quorums,daemon=True).start()
        threading.Thread(target=self._monitor_rm_failures, daemon=True).start()
        # Após uma promoção os RMs precisam receber imediatamente o novo
        # anel, sem depender do próximo ingresso no cluster.
        self._send_topology()

        while not self.stop_event.is_set():
            try:
                data, address = self.recv_packet(0.1)
            except socket.timeout:
                continue
            except (OSError, ValueError):
                return
            message_type = data[:1]
            if message_type == DISCOVER and len(data) == 1:
                self._handle_discovery(address)
            elif message_type == SERVER_DISCOVER and len(data) == 1:
                self._handle_server_discovery(address)
            elif message_type == RM_JOIN and len(data) == 1:
                self._handle_rm_join(address)
            elif message_type == RM_HEARTBEAT and len(data) == 1:
                self._handle_rm_heartbeat(address)
            elif message_type == RM_SNAPSHOT_ACK and len(data) == 1:
                self._handle_rm_snapshot_ack(data, address)
            elif message_type == RM_LEAVE and len(data) == 1:
                self._handle_rm_leave(address)
            elif message_type == REQUEST:
                self.request_executor.submit(self._handle_request, data, address)
            elif message_type == REPLICA_ACK:
                self._handle_replica_ack(data, address)
            elif message_type in (ELECTION, COORDINATOR):
                self._ack_ring_control(data, address)
            elif message_type == EXIT and len(data) == 1:
                # A saída não altera o acumulador; somente remove o participante
                # que explicitamente deixou o serviço.
                # Lock evita corrida com _handle_request e _commit_entry_locked.
                with self.coord_lock:
                    self.clients.pop(address[0], None)


class Replica(SumServer):
    """Processa requisições recebidas do FE e monitora o heartbeat."""

    def __init__(self, port: int, fe_ip: str, own_ip: str):
        super().__init__(port, own_ip)
        self.fe_ip = fe_ip
        self.rm_ips: set[str] = {own_ip}
        self.synced = False

        self.num_reqs = 0
        self.total_sum = 0
        self.last_alive = time.monotonic()
        
        # Controle de ordenacao total e deduplicacao para replicacao ativa
        self.state_lock = threading.Lock()
        self.expected_seq = 1
        self.pending_ops: dict[int, "ReplicateOp"] = {}
        self.recent_acks: dict[int, tuple["ReplicateOp", int, int]] = {}
        # O estado dos clientes é replicado junto à operação. Ele é tão
        # importante quanto o acumulador quando a réplica vira FE: permite
        # reconhecer uma retransmissão do cliente após um failover.
        self.client_states: dict[str, ClientState] = {}

        self.stop_event = threading.Event()
        self.promotion_lock = threading.Lock()
        self.replacement: FrontEnd | None = None
        self.election_in_progress = False
        self.current_election_id: int | None = None
        self.election_deadline = 0.0
        # Maior candidato observado desde que a ausência do FE foi detectada.
        # Ele é compartilhado por todos os tokens concorrentes da mesma rodada
        # local; portanto, um token antigo com candidato menor não consegue
        # desfazer a eleição de um candidato maior já visto.
        self.best_election_candidate: str | None = None
        self.coordinator_scheduled = False
        # Após receber um coordenador, aguarda uma breve janela para o vencedor
        # trocar de papel e iniciar seus heartbeats antes de suspeitar dele.
        self.leader_grace_until = 0.0
        self.ring_ack_events: dict[tuple[int, str], threading.Event] = {}

        # Socket exclusivo para receber operacoes multicast do FE.
        self.multicast_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM,socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.multicast_socket.bind(("", MULTICAST_PORT))
        membership = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton(own_ip)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP,socket.IP_ADD_MEMBERSHIP,membership)
        
        
    def _apply_value_locked(self, value: int) -> bool:
        """Aplica um valor inteiro positivo ao acumulador local respeitando uint64."""
        if value <= 0:
            return False
        if self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - value:
            return False
        self.num_reqs += 1
        self.total_sum += value
        return True

    def _record_client_state_locked(
        self,
        op: "ReplicateOp",
        num_reqs: int,
        total_sum: int,
    ) -> None:
        """Atualiza o retrato replicado do ACK do cliente que originou ``op``."""
        state = self.client_states.get(op.client_ip)
        if state is None:
            state = ClientState()
            self.client_states[op.client_ip] = state
        # A ordenação global pode intercalar clientes, mas cada cliente é
        # stop-and-wait: um ID maior sempre substitui seu retrato anterior.
        if op.request_id >= state.last_req:
            state.last_req = op.request_id
            state.last_num_reqs = num_reqs
            state.last_total_sum = total_sum
    
    
    def _handle_replicate(self, data: bytes, address: tuple[str, int]) -> None:
        """Ordena, aplica e confirma operações recebidas do FE."""

        if self.stop_event.is_set():
            return

        try:
            op = unpack_replicate(data)
        except ProtocolError:
            return

        # Leitura de fe_ip sob state_lock para acompanhar atualizacao de topologia.
        with self.state_lock:
            current_fe = self.fe_ip

        if address[0] != current_fe:
            return

        acknowledgements: list[tuple["ReplicateOp", int, int]] = []
        applied_count = 0

        with self.state_lock:

            if op.seq_id == 0 or op.value == 0:
                return

            if op.seq_id < self.expected_seq:
                previous_ack = self.recent_acks.get(op.seq_id)

                if previous_ack is not None:
                    previous_op, num_reqs, total_sum = previous_ack

                    if previous_op != op:
                        print(
                            f"{self._timestamp()} RM CONFLICT seq_id {op.seq_id}",
                            flush=True,
                        )
                        return

                    acknowledgements.append((op, num_reqs, total_sum))
                
                else:
                    print(
                        f"{self._timestamp()} RM DUP seq_id {op.seq_id} "
                        f"expected_seq {self.expected_seq} ACK não encontrado",
                        flush=True,
                    )

            elif op.seq_id > self.expected_seq:
                if op.seq_id not in self.pending_ops:
                    self.pending_ops[op.seq_id] = op

                elif self.pending_ops[op.seq_id] != op:
                    print(
                        f"{self._timestamp()} RM CONFLICT seq_id {op.seq_id}",
                        flush=True,
                    )
                    return

                print(
                    f"{self._timestamp()} RM BUFFERED seq_id {op.seq_id} "
                    f"value {op.value} expected_seq {self.expected_seq}",
                    flush=True,
                )

            else:
                if not self._apply_value_locked(op.value):
                    print(
                        f"{self._timestamp()} RM INVALID seq_id {op.seq_id}",
                        flush=True,
                    )
                    return

                acknowledgements.append((op, self.num_reqs, self.total_sum))
                self._record_client_state_locked(op, self.num_reqs, self.total_sum)

                self.expected_seq += 1
                applied_count += 1

                while self.expected_seq in self.pending_ops:
                    buffered_op = self.pending_ops[self.expected_seq]
                    buffered_value = buffered_op.value

                    if not self._apply_value_locked(buffered_value):
                        print(
                            f"{self._timestamp()} RM INVALID "
                            f"seq_id {self.expected_seq}",
                            flush=True,
                        )
                        break

                    self.pending_ops.pop(self.expected_seq)

                    acknowledgements.append(
                        (buffered_op, self.num_reqs, self.total_sum)
                    )
                    self._record_client_state_locked(
                        buffered_op,
                        self.num_reqs,
                        self.total_sum,
                    )

                    self.expected_seq += 1
                    applied_count += 1

            for operation, num_reqs, total_sum in acknowledgements:
                self.recent_acks[operation.seq_id] = (operation, num_reqs, total_sum)

                old_seq = operation.seq_id - 128
                self.recent_acks.pop(old_seq, None)

            current_reqs = self.num_reqs
            current_sum = self.total_sum

        for operation, num_reqs, total_sum in acknowledgements:
            self.socket.sendto(
                pack_replica_ack(operation.seq_id, num_reqs, total_sum),
                (current_fe, self.port),
            )

        if applied_count > 0:
            print(
                f"{self._timestamp()} RM APPLIED seq_id {op.seq_id} "
                f"(+{applied_count} seqs) "
                f"num_reqs {current_reqs} total_sum {current_sum}",
                flush=True,
            )

    def _join_cluster(self) -> None:
        """Solicita registro e aguarda snapshot antes de entrar no quorum."""
        
        while not self.stop_event.is_set():
            with self.state_lock:
                if self.synced:
                    return
                target_fe = self.fe_ip
            print(
                f"{self._timestamp()} RM JOIN_REQUEST fe {target_fe}",
                flush=True,
            )
            try:
                self.socket.sendto(RM_JOIN, (target_fe, self.port))
            except OSError:
                return
            self.stop_event.wait(RM_HEARTBEAT_INTERVAL)

    def _handle_rm_snapshot(self, data: bytes) -> None:
        """Substitui o estado local pelo snapshot consolidado do FE."""
        try:
            snapshot = unpack_rm_snapshot(data)
        except ProtocolError:
            return
        with self.state_lock:
            self.num_reqs = snapshot.num_reqs
            self.total_sum = snapshot.total_sum
            self.expected_seq = snapshot.next_seq
            self.pending_ops.clear()
            self.recent_acks.clear()
            self.client_states = {
                ip: ClientState(last_req, last_num_reqs, last_total_sum)
                for ip, last_req, last_num_reqs, last_total_sum in snapshot.client_states
            }
            self.synced = True
            target_fe = self.fe_ip
        
        print(
            f"{self._timestamp()} RM SNAPSHOT_APPLIED "
            f"num_reqs {snapshot.num_reqs} "
            f"total_sum {snapshot.total_sum} "
            f"expected_seq {snapshot.next_seq}",
            flush=True,
        )
        self.socket.sendto(RM_SNAPSHOT_ACK, (target_fe, self.port))

    def _handle_topology(self, data: bytes) -> None:
        """Atualiza o FE e a lista do anel recebida do coordenador."""
        
        try:
            topology = json.loads(data[len(RM_TOPOLOGY):].decode("utf-8"))
            fe_ip = str(topology["fe_ip"])
            rm_ips = {str(ip) for ip in topology["rm_ips"]}
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
            return
        
        # Protecao de fe_ip e rm_ips compartilhados entre threads da replica.
        with self.state_lock:
            self.fe_ip = fe_ip
            self.rm_ips = rm_ips
            self.last_alive = time.monotonic()
            self.election_in_progress = False
            self.current_election_id = None
            self.leader_grace_until = time.monotonic() + LEADER_STABILIZATION
        print(
            f"{self._timestamp()} RM TOPOLOGY "
            f"fe {fe_ip} "
            f"rms {','.join(sorted(rm_ips))}",
            flush=True,
        )
        
    def _send_rm_heartbeat(self) -> None:
        """Mantém o FE informado de que esta réplica continua ativa."""
        while not self.stop_event.is_set():
            with self.state_lock:
                target_fe = self.fe_ip
            try:
                self.heartbeat_socket.sendto(RM_HEARTBEAT, (target_fe, self.port))
            except OSError:
                return
            self.stop_event.wait(RM_HEARTBEAT_INTERVAL)


    def _monitor_heartbeat(self) -> None:
        """Detecta ausencia de ALIVE consecutivos do FE e dispara eleicao."""
        missed = 0
        while not self.stop_event.wait(HEARTBEAT):
            with self.state_lock:
                current_alive = self.last_alive
                grace_until = self.leader_grace_until
                synced = self.synced
            # Antes de aplicar o snapshot, este nó ainda não faz parte do
            # conjunto ativo do FE. Um snapshot UDP pode ser retransmitido
            # algumas vezes durante a entrada; tratá-lo como queda do líder
            # criaria uma eleição e dois FEs em uma inicialização saudável.
            if not synced:
                missed = 0
                continue
            if time.monotonic() < grace_until:
                missed = 0
                continue
            if time.monotonic() - current_alive > FE_HEARTBEAT_TIMEOUT:
                missed += 1
                if missed == 3:
                    print(
                        f"{self._timestamp()} RM FE_FAILURE_DETECTED "
                        f"fe {self.fe_ip}",
                        flush=True,
                    )
                    self._handle_election()
                    missed = 0
            else:
                missed = 0

    def _handle_election(self) -> None:
        """Inicia uma rodada do anel, evitando rodadas simultâneas locais."""
        now = time.monotonic()
        with self.state_lock:
            if self.replacement is not None:
                return
            if self.election_in_progress and now < self.election_deadline:
                return
            self.election_in_progress = True
            self.election_deadline = now + (RING_ELECTION_TIMEOUT * 4)
            self.best_election_candidate = self.own_ip
            self.coordinator_scheduled = False
            election_id = time.time_ns() & MAX_UINT64
            if election_id == 0:
                election_id = 1
            self.current_election_id = election_id
            members = sorted(self.rm_ips | {self.own_ip}, key=self._ip_value)

        print(
            f"{self._timestamp()} RING ELECTION_START "
            f"id {election_id} initiator {self.own_ip} "
            f"members {','.join(members)}",
            flush=True,
        )
        if len(members) == 1:
            self._announce_coordinator(election_id, self.own_ip)
            return

        packet = pack_ring_message(ELECTION, election_id, self.own_ip, self.own_ip)
        if not self._forward_ring_packet(packet, election_id, "ELECTION"):
            # Todos os outros participantes conhecidos falharam durante a
            # rodada; a única réplica viva se torna coordenadora.
            self._announce_coordinator(election_id, self.own_ip)

    @staticmethod
    def _ip_value(ip: str) -> int:
        return int.from_bytes(socket.inet_aton(ip), byteorder="big")

    def _successors(self) -> list[str]:
        """Lista os demais RMs na ordem cíclica após este nó."""
        with self.state_lock:
            members = sorted(self.rm_ips | {self.own_ip}, key=self._ip_value)
        if self.own_ip not in members:
            return []
        own_index = members.index(self.own_ip)
        return members[own_index + 1 :] + members[:own_index]

    def _forward_ring_packet(self, packet: bytes, election_id: int, label: str) -> bool:
        """Encaminha ao próximo sucessor que confirmar a recepção.

        A confirmação curta torna detectável a queda de um sucessor sem
        bloquear o laço principal que recebe os demais datagramas do anel.
        """
        for successor in self._successors():
            event = threading.Event()
            with self.state_lock:
                self.ring_ack_events[(election_id, successor)] = event
            print(
                f"{self._timestamp()} RING {label}_SEND "
                f"id {election_id} to {successor}",
                flush=True,
            )
            try:
                self.socket.sendto(packet, (successor, self.port))
            except OSError:
                delivered = False
            else:
                delivered = event.wait(RING_ACK_TIMEOUT)
            with self.state_lock:
                self.ring_ack_events.pop((election_id, successor), None)
            if delivered:
                print(
                    f"{self._timestamp()} RING {label}_FORWARDED "
                    f"id {election_id} to {successor}",
                    flush=True,
                )
                return True
            print(
                f"{self._timestamp()} RING SUCCESSOR_UNAVAILABLE "
                f"id {election_id} successor {successor}",
                flush=True,
            )
            with self.state_lock:
                self.rm_ips.discard(successor)
        return False

    def _handle_ring_ack(self, data: bytes) -> None:
        try:
            acknowledgement = unpack_ring_ack(data)
        except ProtocolError:
            return
        with self.state_lock:
            event = self.ring_ack_events.get(
                (acknowledgement.election_id, acknowledgement.sender_ip)
            )
        if event is not None:
            event.set()

    def _reply_ring_ack(self, election_id: int, address: tuple[str, int]) -> None:
        try:
            self.socket.sendto(pack_ring_ack(election_id, self.own_ip), address)
        except OSError:
            pass

    def _handle_ring_election(self, data: bytes, address: tuple[str, int]) -> None:
        try:
            message = unpack_ring_message(data)
        except ProtocolError:
            return
        if message.kind != ELECTION:
            return
        self._reply_ring_ack(message.election_id, address)
        # O encaminhamento espera o ACK do próximo sucessor. Ele precisa
        # rodar fora do laço de recepção para que esse mesmo laço possa receber
        # a confirmação e sinalizar ``ring_ack_events``.
        threading.Thread(
            target=self._process_ring_election,
            args=(message, address),
            daemon=True,
        ).start()

    def _process_ring_election(self, message: object, address: tuple[str, int]) -> None:
        """Processa o token de eleição depois de confirmar a entrega."""
        # ``message`` já foi validada por ``_handle_ring_election``.
        assert hasattr(message, "initiator_ip")
        with self.state_lock:
            # Um coordenador recém-aceito encerra todas as rodadas anteriores.
            # Ainda respondemos ao ACK acima para não parecer uma falha de rede,
            # mas não propagamos mais tokens obsoletos.
            if time.monotonic() < self.leader_grace_until:
                return
            previous_best = self.best_election_candidate
            if previous_best is None or self._ip_value(message.candidate_ip) > self._ip_value(previous_best):
                self.best_election_candidate = message.candidate_ip

        print(
            f"{self._timestamp()} RING ELECTION_RECEIVED "
            f"id {message.election_id} from {address[0]} "
            f"candidate {message.candidate_ip}",
            flush=True,
        )

        if message.initiator_ip == self.own_ip:
            print(
                f"{self._timestamp()} RING ELECTION_COMPLETED "
                f"id {message.election_id} leader {message.candidate_ip}",
                flush=True,
            )
            self._schedule_coordinator(message.election_id)
            return

        candidate = max(
            (message.candidate_ip, self.own_ip),
            key=self._ip_value,
        )
        packet = pack_ring_message(
            ELECTION,
            message.election_id,
            message.initiator_ip,
            candidate,
        )
        if not self._forward_ring_packet(packet, message.election_id, "ELECTION"):
            # O token não conseguiu seguir para outro membro conhecido.  A
            # proclamação também passa pela janela de estabilização para que um
            # token concorrente com candidato maior ainda possa chegar.
            self._schedule_coordinator(message.election_id)

    def _schedule_coordinator(self, election_id: int) -> None:
        """Agenda uma única proclamação após os tokens concorrentes circularem."""
        with self.state_lock:
            if self.replacement is not None or self.coordinator_scheduled:
                return
            self.coordinator_scheduled = True
        threading.Thread(
            target=self._announce_after_settle,
            args=(election_id,),
            daemon=True,
        ).start()

    def _announce_after_settle(self, election_id: int) -> None:
        """Proclama o maior candidato vivo conhecido ao fim da rodada local."""
        if self.stop_event.wait(RING_ELECTION_SETTLE):
            return
        with self.state_lock:
            if self.replacement is not None or not self.election_in_progress:
                return
            leader_ip = self.best_election_candidate or self.own_ip
        self._announce_coordinator(election_id, leader_ip)

    def _announce_coordinator(self, election_id: int, leader_ip: str) -> None:
        """Inicia a volta de proclamação do líder escolhido pelo anel."""
        print(
            f"{self._timestamp()} RING COORDINATOR_ANNOUNCE "
            f"id {election_id} leader {leader_ip}",
            flush=True,
        )
        if not self._successors():
            if leader_ip == self.own_ip:
                self._promote_to_frontend(election_id)
            return
        packet = pack_ring_message(
            COORDINATOR,
            election_id,
            self.own_ip,
            leader_ip,
        )
        self._forward_ring_packet(packet, election_id, "COORDINATOR")

    def _handle_coordinator(self, data: bytes, address: tuple[str, int]) -> None:
        try:
            message = unpack_ring_message(data)
        except ProtocolError:
            return
        if message.kind != COORDINATOR:
            return
        self._reply_ring_ack(message.election_id, address)
        threading.Thread(
            target=self._process_coordinator,
            args=(message, address),
            daemon=True,
        ).start()

    def _process_coordinator(self, message: object, address: tuple[str, int]) -> None:
        """Propaga a proclamação sem bloquear o receptor de ACKs."""
        assert hasattr(message, "candidate_ip")
        with self.state_lock:
            # Não permita que um token atrasado proclame um IP menor depois de
            # uma eleição já concluída.  Isto é o ponto que eliminou a corrida
            # em que .2 substituía indevidamente o FE .3/.4 recém-promovido.
            if (
                time.monotonic() < self.leader_grace_until
                and self._ip_value(message.candidate_ip) < self._ip_value(self.fe_ip)
            ):
                return
            self.fe_ip = message.candidate_ip
            self.last_alive = time.monotonic()
            self.election_in_progress = False
            self.current_election_id = None
            self.best_election_candidate = message.candidate_ip
            self.coordinator_scheduled = False
            self.leader_grace_until = time.monotonic() + LEADER_STABILIZATION
        print(
            f"{self._timestamp()} RING COORDINATOR_RECEIVED "
            f"id {message.election_id} leader {message.candidate_ip}",
            flush=True,
        )

        if message.initiator_ip != self.own_ip:
            packet = pack_ring_message(
                COORDINATOR,
                message.election_id,
                message.initiator_ip,
                message.candidate_ip,
            )
            self._forward_ring_packet(packet, message.election_id, "COORDINATOR")
        else:
            print(
                f"{self._timestamp()} RING COORDINATOR_COMPLETED "
                f"id {message.election_id} leader {message.candidate_ip}",
                flush=True,
            )

        if message.candidate_ip == self.own_ip:
            self._promote_to_frontend(message.election_id)

    def _promote_to_frontend(self, election_id: int) -> None:
        """Interrompe rotinas de RM e entrega o socket ao novo Front-End."""
        with self.promotion_lock:
            if self.replacement is not None:
                return
            print(
                f"{self._timestamp()} RING NEW_FRONTEND "
                f"id {election_id} ip {self.own_ip}",
                flush=True,
            )
            self.stop_event.set()
            try:
                self.multicast_socket.close()
            except OSError:
                pass
            self.replacement = FrontEnd.from_replica(self)
            # Publica a transição antes de o laço principal trocar de classe.
            # Isso evita que outro RM inicie uma segunda eleição na pequena
            # janela entre a proclamação e a primeira iteração do novo FE.
            self.replacement._send_topology()
            with self.replacement.coord_lock:
                remaining_rms = set(self.replacement.rm_ips)
            for rm_ip in remaining_rms:
                try:
                    self.replacement.heartbeat_socket.sendto(ALIVE, (rm_ip, self.port))
                except OSError:
                    continue

    def _receive_multicast(self) -> None:
        """Recebe operações disseminadas pelo FE no grupo multicast."""

        while not self.stop_event.is_set():
            try:
                data, address = self.multicast_socket.recvfrom(65535)
            except OSError:
                return

            if self.stop_event.is_set():
                return

            if data[:1] != REPLICATE:
                continue

            self._handle_replicate(data, address)


    def serve_forever(self) -> FrontEnd | None:
        # mantem monitor do heartbeat em thread separada para nao bloquear a funcao
        threading.Thread(target=self._monitor_heartbeat, daemon=True).start()
        threading.Thread(target=self._receive_multicast, daemon=True).start()
        threading.Thread(target=self._join_cluster, daemon=True).start()
        threading.Thread(target=self._send_rm_heartbeat, daemon=True).start()
        
        while not self.stop_event.is_set():
            try:
                data, address = self.recv_packet(0.1)
            except socket.timeout:
                continue
            except (OSError, ValueError):
                return None
            message_type = data[:1]
            if message_type == ALIVE and len(data) == 1:
                with self.state_lock:
                    if address[0] == self.fe_ip:
                        self.last_alive = time.monotonic()
            elif message_type == RM_HEARTBEAT_ACK and len(data) == 1:
                # Além do ALIVE periódico, este ACK confirma diretamente que
                # o FE recebeu o heartbeat desta réplica. É especialmente
                # importante logo após uma promoção, quando pode haver um
                # pequeno desencontro entre a troca de topologia e o primeiro
                # ALIVE do novo coordenador.
                with self.state_lock:
                    if address[0] == self.fe_ip:
                        self.last_alive = time.monotonic()
            elif message_type == RM_SNAPSHOT:
                self._handle_rm_snapshot(data)
            elif message_type == RM_TOPOLOGY:
                self._handle_topology(data)
            elif message_type == ELECTION:
                self._handle_ring_election(data, address)
            elif message_type == COORDINATOR:
                self._handle_coordinator(data, address)
            elif message_type == RING_ACK:
                self._handle_ring_ack(data)
        return self.replacement




def parse_args() -> int:
    """Lê apenas a porta; papel e IP são definidos por descoberta."""
    parser = argparse.ArgumentParser(description="Servidor UDP de soma confiável")
    parser.add_argument("port", type=int, help="porta UDP (1-65535)")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("a porta deve estar entre 1 e 65535")
    return args.port


def discover_fe(port: int) -> str | None:
    """Procura um FE ativo na rede local sem argumento de configuração."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    probe.bind(("", 0))
    deadline = time.monotonic() + 0.30
    try:
        while time.monotonic() < deadline:
            for destination in broadcast_destinations():
                try:
                    probe.sendto(SERVER_DISCOVER, (destination, port))
                except OSError:
                    # Uma interface pode não aceitar broadcast dirigido; a
                    # outra alternativa de descoberta continua válida.
                    continue
            probe.settimeout(min(0.05, max(0.0, deadline - time.monotonic())))
            try:
                data, address = probe.recvfrom(65535)
            except socket.timeout:
                continue
            if data == SERVER_DISCOVER_ACK:
                return address[0]
    finally:
        probe.close()
    return None


def main() -> None:
    """Inicializa o servidor e garante o fechamento do socket no encerramento."""
    port = parse_args()
    own_ip = SumServer.get_local_ip()
    fe_ip = discover_fe(port)
    if fe_ip is None:
        print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"SERVER DISCOVERY: no FE found, self-proclaim FE ip {own_ip}",
            flush=True,
        )
        server: SumServer = FrontEnd(port, own_ip)
    else:
        print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"SERVER DISCOVERY: FE found ip {fe_ip}; starting as RM",
            flush=True,
        )
        server = Replica(port, fe_ip, own_ip)
    try:
        while True:
            replacement = server.serve_forever()
            if replacement is None:
                break
            server = replacement
    except KeyboardInterrupt:
        pass
    finally:
        if isinstance(server, Replica):
            try:
                with server.state_lock:
                    target_fe = server.fe_ip
                server.socket.sendto(RM_LEAVE, (target_fe, server.port))
            except OSError:
                pass
        elif isinstance(server, FrontEnd):
            server.request_executor.shutdown(wait=False)
        server.socket.close()
        if server.discovery_socket is not None:
            server.discovery_socket.close()
        server.heartbeat_socket.close()
        multicast_socket = getattr(server, "multicast_socket", None)

        if multicast_socket is not None:
            multicast_socket.close()


if __name__ == "__main__":
    main()
