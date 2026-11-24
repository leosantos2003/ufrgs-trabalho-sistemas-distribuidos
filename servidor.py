#!/usr/bin/env python3
"""Servidor UDP confiável para a agregação de números inteiros.

Dois tipos de servidor:
- FrontEnd (Servidor.FE): atende clientes, repassa requisições às réplicas.
- Replica  (Servidor.RM): processa a soma, monitora heartbeat, participa de eleição.
"""

from __future__ import annotations

import argparse
import json
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
    DISCOVER,
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
    SERVER_DISCOVER,
    SERVER_DISCOVER_ACK,
    ProtocolError,
    pack_ack,
    pack_discover_ack,
    pack_replica_ack,
    pack_replicate,
    unpack_replica_ack,
    unpack_replicate,
    unpack_request,
    pack_rm_snapshot,
    unpack_rm_snapshot,
)

HEARTBEAT = 0.1

MULTICAST_GROUP = "239.0.0.1"
MULTICAST_TTL = 1
MULTICAST_PORT = 6571
REPLICATION_TIMEOUT = 0.05
REPLICATION_CHECK_INTERVAL = 0.01
RM_HEARTBEAT_INTERVAL = 0.1
RM_HEARTBEAT_TIMEOUT = 0.05
RM_HEARTBEAT_FAILURES = 3


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
        # O endereço vazio significa "todas as interfaces IPv4". Dessa forma,
        # o servidor recebe tanto broadcasts de descoberta quanto unicast.
        self.socket.bind(("", port))

        self.port = port
        self.own_ip = own_ip

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
    
    def __init__(self, port: int, own_ip: str):
        
        super().__init__(port, own_ip)

        self.fe_ip = own_ip
        self.rm_ips: set[str] = set()
        self.rm_last_seen: dict[str, float] = {}
        self.rm_sync_pending: set[str] = set()

        # A chave é o endereço IP porque o enunciado define o participante por
        # esse campo. A porta UDP serve somente para responder ao datagrama.
        self.clients: dict[str, ClientState] = {}
        self.num_reqs = 0
        self.total_sum = 0
        
        # Controle do sequenciador global e da agregacao de quorum dos RMs
        self.coord_lock = threading.Lock()
        self.next_seq = 1
        self.pending_quorums: dict[int, dict] = {}
        self.pending_clients: dict[tuple[str, int], int] = {}
        self.request_executor = ThreadPoolExecutor(max_workers=64, thread_name_prefix="fe-request")

        # Próxima operação que pode ser consolidada no estado do FE.
        self.next_commit_seq = 1

        # Socket usado exclusivamente para disseminar operacoes aos RMs.
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)

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
            self.socket.sendto(packet, (rm_ip, self.port))

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
            pack_rm_snapshot(num_reqs, total_sum, next_seq),
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
        self.socket.sendto(RM_HEARTBEAT_ACK, (rm_ip, self.port))

    def _handle_rm_leave(self, address: tuple[str, int]) -> None:
        rm_ip = address[0]
        self.handle_rm_failure(rm_ip)
        self._send_topology()

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

        while True:
            time.sleep(REPLICATION_CHECK_INTERVAL)

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
                    if not self.rm_ips:
                        print(
                            f"{self._timestamp()} FE NO_REPLICAS "
                            f"client {address[0]} "
                            f"id_req {request.request_id}",
                            flush=True,
                        )
                        return
                    
                    if (self.num_reqs == MAX_UINT64 or self.total_sum > MAX_UINT64 - request.value):
                        return

                    seq_id = self.next_seq
                    self.next_seq += 1

                    packet = pack_replicate(seq_id, request.value)

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
        while True:
            with self.coord_lock:
                active_rms = set(self.rm_ips)
            for ip in active_rms:
                self.socket.sendto(ALIVE, (ip, self.port))
            time.sleep(HEARTBEAT)

    def _monitor_rm_failures(self) -> None:
        """Remove RMs que não enviaram heartbeat três vezes seguidas."""
        missed: dict[str, int] = {}
        while True:
            time.sleep(RM_HEARTBEAT_INTERVAL)
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

        while True:
            data, address = self.socket.recvfrom(65535)
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
        self.pending_ops: dict[int, int] = {}
        self.recent_acks: dict[int, tuple[int, int, int]] = {}

        # Socket exclusivo para receber operacoes multicast do FE.
        self.multicast_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM,socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.multicast_socket.bind(("", MULTICAST_PORT))
        membership = (socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton("0.0.0.0"))
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
    
    
    def _handle_replicate(self, data: bytes, address: tuple[str, int]) -> None:
        """Ordena, aplica e confirma operações recebidas do FE."""

        try:
            op = unpack_replicate(data)
        except ProtocolError:
            return

        # Leitura de fe_ip sob state_lock para acompanhar atualizacao de topologia.
        with self.state_lock:
            current_fe = self.fe_ip

        if address[0] != current_fe:
            return

        acknowledgements = []
        applied_count = 0

        with self.state_lock:

            if op.seq_id == 0 or op.value == 0:
                return

            if op.seq_id < self.expected_seq:
                previous_ack = self.recent_acks.get(op.seq_id)

                if previous_ack is not None:
                    previous_value, num_reqs, total_sum = previous_ack

                    if previous_value != op.value:
                        print(
                            f"{self._timestamp()} RM CONFLICT seq_id {op.seq_id}",
                            flush=True,
                        )
                        return

                    acknowledgements.append((op.seq_id, op.value, num_reqs, total_sum))
                
                else:
                    print(
                        f"{self._timestamp()} RM DUP seq_id {op.seq_id} "
                        f"expected_seq {self.expected_seq} ACK não encontrado",
                        flush=True,
                    )

            elif op.seq_id > self.expected_seq:
                if op.seq_id not in self.pending_ops:
                    self.pending_ops[op.seq_id] = op.value

                elif self.pending_ops[op.seq_id] != op.value:
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

                acknowledgements.append(
                    (self.expected_seq, op.value, self.num_reqs, self.total_sum)
                )

                self.expected_seq += 1
                applied_count += 1

                while self.expected_seq in self.pending_ops:
                    buffered_value = self.pending_ops[self.expected_seq]

                    if not self._apply_value_locked(buffered_value):
                        print(
                            f"{self._timestamp()} RM INVALID "
                            f"seq_id {self.expected_seq}",
                            flush=True,
                        )
                        break

                    self.pending_ops.pop(self.expected_seq)

                    acknowledgements.append(
                        (
                            self.expected_seq,
                            buffered_value,
                            self.num_reqs,
                            self.total_sum,
                        )
                    )

                    self.expected_seq += 1
                    applied_count += 1

            for seq_id, value, num_reqs, total_sum in acknowledgements:
                self.recent_acks[seq_id] = (value, num_reqs, total_sum)

                old_seq = seq_id - 128
                self.recent_acks.pop(old_seq, None)

            current_reqs = self.num_reqs
            current_sum = self.total_sum

        for seq_id, value, num_reqs, total_sum in acknowledgements:
            self.socket.sendto( pack_replica_ack(seq_id, num_reqs, total_sum), (current_fe, self.port))

        if applied_count > 0:
            print(
                f"{self._timestamp()} RM APPLIED seq_id {op.seq_id} "
                f"(+{applied_count} seqs) "
                f"num_reqs {current_reqs} total_sum {current_sum}",
                flush=True,
            )

    def _join_cluster(self) -> None:
        """Solicita registro e aguarda snapshot antes de entrar no quorum."""
        
        while True:
            with self.state_lock:
                if self.synced:
                    return
                target_fe = self.fe_ip
            print(
                f"{self._timestamp()} RM JOIN_REQUEST fe {target_fe}",
                flush=True,
            )
            self.socket.sendto(RM_JOIN, (target_fe, self.port))
            time.sleep(RM_HEARTBEAT_INTERVAL)

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
        print(
            f"{self._timestamp()} RM TOPOLOGY "
            f"fe {fe_ip} "
            f"rms {','.join(sorted(rm_ips))}",
            flush=True,
        )
        
    def _send_rm_heartbeat(self) -> None:
        """Mantém o FE informado de que esta réplica continua ativa."""
        while True:
            with self.state_lock:
                target_fe = self.fe_ip
            self.socket.sendto(RM_HEARTBEAT, (target_fe, self.port))
            time.sleep(RM_HEARTBEAT_INTERVAL)


    def _monitor_heartbeat(self) -> None:
        """Detecta ausencia de ALIVE consecutivos do FE e dispara eleicao."""
        missed = 0
        last_seen = self.last_alive
        while True:
            time.sleep(HEARTBEAT)
            if self.last_alive == last_seen:
                missed += 1
                if missed == 3:
                    # FE não mandou ALIVE no tempo esperado
                    #### Ponto de integracao da eleicao por anel e promocao a FE.
                    self._handle_election()
                    missed = 0
            else:
                missed = 0
                last_seen = self.last_alive

    def _handle_election(self) -> None:
        """Participa da eleição por anel para escolher novo FE."""
        ...

    def _receive_multicast(self) -> None:
        """Recebe operações disseminadas pelo FE no grupo multicast."""

        while True:
            try:
                data, address = self.multicast_socket.recvfrom(65535)
            except OSError:
                return

            if data[:1] != REPLICATE:
                continue

            self._handle_replicate(data, address)


    def serve_forever(self) -> None:
        # mantem monitor do heartbeat em thread separada para nao bloquear a funcao
        threading.Thread(target=self._monitor_heartbeat, daemon=True).start()
        threading.Thread(target=self._receive_multicast, daemon=True).start()
        threading.Thread(target=self._join_cluster, daemon=True).start()
        threading.Thread(target=self._send_rm_heartbeat, daemon=True).start()
        
        while True:
            data, address = self.socket.recvfrom(65535)
            message_type = data[:1]
            if message_type == ALIVE and len(data) == 1:
                self.last_alive = time.monotonic()
            elif message_type == RM_SNAPSHOT:
                self._handle_rm_snapshot(data)
            elif message_type == RM_TOPOLOGY:
                self._handle_topology(data)




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
            probe.sendto(SERVER_DISCOVER, ("255.255.255.255", port))
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
        server.serve_forever()
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
        multicast_socket = getattr(server, "multicast_socket", None)

        if multicast_socket is not None:
            multicast_socket.close()


if __name__ == "__main__":
    main()
