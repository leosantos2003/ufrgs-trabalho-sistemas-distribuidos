"""Formato binário das mensagens do serviço de soma sobre UDP.

O protocolo deliberadamente usa datagramas pequenos e de tamanho fixo nas
mensagens de processamento. Isto evita custo de serialização desnecessário
quando arquivos grandes são usados como entrada dos clientes.

Os programas não trocam objetos Python nem texto JSON. Eles trocam somente
bytes, definidos aqui de forma centralizada para garantir que cliente e
servidor concordem sobre os tipos, a ordem dos campos e os tamanhos.
"""

from __future__ import annotations

import socket
import struct
import sys
from dataclasses import dataclass
from typing import Final, Mapping

# Limite máximo representável pelos campos ``Q`` do módulo ``struct``.
# Python aceita inteiros maiores, mas o protocolo precisa obedecer ao requisito
# de valores unsigned de 64 bits.
MAX_UINT64: Final[int] = (1 << 64) - 1


def broadcast_destinations() -> tuple[str, ...]:
    """Retorna os broadcasts adequados à interface de rota padrão.

    ``255.255.255.255`` é o broadcast limitado previsto originalmente, mas
    algumas redes Wi-Fi encaminham somente o broadcast dirigido da sub-rede.
    Em Linux, a informação da interface padrão é obtida do sistema e o seu
    broadcast é consultado por ``ioctl``. Em outros sistemas, ou em caso de
    indisponibilidade dessa informação, a função preserva o fallback padrão.

    Nenhum endereço é recebido por argumento, arquivo de configuração ou
    ambiente: a escolha continua sendo feita pela própria interface de rede.
    """
    destinations = ["255.255.255.255"]
    if not sys.platform.startswith("linux"):
        return tuple(destinations)

    try:
        import fcntl

        default_interface = None
        with open("/proc/net/route", encoding="ascii") as routes:
            next(routes, None)
            for line in routes:
                fields = line.split()
                if len(fields) < 4:
                    continue
                interface, destination, flags = fields[0], fields[1], fields[3]
                if destination == "00000000" and int(flags, 16) & 0x1:
                    default_interface = interface
                    break

        if default_interface is None:
            return tuple(destinations)

        request = struct.pack("256s", default_interface[:15].encode("ascii"))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            response = fcntl.ioctl(probe.fileno(), 0x8919, request)
        directed_broadcast = socket.inet_ntoa(response[20:24])
        if directed_broadcast not in destinations:
            destinations.append(directed_broadcast)
    except (OSError, UnicodeEncodeError, ValueError):
        pass

    return tuple(destinations)

# Cada mensagem inicia com um byte que identifica sua finalidade. As mensagens
# de descoberta e saída não precisam de campos adicionais; por isso ocupam um
# único byte no datagrama UDP.
DISCOVER: Final[bytes] = b"D"
DISCOVER_ACK: Final[bytes] = b"S"
REQUEST: Final[bytes] = b"R"
ACK: Final[bytes] = b"A"
EXIT: Final[bytes] = b"E"
ALIVE: Final[bytes] = b"H"
SERVER_DISCOVER: Final[bytes] = b"J"
SERVER_DISCOVER_ACK: Final[bytes] = b"K"
RM_JOIN: Final[bytes] = b"G"
RM_JOIN_ACK: Final[bytes] = b"N"
RM_HEARTBEAT: Final[bytes] = b"B"
RM_HEARTBEAT_ACK: Final[bytes] = b"O"
RM_SNAPSHOT: Final[bytes] = b"P"
RM_SNAPSHOT_ACK: Final[bytes] = b"Q"
RM_TOPOLOGY: Final[bytes] = b"T"
RM_LEAVE: Final[bytes] = b"L"
ELECTION: Final[bytes] = b"V"
COORDINATOR: Final[bytes] = b"W"
RING_ACK: Final[bytes] = b"X"

# ``!`` seleciona a ordem de bytes de rede (big-endian). ``c`` é um byte de
# tipo de mensagem e ``Q`` é um inteiro unsigned de 64 bits. Assim, REQUEST
# ocupa 1 + 8 + 8 = 17 bytes e ACK ocupa 1 + 8 + 8 + 8 = 25 bytes.
_REQUEST = struct.Struct("!cQQ")
_ACK = struct.Struct("!cQQQ")


REPLICATE: Final[bytes] = b"M"
REPLICA_ACK: Final[bytes] = b"C"

# A operação replicada inclui a identidade da requisição original. Além de
# atualizar o acumulador, isto permite que um RM promovido a FE restaure a
# deduplicação stop-and-wait dos clientes após uma falha do líder.
_REPLICATE = struct.Struct("!cQQ4sQ")
_REPLICA_ACK = struct.Struct("!cQQQ")
_RM_JOIN_ACK = struct.Struct("!cQQQ")
_RM_SNAPSHOT_ACK = struct.Struct("!c")


@dataclass(frozen=True)
class ReplicateOp:
    """Operação de soma ordenada enviada pelo Front-End aos Replica Managers."""

    seq_id: int
    value: int
    client_ip: str
    request_id: int


@dataclass(frozen=True)
class ReplicaAck:
    """Resposta de um Replica Manager ao Front-End após aplicar a soma."""

    seq_id: int
    num_reqs: int
    total_sum: int


def _pack_ip(ip: str, field: str) -> bytes:
    try:
        return socket.inet_aton(ip)
    except OSError as exc:
        raise ProtocolError(f"o campo {field} não é um IPv4 válido") from exc


def _unpack_ip(raw: bytes) -> str:
    return socket.inet_ntoa(raw)


def pack_replicate(seq_id: int, value: int, client_ip: str, request_id: int) -> bytes:
    """Serializa uma operação de soma com seu número de sequência global."""
    
    _uint64(seq_id, "seq_id")
    _uint64(value, "value")
    _uint64(request_id, "request_id")
    return _REPLICATE.pack(
        REPLICATE,
        seq_id,
        value,
        _pack_ip(client_ip, "client_ip"),
        request_id,
    )


def unpack_replicate(data: bytes) -> ReplicateOp:
    """Valida e decodifica um datagrama de replicação recebido pela réplica."""
    
    if len(data) != _REPLICATE.size or data[:1] != REPLICATE:
        raise ProtocolError("datagrama de replicação inválido")
    _, seq_id, value, client_ip, request_id = _REPLICATE.unpack(data)
    return ReplicateOp(seq_id, value, _unpack_ip(client_ip), request_id)


def pack_replica_ack(seq_id: int, num_reqs: int, total_sum: int) -> bytes:
    """Serializa o resultado local calculado por uma réplica para o Front-End."""
    
    _uint64(seq_id, "seq_id")
    _uint64(num_reqs, "num_reqs")
    _uint64(total_sum, "total_sum")
    return _REPLICA_ACK.pack(REPLICA_ACK, seq_id, num_reqs, total_sum)


def unpack_replica_ack(data: bytes) -> ReplicaAck:
    """Valida e decodifica uma confirmação de réplica recebida pelo Front-End."""
    
    if len(data) != _REPLICA_ACK.size or data[:1] != REPLICA_ACK:
        raise ProtocolError("datagrama de confirmação de réplica inválido")
    _, seq_id, num_reqs, total_sum = _REPLICA_ACK.unpack(data)
    return ReplicaAck(seq_id, num_reqs, total_sum)


@dataclass(frozen=True)
class ReplicaSnapshot:
    """Estado consolidado enviado pelo Front-End ao Replica Manager."""

    num_reqs: int
    total_sum: int
    next_seq: int
    client_states: tuple[tuple[str, int, int, int], ...]


_RM_SNAPSHOT = struct.Struct("!cQQQI")
_RM_SNAPSHOT_CLIENT = struct.Struct("!4sQQQ")


def pack_rm_snapshot(
    num_reqs: int,
    total_sum: int,
    next_seq: int,
    client_states: Mapping[str, tuple[int, int, int]] | None = None,
) -> bytes:
    """Serializa estado de um FE para a entrada de um RM.

    Além do acumulador, o snapshot transporta o último ACK de cada cliente.
    Dessa forma, qualquer RM que venha a vencer uma eleição conhece o último
    ``id_req`` de cada cliente e não reaplica uma retransmissão antiga.
    """
    _uint64(num_reqs, "num_reqs")
    _uint64(total_sum, "total_sum")
    _uint64(next_seq, "next_seq")
    states = client_states or {}
    if len(states) > 0xFFFFFFFF:
        raise ProtocolError("há clientes demais no snapshot")
    packet = bytearray(
        _RM_SNAPSHOT.pack(RM_SNAPSHOT, num_reqs, total_sum, next_seq, len(states))
    )
    for client_ip, (last_req, last_num_reqs, last_total_sum) in sorted(states.items()):
        _uint64(last_req, "last_req")
        _uint64(last_num_reqs, "last_num_reqs")
        _uint64(last_total_sum, "last_total_sum")
        packet.extend(
            _RM_SNAPSHOT_CLIENT.pack(
                _pack_ip(client_ip, "client_ip"),
                last_req,
                last_num_reqs,
                last_total_sum,
            )
        )
    if len(packet) > 65507:
        raise ProtocolError("snapshot excede o limite de um datagrama UDP")
    return bytes(packet)


def unpack_rm_snapshot(data: bytes) -> ReplicaSnapshot:
    if len(data) < _RM_SNAPSHOT.size or data[:1] != RM_SNAPSHOT:
        raise ProtocolError("snapshot de replica invalido")
    _, num_reqs, total_sum, next_seq, client_count = _RM_SNAPSHOT.unpack_from(data)
    expected_size = _RM_SNAPSHOT.size + client_count * _RM_SNAPSHOT_CLIENT.size
    if len(data) != expected_size:
        raise ProtocolError("tamanho de snapshot de replica invalido")
    states = []
    offset = _RM_SNAPSHOT.size
    for _ in range(client_count):
        raw_ip, last_req, last_num_reqs, last_total_sum = _RM_SNAPSHOT_CLIENT.unpack_from(
            data, offset
        )
        states.append((_unpack_ip(raw_ip), last_req, last_num_reqs, last_total_sum))
        offset += _RM_SNAPSHOT_CLIENT.size
    return ReplicaSnapshot(num_reqs, total_sum, next_seq, tuple(states))


_RING_MESSAGE = struct.Struct("!cQ4s4s")
_RING_ACK = struct.Struct("!cQ4s")


@dataclass(frozen=True)
class RingMessage:
    """Token de eleição ou de proclamação percorrendo o anel."""

    kind: bytes
    election_id: int
    initiator_ip: str
    candidate_ip: str


@dataclass(frozen=True)
class RingAck:
    """Confirmação de entrega ponto-a-ponto entre dois sucessores do anel."""

    election_id: int
    sender_ip: str


def pack_ring_message(
    kind: bytes,
    election_id: int,
    initiator_ip: str,
    candidate_ip: str,
) -> bytes:
    if kind not in (ELECTION, COORDINATOR):
        raise ProtocolError("tipo de mensagem do anel inválido")
    _uint64(election_id, "election_id")
    return _RING_MESSAGE.pack(
        kind,
        election_id,
        _pack_ip(initiator_ip, "initiator_ip"),
        _pack_ip(candidate_ip, "candidate_ip"),
    )


def unpack_ring_message(data: bytes) -> RingMessage:
    if len(data) != _RING_MESSAGE.size or data[:1] not in (ELECTION, COORDINATOR):
        raise ProtocolError("mensagem do anel inválida")
    kind, election_id, initiator_ip, candidate_ip = _RING_MESSAGE.unpack(data)
    return RingMessage(kind, election_id, _unpack_ip(initiator_ip), _unpack_ip(candidate_ip))


def pack_ring_ack(election_id: int, sender_ip: str) -> bytes:
    _uint64(election_id, "election_id")
    return _RING_ACK.pack(RING_ACK, election_id, _pack_ip(sender_ip, "sender_ip"))


def unpack_ring_ack(data: bytes) -> RingAck:
    if len(data) != _RING_ACK.size or data[:1] != RING_ACK:
        raise ProtocolError("confirmação do anel inválida")
    _, election_id, sender_ip = _RING_ACK.unpack(data)
    return RingAck(election_id, _unpack_ip(sender_ip))



class ProtocolError(ValueError):
    """Mensagem malformada ou valor que não cabe em um uint64."""


@dataclass(frozen=True)
class Request:
    """Requisição decodificada, pronta para o servidor processar."""

    request_id: int
    value: int


@dataclass(frozen=True)
class Acknowledgement:
    """Confirmação decodificada, enviada pelo servidor ao cliente."""

    request_id: int
    num_reqs: int
    total_sum: int


def _uint64(value: int, field: str) -> None:
    """Valida campos antes de delegar a serialização ao ``struct``.

    A validação explícita produz uma mensagem de erro controlada e também
    documenta que o acumulador do trabalho tem limite de 64 bits.
    """
    if not 0 <= value <= MAX_UINT64:
        raise ProtocolError(f"o campo {field} está fora do intervalo uint64")


def pack_discover() -> bytes:
    """Monta a mensagem broadcast enviada pelo cliente na descoberta."""
    return DISCOVER


def pack_discover_ack() -> bytes:
    """Monta a resposta unicast do servidor à descoberta."""
    return DISCOVER_ACK


def pack_exit() -> bytes:
    """Monta o aviso de saída enviado no encerramento do cliente."""
    return EXIT


def pack_request(request_id: int, value: int) -> bytes:
    """Serializa uma requisição com seu identificador e valor positivo."""
    _uint64(request_id, "request_id")
    _uint64(value, "value")
    return _REQUEST.pack(REQUEST, request_id, value)


def unpack_request(data: bytes) -> Request:
    """Valida e decodifica um datagrama de requisição recebido pelo servidor."""
    if len(data) != _REQUEST.size or data[:1] != REQUEST:
        raise ProtocolError("datagrama de requisição inválido")
    _, request_id, value = _REQUEST.unpack(data)
    return Request(request_id, value)


def pack_ack(request_id: int, num_reqs: int, total_sum: int) -> bytes:
    """Serializa o retrato do estado global confirmado por um ACK."""
    _uint64(request_id, "request_id")
    _uint64(num_reqs, "num_reqs")
    _uint64(total_sum, "total_sum")
    return _ACK.pack(ACK, request_id, num_reqs, total_sum)


def unpack_ack(data: bytes) -> Acknowledgement:
    """Valida e decodifica um ACK antes de o cliente aceitá-lo."""
    if len(data) != _ACK.size or data[:1] != ACK:
        raise ProtocolError("datagrama de confirmação inválido")
    _, request_id, num_reqs, total_sum = _ACK.unpack(data)
    return Acknowledgement(request_id, num_reqs, total_sum)
