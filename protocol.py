"""Formato binário das mensagens do serviço de soma sobre UDP.

O protocolo deliberadamente usa datagramas pequenos e de tamanho fixo nas
mensagens de processamento. Isto evita custo de serialização desnecessário
quando arquivos grandes são usados como entrada dos clientes.

Os programas não trocam objetos Python nem texto JSON. Eles trocam somente
bytes, definidos aqui de forma centralizada para garantir que cliente e
servidor concordem sobre os tipos, a ordem dos campos e os tamanhos.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Final

# Limite máximo representável pelos campos ``Q`` do módulo ``struct``.
# Python aceita inteiros maiores, mas o protocolo precisa obedecer ao requisito
# de valores unsigned de 64 bits.
MAX_UINT64: Final[int] = (1 << 64) - 1

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

# ``!`` seleciona a ordem de bytes de rede (big-endian). ``c`` é um byte de
# tipo de mensagem e ``Q`` é um inteiro unsigned de 64 bits. Assim, REQUEST
# ocupa 1 + 8 + 8 = 17 bytes e ACK ocupa 1 + 8 + 8 + 8 = 25 bytes.
_REQUEST = struct.Struct("!cQQ")
_ACK = struct.Struct("!cQQQ")


REPLICATE: Final[bytes] = b"M"
REPLICA_ACK: Final[bytes] = b"C"

_REPLICATE = struct.Struct("!cQQ")
_REPLICA_ACK = struct.Struct("!cQQQ")
_RM_JOIN_ACK = struct.Struct("!cQQQ")
_RM_SNAPSHOT_ACK = struct.Struct("!c")


@dataclass(frozen=True)
class ReplicateOp:
    """Operação de soma ordenada enviada pelo Front-End aos Replica Managers."""

    seq_id: int
    value: int


@dataclass(frozen=True)
class ReplicaAck:
    """Resposta de um Replica Manager ao Front-End após aplicar a soma."""

    seq_id: int
    num_reqs: int
    total_sum: int


def pack_replicate(seq_id: int, value: int) -> bytes:
    """Serializa uma operação de soma com seu número de sequência global."""
    
    _uint64(seq_id, "seq_id")
    _uint64(value, "value")
    return _REPLICATE.pack(REPLICATE, seq_id, value)


def unpack_replicate(data: bytes) -> ReplicateOp:
    """Valida e decodifica um datagrama de replicação recebido pela réplica."""
    
    if len(data) != _REPLICATE.size or data[:1] != REPLICATE:
        raise ProtocolError("datagrama de replicação inválido")
    _, seq_id, value = _REPLICATE.unpack(data)
    return ReplicateOp(seq_id, value)


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


_RM_SNAPSHOT = struct.Struct("!cQQQ")


def pack_rm_snapshot(num_reqs: int, total_sum: int, next_seq: int) -> bytes:
    _uint64(num_reqs, "num_reqs")
    _uint64(total_sum, "total_sum")
    _uint64(next_seq, "next_seq")
    return _RM_SNAPSHOT.pack(RM_SNAPSHOT, num_reqs, total_sum, next_seq)


def unpack_rm_snapshot(data: bytes) -> ReplicaSnapshot:
    if len(data) != _RM_SNAPSHOT.size or data[:1] != RM_SNAPSHOT:
        raise ProtocolError("snapshot de replica invalido")
    _, num_reqs, total_sum, next_seq = _RM_SNAPSHOT.unpack(data)
    return ReplicaSnapshot(num_reqs, total_sum, next_seq)



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
