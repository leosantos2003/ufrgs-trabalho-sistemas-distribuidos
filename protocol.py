"""Formato binário das mensagens do serviço de soma sobre UDP.

O protocolo deliberadamente usa datagramas pequenos e de tamanho fixo nas
mensagens de processamento. Isto evita custo de serialização desnecessário
quando arquivos grandes são usados como entrada dos clientes.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Final

MAX_UINT64: Final[int] = (1 << 64) - 1

DISCOVER: Final[bytes] = b"D"
DISCOVER_ACK: Final[bytes] = b"S"
REQUEST: Final[bytes] = b"R"
ACK: Final[bytes] = b"A"
EXIT: Final[bytes] = b"E"

_REQUEST = struct.Struct("!cQQ")
_ACK = struct.Struct("!cQQQ")


class ProtocolError(ValueError):
    """Mensagem malformada ou valor que não cabe em um uint64."""


@dataclass(frozen=True)
class Request:
    request_id: int
    value: int


@dataclass(frozen=True)
class Acknowledgement:
    request_id: int
    num_reqs: int
    total_sum: int


def _uint64(value: int, field: str) -> None:
    if not 0 <= value <= MAX_UINT64:
        raise ProtocolError(f"o campo {field} está fora do intervalo uint64")


def pack_discover() -> bytes:
    return DISCOVER


def pack_discover_ack() -> bytes:
    return DISCOVER_ACK


def pack_exit() -> bytes:
    return EXIT


def pack_request(request_id: int, value: int) -> bytes:
    _uint64(request_id, "request_id")
    _uint64(value, "value")
    return _REQUEST.pack(REQUEST, request_id, value)


def unpack_request(data: bytes) -> Request:
    if len(data) != _REQUEST.size or data[:1] != REQUEST:
        raise ProtocolError("datagrama de requisição inválido")
    _, request_id, value = _REQUEST.unpack(data)
    return Request(request_id, value)


def pack_ack(request_id: int, num_reqs: int, total_sum: int) -> bytes:
    _uint64(request_id, "request_id")
    _uint64(num_reqs, "num_reqs")
    _uint64(total_sum, "total_sum")
    return _ACK.pack(ACK, request_id, num_reqs, total_sum)


def unpack_ack(data: bytes) -> Acknowledgement:
    if len(data) != _ACK.size or data[:1] != ACK:
        raise ProtocolError("datagrama de confirmação inválido")
    _, request_id, num_reqs, total_sum = _ACK.unpack(data)
    return Acknowledgement(request_id, num_reqs, total_sum)
