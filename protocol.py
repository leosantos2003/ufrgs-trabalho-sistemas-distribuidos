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

# ``!`` seleciona a ordem de bytes de rede (big-endian). ``c`` é um byte de
# tipo de mensagem e ``Q`` é um inteiro unsigned de 64 bits. Assim, REQUEST
# ocupa 1 + 8 + 8 = 17 bytes e ACK ocupa 1 + 8 + 8 + 8 = 25 bytes.
_REQUEST = struct.Struct("!cQQ")
_ACK = struct.Struct("!cQQQ")


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
