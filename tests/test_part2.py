"""Validações de protocolo e failover da Parte 2 em loopback."""

from __future__ import annotations

import socket
import threading
import time
import unittest

from cliente import SumClient
from protocol import DISCOVER, pack_ack, pack_request, unpack_ack
from servidor import FrontEnd, Replica, SumServer, discover_fe


class Part2IntegrationTest(unittest.TestCase):
    """Exercita entrada de RMs, replicação, eleição e continuidade do cliente."""

    def setUp(self) -> None:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.bind(("127.0.0.1", 0))
        self.port = probe.getsockname()[1]
        probe.close()
        self.created: list[object] = []
        self.promoted: list[FrontEnd] = []

    def _run_server(self, server: object) -> None:
        current = server
        while True:
            replacement = current.serve_forever()
            if replacement is None:
                return
            self.promoted.append(replacement)
            self.created.append(replacement)
            current = replacement

    @staticmethod
    def _close_server(server: object) -> None:
        stop_event = getattr(server, "stop_event", None)
        if stop_event is not None:
            stop_event.set()
        for attribute in (
            "socket",
            "discovery_socket",
            "heartbeat_socket",
            "multicast_socket",
        ):
            sock = getattr(server, attribute, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    def tearDown(self) -> None:
        for server in self.created:
            self._close_server(server)

    def test_failover_keeps_client_sequence_and_total(self) -> None:
        front_end = FrontEnd(self.port, "127.0.0.1")
        rm_low = Replica(self.port, "127.0.0.1", "127.0.0.2")
        rm_high = Replica(self.port, "127.0.0.1", "127.0.0.3")
        self.created.extend((front_end, rm_low, rm_high))
        for server in (front_end, rm_low, rm_high):
            threading.Thread(target=self._run_server, args=(server,), daemon=True).start()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and len(front_end.rm_ips) != 2:
            time.sleep(0.02)
        self.assertEqual(front_end.rm_ips, {"127.0.0.2", "127.0.0.3"})

        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.bind(("127.0.0.4", 0))
        client.settimeout(2.0)
        try:
            client.sendto(DISCOVER, ("127.0.0.1", self.port))
            self.assertEqual(client.recvfrom(64)[0], b"S")
            client.sendto(pack_request(1, 7), ("127.0.0.1", self.port))
            first_ack = unpack_ack(client.recvfrom(64)[0])
            self.assertEqual((first_ack.request_id, first_ack.total_sum), (1, 7))

            # Simula a queda abrupta: 127.0.0.3 deve vencer por ser o maior IP.
            self._close_server(front_end)
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline and rm_high.replacement is None:
                time.sleep(0.02)
            self.assertIsNotNone(rm_high.replacement)
            new_front_end = rm_high.replacement
            assert new_front_end is not None
            if new_front_end not in self.created:
                self.created.append(new_front_end)
            self.assertEqual(new_front_end.own_ip, "127.0.0.3")

            # A promoção publica o novo FE antes de o thread do servidor
            # iniciar seu novo laço de recepção. Aguarda essa troca de papel
            # antes de enviar a próxima descoberta do cliente.
            # Podem coexistir duas rodadas de eleição iniciadas no mesmo
            # instante. Aguarda a estabilização completa antes de falar com o
            # FE promovido.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and new_front_end not in self.promoted:
                time.sleep(0.01)
            self.assertIn(new_front_end, self.promoted)

            client.sendto(DISCOVER, ("127.0.0.3", self.port))
            self.assertEqual(client.recvfrom(64)[0], b"S")
            client.sendto(pack_request(2, 5), ("127.0.0.3", self.port))
            second_ack = unpack_ack(client.recvfrom(64)[0])
            self.assertEqual(
                (second_ack.request_id, second_ack.num_reqs, second_ack.total_sum),
                (2, 2, 12),
            )
        finally:
            client.close()

    def test_lone_promoted_frontend_accepts_new_request(self) -> None:
        """O único RM sobrevivente continua o serviço após virar FE."""
        front_end = FrontEnd(self.port, "127.0.0.1")
        rm = Replica(self.port, "127.0.0.1", "127.0.0.2")
        self.created.extend((front_end, rm))
        for server in (front_end, rm):
            threading.Thread(target=self._run_server, args=(server,), daemon=True).start()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and front_end.rm_ips != {"127.0.0.2"}:
            time.sleep(0.02)
        self.assertEqual(front_end.rm_ips, {"127.0.0.2"})

        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client.bind(("127.0.0.4", 0))
        client.settimeout(2.0)
        try:
            client.sendto(DISCOVER, ("127.0.0.1", self.port))
            self.assertEqual(client.recvfrom(64)[0], b"S")
            client.sendto(pack_request(1, 7), ("127.0.0.1", self.port))
            first_ack = unpack_ack(client.recvfrom(64)[0])
            self.assertEqual((first_ack.request_id, first_ack.total_sum), (1, 7))

            self._close_server(front_end)
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline and rm.replacement is None:
                time.sleep(0.02)
            self.assertIsNotNone(rm.replacement)
            new_front_end = rm.replacement
            assert new_front_end is not None
            if new_front_end not in self.created:
                self.created.append(new_front_end)

            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and new_front_end not in self.promoted:
                time.sleep(0.01)
            self.assertIn(new_front_end, self.promoted)

            client.sendto(DISCOVER, ("127.0.0.2", self.port))
            self.assertEqual(client.recvfrom(64)[0], b"S")
            client.sendto(pack_request(2, 5), ("127.0.0.2", self.port))
            second_ack = unpack_ack(client.recvfrom(64)[0])
            self.assertEqual(
                (second_ack.request_id, second_ack.num_reqs, second_ack.total_sum),
                (2, 2, 12),
            )
        finally:
            client.close()

    def test_server_discovery_reaches_frontend(self) -> None:
        """O socket do FE recebe a descoberta por broadcast."""
        own_ip = SumServer.get_local_ip()
        front_end = FrontEnd(self.port, own_ip)
        self.created.append(front_end)
        threading.Thread(target=self._run_server, args=(front_end,), daemon=True).start()
        time.sleep(0.05)
        self.assertEqual(discover_fe(self.port), own_ip)

    def test_ring_skips_a_failed_successor(self) -> None:
        """O nó .2 precisa pular .3 e ainda eleger o maior IP (.4)."""
        front_end = FrontEnd(self.port, "127.0.0.1")
        rm_low = Replica(self.port, "127.0.0.1", "127.0.0.2")
        rm_failed = Replica(self.port, "127.0.0.1", "127.0.0.3")
        rm_high = Replica(self.port, "127.0.0.1", "127.0.0.4")
        self.created.extend((front_end, rm_low, rm_failed, rm_high))
        for server in (front_end, rm_low, rm_failed, rm_high):
            threading.Thread(target=self._run_server, args=(server,), daemon=True).start()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and len(front_end.rm_ips) != 3:
            time.sleep(0.02)
        self.assertEqual(
            front_end.rm_ips,
            {"127.0.0.2", "127.0.0.3", "127.0.0.4"},
        )

        # O RM .3 é o sucessor imediato de .2 no anel, mas cai antes do FE.
        self._close_server(rm_failed)
        self._close_server(front_end)
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline and rm_high.replacement is None:
            time.sleep(0.02)
        self.assertIsNotNone(rm_high.replacement)
        new_front_end = rm_high.replacement
        assert new_front_end is not None
        if new_front_end not in self.created:
            self.created.append(new_front_end)
        self.assertEqual(new_front_end.own_ip, "127.0.0.4")


class ClientUdpResetTest(unittest.TestCase):
    """Garante que resets UDP do Windows não interrompam o failover."""

    class _Console:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def log(self, message: str) -> None:
            self.messages.append(message)

    class _ResetThenAckSocket:
        def __init__(self, server: tuple[str, int], acknowledgement: bytes) -> None:
            self.server = server
            self.acknowledgement = acknowledgement
            self.sent: list[tuple[bytes, tuple[str, int]]] = []
            self.receives = 0

        def sendto(self, data: bytes, address: tuple[str, int]) -> None:
            self.sent.append((data, address))

        def settimeout(self, _timeout: float) -> None:
            pass

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            self.receives += 1
            if self.receives == 1:
                # Mesmo erro emitido pelo Winsock após receber ICMP de uma
                # porta UDP fechada no FE que acabou de cair.
                raise ConnectionResetError(10054, "Connection reset by peer")
            return self.acknowledgement, self.server

    def test_udp_reset_is_retried_like_a_timeout(self) -> None:
        server = ("192.0.2.10", 4000)
        console = self._Console()
        fake_socket = self._ResetThenAckSocket(server, pack_ack(4, 4, 19))
        # O método testado não depende do construtor; criar a instância desta
        # forma deixa a prova unitária independente de sockets reais.
        client = SumClient.__new__(SumClient)
        client.console = console
        client.socket = fake_socket
        client.server = server

        self.assertTrue(client._send_until_acknowledged(4, 6))
        self.assertEqual(len(fake_socket.sent), 2)
        self.assertTrue(any("TIMEOUT id_req 4 value 6" in line for line in console.messages))
        self.assertTrue(any("RESEND id_req 4 value 6" in line for line in console.messages))


if __name__ == "__main__":
    unittest.main()
