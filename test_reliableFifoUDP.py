"""Fault-injection tests for MPComm's reliable FIFO UDP links."""

import pickle
import socket
import time
import unittest

import reliableFifoUDP as reliable_module
from reliableFifoUDP import ReliableFifoUDP


def free_udp_port():
    """Reserve and release one local UDP port for an isolated test."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_until(predicate, timeout=2):
    """Wait for an asynchronous condition and fail with a finite deadline."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise TimeoutError("Asynchronous transport condition was not reached")


class DropFirstPacketTransport(ReliableFifoUDP):
    """Drop one DATA/ORDER_ACK envelope before delegating later sends."""

    def __init__(self, *args, **kwargs):
        """Initialize one-shot packet loss state."""
        super().__init__(*args, **kwargs)
        self.dropped_packet = False

    def _send_datagram(self, packet, address):
        """Pretend the first LINK_PACKET was sent, forcing retransmission."""
        if packet["kind"] == "LINK_PACKET" and not self.dropped_packet:
            self.dropped_packet = True
            return True
        return super()._send_datagram(packet, address)


class DuplicateFirstPacketTransport(ReliableFifoUDP):
    """Duplicate the first protocol envelope on the UDP wire."""

    def __init__(self, *args, **kwargs):
        """Initialize one-shot duplication state."""
        super().__init__(*args, **kwargs)
        self.duplicated_packet = False

    def _send_datagram(self, packet, address):
        """Send the first LINK_PACKET twice and all other packets once."""
        if packet["kind"] == "LINK_PACKET" and not self.duplicated_packet:
            self.duplicated_packet = True
            first = super()._send_datagram(packet, address)
            second = super()._send_datagram(packet, address)
            return first and second
        return super()._send_datagram(packet, address)


class DropFirstAckTransport(ReliableFifoUDP):
    """Drop the first LINK_ACK to force duplicate packet reception."""

    def __init__(self, *args, **kwargs):
        """Initialize one-shot acknowledgment loss state."""
        super().__init__(*args, **kwargs)
        self.dropped_ack = False

    def _send_datagram(self, packet, address):
        """Pretend the first LINK_ACK was sent and allow its replacement."""
        if packet["kind"] == "LINK_ACK" and not self.dropped_ack:
            self.dropped_ack = True
            return True
        return super()._send_datagram(packet, address)


class DropAllPacketsTransport(ReliableFifoUDP):
    """Model a permanent partition for outgoing protocol envelopes."""

    def _send_datagram(self, packet, address):
        """Drop every LINK_PACKET while allowing unrelated datagrams."""
        if packet["kind"] == "LINK_PACKET":
            return True
        return super()._send_datagram(packet, address)


class ReliableFifoUDPTests(unittest.TestCase):
    """Validate recovery from transient faults and explicit permanent failure."""

    @classmethod
    def setUpClass(cls):
        """Use short deterministic retry limits during fault tests."""
        cls.original_interval = reliable_module.RELIABLE_RETRY_INTERVAL
        cls.original_attempts = reliable_module.RELIABLE_MAX_ATTEMPTS
        reliable_module.RELIABLE_RETRY_INTERVAL = 0.02
        reliable_module.RELIABLE_MAX_ATTEMPTS = 4

    @classmethod
    def tearDownClass(cls):
        """Restore production retry configuration."""
        reliable_module.RELIABLE_RETRY_INTERVAL = cls.original_interval
        reliable_module.RELIABLE_MAX_ATTEMPTS = cls.original_attempts

    def setUp(self):
        """Allocate fresh ports and transport tracking for one test."""
        self.port0 = free_udp_port()
        self.port1 = free_udp_port()
        while self.port1 == self.port0:
            self.port1 = free_udp_port()
        self.peers = [
            {"peer_id": 0, "ip": "127.0.0.1", "udp_port": self.port0},
            {"peer_id": 1, "ip": "127.0.0.1", "udp_port": self.port1},
        ]
        self.transports = []

    def tearDown(self):
        """Close every worker and socket created by the current test."""
        for transport in reversed(self.transports):
            transport.close()

    def make_pair(
        self,
        source_class=ReliableFifoUDP,
        destination_class=ReliableFifoUDP,
    ):
        """Start two transports and return source, destination, and deliveries."""
        source_received = []
        destination_received = []
        source = source_class(
            "127.0.0.1",
            self.port0,
            0,
            self.peers,
            "transport-test",
            source_received.append,
        )
        destination = destination_class(
            "127.0.0.1",
            self.port1,
            1,
            self.peers,
            "transport-test",
            destination_received.append,
        )
        self.transports.extend((source, destination))
        source.start()
        destination.start()
        return source, destination, destination_received

    def test_transient_packet_loss_is_recovered(self):
        """A dropped LINK_PACKET is retransmitted and delivered once."""
        source, _destination, received = self.make_pair(
            source_class=DropFirstPacketTransport
        )
        source.multicast({"value": "transient-loss"})
        wait_until(lambda: len(received) == 1)
        source.drain(2)

        self.assertEqual(received, [{"value": "transient-loss"}])
        self.assertGreaterEqual(
            source.snapshot_stats()["retransmissions"],
            1,
        )

    def test_wire_duplicate_is_suppressed(self):
        """A duplicated LINK_PACKET reaches the protocol callback once."""
        source, destination, received = self.make_pair(
            source_class=DuplicateFirstPacketTransport
        )
        source.multicast({"value": "duplicate"})
        wait_until(
            lambda: destination.snapshot_stats()["duplicates_suppressed"] >= 1
        )
        source.drain(2)

        self.assertEqual(received, [{"value": "duplicate"}])
        self.assertEqual(
            destination.snapshot_stats()["protocol_messages_received"],
            1,
        )

    def test_lost_link_ack_triggers_safe_duplicate_reception(self):
        """A lost LINK_ACK causes retry without a second protocol delivery."""
        source, destination, received = self.make_pair(
            destination_class=DropFirstAckTransport
        )
        source.multicast({"value": "lost-ack"})
        wait_until(
            lambda: destination.snapshot_stats()["duplicates_suppressed"] >= 1
        )
        source.drain(2)

        self.assertEqual(received, [{"value": "lost-ack"}])
        self.assertGreaterEqual(
            source.snapshot_stats()["retransmissions"],
            1,
        )

    def test_reordered_future_packet_waits_for_missing_sequence(self):
        """A future packet is ignored until its missing predecessor arrives."""
        received = []
        destination = ReliableFifoUDP(
            "127.0.0.1",
            self.port1,
            1,
            self.peers,
            "reorder-test",
            lambda body: received.append(body["sequence"]),
        )
        self.transports.append(destination)
        destination.start()

        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender.bind(("127.0.0.1", self.port0))
        try:
            def packet(sequence):
                return {
                    "kind": "LINK_PACKET",
                    "run_id": "reorder-test",
                    "source_id": 0,
                    "destination_id": 1,
                    "link_sequence": sequence,
                    "body": {"sequence": sequence},
                }

            sender.sendto(
                pickle.dumps(packet(1)),
                ("127.0.0.1", self.port1),
            )
            time.sleep(0.05)
            self.assertEqual(received, [])

            sender.sendto(
                pickle.dumps(packet(0)),
                ("127.0.0.1", self.port1),
            )
            wait_until(lambda: received == [0])

            sender.sendto(
                pickle.dumps(packet(1)),
                ("127.0.0.1", self.port1),
            )
            wait_until(lambda: received == [0, 1])
        finally:
            sender.close()

    def test_permanent_loss_fails_instead_of_claiming_progress(self):
        """A permanent partition reaches retry failure with no delivery."""
        source, _destination, received = self.make_pair(
            source_class=DropAllPacketsTransport
        )
        source.multicast({"value": "permanent-loss"})

        with self.assertRaisesRegex(RuntimeError, "retry limit reached"):
            source.drain(2)
        self.assertEqual(received, [])


if __name__ == "__main__":
    unittest.main()
