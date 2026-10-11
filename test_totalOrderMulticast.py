"""Deterministic tests for the distributed total-order state machine."""

from collections import defaultdict, deque
import random
import unittest

from totalOrderMulticast import TotalOrderMulticast


class SimulatedFifoNetwork:
    """Select links in arbitrary order while preserving FIFO on each link."""

    def __init__(self, peer_ids):
        """Create one directed queue for every pair of distinct peers."""
        self.peer_ids = tuple(peer_ids)
        self.engines = {}
        self.links = defaultdict(deque)

    def transport_for(self, source_id):
        """Return a minimal multicast adapter for one engine."""
        network = self

        class Adapter:
            def multicast(self, body):
                for destination_id in network.peer_ids:
                    if destination_id != source_id:
                        network.links[(source_id, destination_id)].append(body)

        return Adapter()

    def pump(self, seed):
        """Deliver all queued messages with randomized cross-link interleaving."""
        rng = random.Random(seed)
        while True:
            available = [link for link, queue in self.links.items() if queue]
            if not available:
                return
            source_id, destination_id = rng.choice(available)
            body = self.links[(source_id, destination_id)].popleft()
            self.engines[destination_id].receive_network(body)

    def deliver_next(self, source_id, destination_id):
        """Deliver the next body on one selected FIFO link."""
        body = self.links[(source_id, destination_id)].popleft()
        self.engines[destination_id].receive_network(body)


class TotalOrderMulticastTests(unittest.TestCase):
    """Validate ordering, completeness, and deterministic tie breaking."""

    def test_delivery_waits_for_every_order_ack(self):
        """A queue head remains blocked until the final peer acknowledgment."""
        peer_ids = (0, 1)
        network = SimulatedFifoNetwork(peer_ids)
        engines = {
            peer_id: TotalOrderMulticast(peer_id, peer_ids, "ack-test")
            for peer_id in peer_ids
        }
        network.engines = engines
        for peer_id, engine in engines.items():
            engine.set_transport(network.transport_for(peer_id))

        engines[0].multicast((0, 0))
        self.assertEqual(engines[0].snapshot()["delivery_log"], [])

        network.deliver_next(0, 1)  # DATA
        self.assertEqual(engines[1].snapshot()["delivery_log"], [])

        network.deliver_next(1, 0)  # ORDER_ACK from peer 1
        self.assertEqual(engines[0].snapshot()["delivery_log"], [(0, 0)])
        self.assertEqual(engines[1].snapshot()["delivery_log"], [])

        network.deliver_next(0, 1)  # ORDER_ACK from peer 0
        self.assertEqual(engines[1].snapshot()["delivery_log"], [(0, 0)])

    def test_concurrent_equal_timestamps_have_identical_delivery(self):
        """Three concurrent timestamp-one messages deliver in peer-id order."""
        peer_ids = (0, 1, 2)
        network = SimulatedFifoNetwork(peer_ids)
        engines = {
            peer_id: TotalOrderMulticast(peer_id, peer_ids, "test-run")
            for peer_id in peer_ids
        }
        network.engines = engines
        for peer_id, engine in engines.items():
            engine.set_transport(network.transport_for(peer_id))

        for peer_id, engine in engines.items():
            engine.multicast((peer_id, 0))
        network.pump(seed=17)

        expected = [(0, 0), (1, 0), (2, 0)]
        for engine in engines.values():
            snapshot = engine.snapshot()
            self.assertEqual(snapshot["delivery_log"], expected)
            self.assertEqual(snapshot["pending_queue"], [])
            self.assertEqual(snapshot["pending_ack_messages"], [])

    def test_two_messages_per_peer_are_complete_and_identical(self):
        """Concurrent FIFO streams deliver all unique identifiers identically."""
        peer_ids = (0, 1, 2)
        network = SimulatedFifoNetwork(peer_ids)
        engines = {
            peer_id: TotalOrderMulticast(peer_id, peer_ids, "test-run-2")
            for peer_id in peer_ids
        }
        network.engines = engines
        for peer_id, engine in engines.items():
            engine.set_transport(network.transport_for(peer_id))

        for sequence in range(2):
            for peer_id, engine in engines.items():
                engine.multicast((peer_id, sequence))
        network.pump(seed=31)

        deliveries = [
            engine.snapshot()["delivery_log"] for engine in engines.values()
        ]
        expected_ids = {
            (peer_id, sequence)
            for peer_id in peer_ids
            for sequence in range(2)
        }
        self.assertTrue(all(log == deliveries[0] for log in deliveries[1:]))
        self.assertEqual(len(deliveries[0]), 6)
        self.assertEqual(set(deliveries[0]), expected_ids)


if __name__ == "__main__":
    unittest.main()
