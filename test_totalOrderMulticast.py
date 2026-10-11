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

    def build_network(self, peer_ids, run_id):
        """Create connected engines and their FIFO link simulation."""
        network = SimulatedFifoNetwork(peer_ids)
        engines = {
            peer_id: TotalOrderMulticast(peer_id, peer_ids, run_id)
            for peer_id in peer_ids
        }
        network.engines = engines
        for peer_id, engine in engines.items():
            engine.set_transport(network.transport_for(peer_id))
        return network, engines

    def test_delivery_waits_for_every_order_ack(self):
        """A queue head remains blocked until the final peer acknowledgment."""
        peer_ids = (0, 1)
        network, engines = self.build_network(peer_ids, "ack-test")

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
        network, engines = self.build_network(peer_ids, "test-run")

        for peer_id, engine in engines.items():
            engine.multicast((peer_id, 0))
        network.pump(seed=17)

        expected = [(0, 0), (1, 0), (2, 0)]
        for engine in engines.values():
            snapshot = engine.snapshot()
            self.assertEqual(snapshot["delivery_log"], expected)
            self.assertEqual(snapshot["pending_queue"], [])
            self.assertEqual(snapshot["pending_ack_messages"], [])

    def test_delayed_lower_message_blocks_a_higher_queue_head(self):
        """FIFO exposes a delayed lower DATA before its later ORDER_ACK."""
        peer_ids = (0, 1, 2)
        network, engines = self.build_network(peer_ids, "delay-test")

        engines[0].multicast((0, 0))
        engines[1].multicast((1, 0))

        # Peer 2 sees sender 1 first. It must not deliver this higher tie key.
        network.deliver_next(1, 2)
        self.assertEqual(engines[2].snapshot()["arrival_log"], [(1, 0)])
        self.assertEqual(engines[2].snapshot()["delivery_log"], [])

        # Peer 0 receives sender 1 and creates ORDER_ACK(1,0), but FIFO puts
        # peer 0's lower DATA(0,0) ahead of that ACK on link 0 -> 2.
        network.deliver_next(1, 0)
        network.deliver_next(0, 2)
        self.assertEqual(
            engines[2].snapshot()["arrival_log"],
            [(1, 0), (0, 0)],
        )
        self.assertEqual(engines[2].snapshot()["delivery_log"], [])

        network.pump(seed=73)
        expected = [(0, 0), (1, 0)]
        for engine in engines.values():
            self.assertEqual(engine.snapshot()["delivery_log"], expected)

    def test_duplicate_data_is_not_delivered_or_acknowledged_twice(self):
        """A duplicated protocol DATA produces one arrival, ACK, and delivery."""
        peer_ids = (0, 1)
        network, engines = self.build_network(peer_ids, "duplicate-test")

        engines[0].multicast((0, 0))
        duplicate = network.links[(0, 1)][0]
        network.deliver_next(0, 1)
        engines[1].receive_network(duplicate)
        network.pump(seed=11)

        snapshot = engines[1].snapshot()
        self.assertEqual(snapshot["arrival_log"], [(0, 0)])
        self.assertEqual(snapshot["delivery_log"], [(0, 0)])
        self.assertEqual(snapshot["stats"]["order_acks_created"], 1)
        self.assertEqual(snapshot["stats"]["duplicates_ignored"], 1)

    def test_two_messages_per_peer_are_complete_and_identical(self):
        """Concurrent FIFO streams deliver all unique identifiers identically."""
        peer_ids = (0, 1, 2)
        network, engines = self.build_network(peer_ids, "test-run-2")

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
