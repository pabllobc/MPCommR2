"""Lamport-clock total-order multicast engine for MPComm peers."""

import heapq
import threading
import time


class TotalOrderMulticast:
    """Order DATA messages and release them only after acknowledgment by all."""

    def __init__(self, peer_id, group_ids, run_id):
        """Initialize one peer's logical clock, holdback queue, and audit state."""
        self.peer_id = int(peer_id)
        self.group_ids = frozenset(int(pid) for pid in group_ids)
        self.run_id = run_id
        if self.peer_id not in self.group_ids:
            raise ValueError("The local peer must belong to the fixed group")

        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.transport = None
        self.clock = 0
        self.next_application_sequence = 0

        self.holdback_queue = []
        self.data_by_id = {}
        self.order_acks = {}
        self.pending_order_acks = {}
        self.acked_once = set()
        self.delivered = set()

        self.arrival_log = []
        self.delivery_log = []
        self.protocol_log = []
        self.stats = {
            "data_created": 0,
            "data_received": 0,
            "order_acks_created": 0,
            "order_acks_received": 0,
            "messages_delivered": 0,
            "duplicates_ignored": 0,
        }

    def set_transport(self, transport):
        """Attach the reliable FIFO transport used for network multicasts."""
        self.transport = transport

    def multicast(self, payload):
        """Create one DATA message, enqueue it locally, and multicast it."""
        if self.transport is None:
            raise RuntimeError("Total-order transport was not configured")

        with self.changed:
            self.clock += 1
            application_sequence = self.next_application_sequence
            self.next_application_sequence += 1
            message_id = (
                self.run_id,
                self.peer_id,
                application_sequence,
            )
            data = {
                "kind": "DATA",
                "run_id": self.run_id,
                "message_id": message_id,
                "sender_id": self.peer_id,
                "application_sequence": application_sequence,
                "lamport": self.clock,
                "payload": payload,
            }
            self.stats["data_created"] += 1
            self._record_event_locked(
                "DATA_CREATED",
                message_id=message_id,
                order_key=self._order_key(data),
            )
            order_ack = self._accept_data_locked(
                data,
                update_clock=False,
                origin="LOCAL",
            )
            self._try_deliver_locked()

        # FIFO requires DATA to enter every destination queue before its ACK.
        self.transport.multicast(data)
        if order_ack is not None:
            self.transport.multicast(order_ack)
        return message_id

    def receive_network(self, message):
        """Process one protocol message delivered by ReliableFifoUDP."""
        kind = message.get("kind")
        order_ack = None
        with self.changed:
            if message.get("run_id") != self.run_id:
                raise ValueError("Protocol message belongs to another run")
            if kind == "DATA":
                order_ack = self._accept_data_locked(
                    message,
                    update_clock=True,
                    origin="NETWORK",
                )
            elif kind == "ORDER_ACK":
                self._accept_order_ack_locked(message)
            else:
                raise ValueError(f"Unknown total-order message kind: {kind}")
            self._try_deliver_locked()
            self.changed.notify_all()

        if order_ack is not None:
            self.transport.multicast(order_ack)

    def wait_for_deliveries(self, expected, timeout):
        """Wait until exactly the expected number of application messages arrive."""
        deadline = time.monotonic() + timeout
        with self.changed:
            while len(self.delivery_log) < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "Timed out waiting for totally ordered delivery: "
                        f"{len(self.delivery_log)}/{expected}"
                    )
                self.changed.wait(min(remaining, 0.1))
            if len(self.delivery_log) != expected:
                raise AssertionError(
                    "Delivered more messages than the fixed workload defines"
                )

    def snapshot(self):
        """Return logs and counters while preserving internal encapsulation."""
        with self.lock:
            return {
                "clock": self.clock,
                "arrival_log": list(self.arrival_log),
                "delivery_log": list(self.delivery_log),
                "protocol_log": list(self.protocol_log),
                "stats": dict(self.stats),
                "pending_queue": [
                    entry[1] for entry in sorted(self.holdback_queue)
                ],
                "pending_ack_messages": sorted(self.order_acks),
            }

    def _accept_data_locked(self, data, update_clock, origin):
        """Insert one new DATA and create this peer's explicit ORDER_ACK."""
        self._validate_data_locked(data)
        if update_clock:
            self.clock = max(self.clock, int(data["lamport"])) + 1

        message_id = tuple(data["message_id"])
        if message_id in self.delivered or message_id in self.data_by_id:
            self.stats["duplicates_ignored"] += 1
            self._record_event_locked(
                "DATA_DUPLICATE",
                message_id=message_id,
                origin=origin,
            )
            return None

        normalized = dict(data)
        normalized["message_id"] = message_id
        self.data_by_id[message_id] = normalized
        heapq.heappush(
            self.holdback_queue,
            (self._order_key(normalized), message_id),
        )
        pending = self.pending_order_acks.pop(message_id, set())
        self.order_acks[message_id] = set(pending)
        display_id = (
            int(normalized["sender_id"]),
            int(normalized["application_sequence"]),
        )
        self.arrival_log.append(display_id)
        self.stats["data_received"] += 1
        self._record_event_locked(
            "DATA_ARRIVED",
            message_id=message_id,
            origin=origin,
            order_key=self._order_key(normalized),
        )

        if message_id in self.acked_once:
            return None
        self.acked_once.add(message_id)
        self.order_acks[message_id].add(self.peer_id)
        self.clock += 1
        ack = {
            "kind": "ORDER_ACK",
            "run_id": self.run_id,
            "message_id": message_id,
            "ack_sender_id": self.peer_id,
            "lamport": self.clock,
        }
        self.stats["order_acks_created"] += 1
        self._record_event_locked(
            "ORDER_ACK_CREATED",
            message_id=message_id,
            ack_sender_id=self.peer_id,
        )
        return ack

    def _accept_order_ack_locked(self, ack):
        """Associate one explicit acknowledgment with its referenced DATA."""
        message_id = tuple(ack["message_id"])
        ack_sender_id = int(ack["ack_sender_id"])
        if ack_sender_id not in self.group_ids:
            raise ValueError("ORDER_ACK sender is not in the fixed group")
        if message_id[0] != self.run_id:
            raise ValueError("ORDER_ACK references another run")

        self.clock = max(self.clock, int(ack["lamport"])) + 1
        self.stats["order_acks_received"] += 1
        if message_id in self.delivered:
            self.stats["duplicates_ignored"] += 1
            return
        if message_id in self.data_by_id:
            self.order_acks[message_id].add(ack_sender_id)
        else:
            self.pending_order_acks.setdefault(message_id, set()).add(
                ack_sender_id
            )
        self._record_event_locked(
            "ORDER_ACK_ARRIVED",
            message_id=message_id,
            ack_sender_id=ack_sender_id,
        )

    def _try_deliver_locked(self):
        """Deliver safe queue heads atomically in deterministic order."""
        while self.holdback_queue:
            _order_key, message_id = self.holdback_queue[0]
            acknowledgers = self.order_acks.get(message_id, set())
            if acknowledgers != set(self.group_ids):
                return

            heapq.heappop(self.holdback_queue)
            data = self.data_by_id.pop(message_id)
            self.order_acks.pop(message_id, None)
            self.pending_order_acks.pop(message_id, None)
            if message_id in self.delivered:
                continue

            self.delivered.add(message_id)
            display_id = (
                int(data["sender_id"]),
                int(data["application_sequence"]),
            )
            self.delivery_log.append(display_id)
            self.stats["messages_delivered"] += 1
            self._record_event_locked(
                "DATA_DELIVERED",
                message_id=message_id,
                order_key=self._order_key(data),
            )
            self.changed.notify_all()

    def _validate_data_locked(self, data):
        """Validate DATA identity and immutable fixed-group fields."""
        message_id = tuple(data["message_id"])
        sender_id = int(data["sender_id"])
        sequence = int(data["application_sequence"])
        if sender_id not in self.group_ids:
            raise ValueError("DATA sender is not in the fixed group")
        if len(message_id) != 3:
            raise ValueError("DATA message_id must contain three fields")
        if message_id != (self.run_id, sender_id, sequence):
            raise ValueError("DATA identity fields are inconsistent")

    def _order_key(self, data):
        """Return the total ordering key used identically by every peer."""
        return (
            int(data["lamport"]),
            int(data["sender_id"]),
            int(data["application_sequence"]),
        )

    def _record_event_locked(self, event, **details):
        """Append a monotonic audit event while the engine lock is held."""
        self.protocol_log.append(
            {
                "monotonic_ns": time.monotonic_ns(),
                "peer_id": self.peer_id,
                "event": event,
                "clock": self.clock,
                "details": details,
            }
        )
