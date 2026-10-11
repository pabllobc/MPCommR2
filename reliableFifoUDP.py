"""Reliable FIFO point-to-point links built over the MPComm UDP socket."""

from collections import deque
import pickle
import socket
import threading
import time

from constMP import RELIABLE_MAX_ATTEMPTS, RELIABLE_RETRY_INTERVAL


class ReliableFifoUDP:
    """Deliver protocol messages reliably and in FIFO order per directed link."""

    def __init__(
        self,
        bind_addr,
        port,
        peer_id,
        peers,
        run_id,
        on_message,
        on_event=None,
    ):
        """Initialize link state without starting worker threads."""
        self.peer_id = int(peer_id)
        self.peers = {
            int(item["peer_id"]): (item["ip"], int(item["udp_port"]))
            for item in peers
        }
        self.run_id = run_id
        self.on_message = on_message
        self.on_event = on_event

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((bind_addr, port))
        self.socket.settimeout(0.1)

        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.running = False
        self.failure = None

        destinations = [pid for pid in self.peers if pid != self.peer_id]
        self.next_sequence = {pid: 0 for pid in destinations}
        self.outboxes = {pid: deque() for pid in destinations}
        self.inflight = {pid: None for pid in destinations}
        self.expected_sequence = {
            pid: 0 for pid in self.peers if pid != self.peer_id
        }

        self.stats = {
            "link_packets_sent": 0,
            "link_acks_sent": 0,
            "retransmissions": 0,
            "duplicates_suppressed": 0,
            "protocol_messages_received": 0,
        }
        self.receiver_thread = None
        self.sender_thread = None

    def start(self):
        """Start sender and receiver workers after the UDP socket is bound."""
        with self.changed:
            if self.running:
                return
            self.running = True
        self.receiver_thread = threading.Thread(
            target=self._receiver_loop,
            name=f"reliable-receiver-{self.peer_id}",
            daemon=True,
        )
        self.sender_thread = threading.Thread(
            target=self._sender_loop,
            name=f"reliable-sender-{self.peer_id}",
            daemon=True,
        )
        self.receiver_thread.start()
        self.sender_thread.start()

    def multicast(self, body):
        """Queue one protocol body to every other peer in a stable FIFO order."""
        with self.changed:
            self._raise_if_failed_locked()
            if not self.running:
                raise RuntimeError("ReliableFifoUDP is not running")
            for destination_id in sorted(self.outboxes):
                sequence = self.next_sequence[destination_id]
                self.next_sequence[destination_id] += 1
                packet = {
                    "kind": "LINK_PACKET",
                    "run_id": self.run_id,
                    "source_id": self.peer_id,
                    "destination_id": destination_id,
                    "link_sequence": sequence,
                    "body": body,
                }
                self.outboxes[destination_id].append(packet)
            self.changed.notify_all()

    def drain(self, timeout):
        """Wait until every queued packet has a matching link acknowledgment."""
        deadline = time.monotonic() + timeout
        with self.changed:
            while True:
                self._raise_if_failed_locked()
                empty = all(not queue for queue in self.outboxes.values())
                idle = all(packet is None for packet in self.inflight.values())
                if empty and idle:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Reliable FIFO transport did not drain")
                self.changed.wait(min(remaining, 0.1))

    def raise_if_failed(self):
        """Raise a worker failure in the caller's thread."""
        with self.lock:
            self._raise_if_failed_locked()

    def snapshot_stats(self):
        """Return a consistent copy of transport counters."""
        with self.lock:
            return dict(self.stats)

    def close(self):
        """Stop workers and close the UDP socket without discarding exceptions."""
        with self.changed:
            self.running = False
            self.changed.notify_all()
        try:
            self.socket.close()
        except OSError:
            pass
        for worker in (self.sender_thread, self.receiver_thread):
            if worker is not None and worker is not threading.current_thread():
                worker.join(timeout=2)

    def _sender_loop(self):
        """Send queued packets and retransmit unacknowledged packets."""
        while True:
            to_send = []
            with self.changed:
                if not self.running:
                    return
                now = time.monotonic()
                for destination_id in sorted(self.outboxes):
                    current = self.inflight[destination_id]
                    if current is None and self.outboxes[destination_id]:
                        current = {
                            "packet": self.outboxes[destination_id].popleft(),
                            "last_sent": 0.0,
                            "attempts": 0,
                        }
                        self.inflight[destination_id] = current

                    if current is None:
                        continue
                    if now - current["last_sent"] < RELIABLE_RETRY_INTERVAL:
                        continue
                    if current["attempts"] >= RELIABLE_MAX_ATTEMPTS:
                        self._fail_locked(
                            "Reliable FIFO retry limit reached for peer "
                            f"{destination_id}, sequence "
                            f"{current['packet']['link_sequence']}"
                        )
                        return
                    current["attempts"] += 1
                    current["last_sent"] = now
                    if current["attempts"] > 1:
                        self.stats["retransmissions"] += 1
                    to_send.append(
                        (current["packet"], self.peers[destination_id])
                    )

                if not to_send:
                    self.changed.wait(RELIABLE_RETRY_INTERVAL / 2)
                    continue

            for packet, address in to_send:
                if not self._send_datagram(packet, address):
                    return
                self._event(
                    "LINK_PACKET_SENT",
                    destination=packet["destination_id"],
                    sequence=packet["link_sequence"],
                )
                with self.lock:
                    self.stats["link_packets_sent"] += 1

    def _receiver_loop(self):
        """Receive link packets and expose each protocol body exactly once."""
        while True:
            with self.lock:
                if not self.running:
                    return
            try:
                payload, _address = self.socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                with self.lock:
                    if self.running:
                        self._fail_locked("Reliable FIFO UDP socket failed")
                return

            try:
                packet = pickle.loads(payload)
                if packet.get("run_id") != self.run_id:
                    continue
                if packet.get("destination_id") != self.peer_id:
                    continue
                if packet.get("kind") == "LINK_ACK":
                    self._accept_link_ack(packet)
                elif packet.get("kind") == "LINK_PACKET":
                    self._accept_link_packet(packet)
            except Exception as exc:
                with self.changed:
                    self._fail_locked(
                        f"Reliable FIFO receiver failed: {type(exc).__name__}: {exc}"
                    )
                return

    def _accept_link_ack(self, packet):
        """Advance one outgoing FIFO after its current packet is acknowledged."""
        source_id = int(packet["source_id"])
        sequence = int(packet["ack_sequence"])
        with self.changed:
            if source_id not in self.inflight:
                return
            current = self.inflight[source_id]
            if (
                current is not None
                and current["packet"]["link_sequence"] == sequence
            ):
                self.inflight[source_id] = None
                self.changed.notify_all()
                self._event(
                    "LINK_ACK_RECEIVED",
                    source=source_id,
                    sequence=sequence,
                )

    def _accept_link_packet(self, packet):
        """Deduplicate and deliver the next packet expected from one source."""
        source_id = int(packet["source_id"])
        sequence = int(packet["link_sequence"])
        if source_id not in self.expected_sequence:
            return

        with self.lock:
            expected = self.expected_sequence[source_id]
        if sequence < expected:
            with self.lock:
                self.stats["duplicates_suppressed"] += 1
            self._send_link_ack(source_id, sequence)
            return
        if sequence > expected:
            return

        self.on_message(packet["body"])
        with self.lock:
            self.expected_sequence[source_id] += 1
            self.stats["protocol_messages_received"] += 1
        self._event(
            "LINK_PACKET_DELIVERED",
            source=source_id,
            sequence=sequence,
        )
        self._send_link_ack(source_id, sequence)

    def _send_link_ack(self, destination_id, sequence):
        """Send an idempotent acknowledgment for one accepted link packet."""
        ack = {
            "kind": "LINK_ACK",
            "run_id": self.run_id,
            "source_id": self.peer_id,
            "destination_id": destination_id,
            "ack_sequence": sequence,
        }
        if self._send_datagram(ack, self.peers[destination_id]):
            with self.lock:
                self.stats["link_acks_sent"] += 1
            self._event(
                "LINK_ACK_SENT",
                destination=destination_id,
                sequence=sequence,
            )

    def _send_datagram(self, packet, address):
        """Serialize and send one bounded UDP datagram."""
        try:
            payload = pickle.dumps(packet)
            if len(payload) > 65507:
                raise ValueError("Protocol packet exceeds the UDP payload limit")
            self.socket.sendto(payload, address)
            return True
        except Exception as exc:
            with self.changed:
                self._fail_locked(
                    f"Reliable FIFO send failed: {type(exc).__name__}: {exc}"
                )
            return False

    def _event(self, name, **details):
        """Forward an optional transport event without affecting correctness."""
        if self.on_event is not None:
            self.on_event(name, details)

    def _fail_locked(self, message):
        """Store the first worker failure and wake all waiters."""
        if self.failure is None:
            self.failure = RuntimeError(message)
        self.running = False
        self.changed.notify_all()

    def _raise_if_failed_locked(self):
        """Raise the stored worker exception while the caller holds the lock."""
        if self.failure is not None:
            raise self.failure
