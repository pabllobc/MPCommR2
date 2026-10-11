# MPComm - Multiparty Communication
Very simple demo of multicast communication without coordination. The intent is to demonstrate how things can go wrong as we scale a distributed system (in this case, scaling in the geographical dimension) without taking appropriate measures to coordinate the behavior of its components. Later, this example will serve as the basis to implement coordination protocols, notably for message ordering, as part of course assignments.

## Overall structure
A set of **peer processes** is established and each process multicasts a sequence of messages to all the others at random intervals. Messages are stamped with the ID of the sending process and a local sequence number defined by the sending process. This is a simple attempt to demonstrate the problem of message ordering (or, indeed, the lack of message ordering).

The peer processes run the *peerCommunicatorUDP.py* program, which has two separate threads, one for sending and the other for receiving messages. After starting, a peer processes waits for a signal (from a process that coordinates the experiment) in order to actually start multicasting the sequence of messages. Each peer collects all the received messages (in the order that they were received) and sends the list of messages to the coordinator process, which will compare the order of messages as received by all the peers (as described next).

A separate process runs *comparisonServer.py*, which, besides coordinating the start of the peer processes (by sending the start signal mentioned above), waits for the list of received messages from each peer and compares their order. It reports a metric that indicates the occurrence of out-of-order messages (actually, the number of message exchange rounds in which at least one process received a different message form the others). Importantly, we assume that no messages are lost, and, in an ideal scenario, all processes should receive all the same messages in the same order. Note, however, that the current version does not use a reliable message communication protocol to ensure that messages are not lost (this can be an improvement to be implemented as an assignment).

A second server process, running *GroupMngr.py*, is used for peers to register themselves (notably their IP addresses) as well as to discover the other peers.

In order to actually see the problem of message ordering, it is necessary to introduce variability on message communication latency, such as by running the peer processes on different networks, e.g., running some of the peers in one region of the cloud, whereas the others are run on another region. This illustrates the issue of geographic scalability, when the system works fine when all its components are in the same network, but starts to behave erroneously when its components are spread across the Internet.

## Setup
- Create one instance (virtual machine in the cloud) to run the two servers, and a number of instances (e.g., 6 in one cloud region, 2 on another) to run the peers.
- Edit *constMP.py* with the correct IP address of the instance where the two servers will run. Tip: allocate a fixed IP address (elastic IP in AWS jargon) to the server instance. Also edit the file to configure the number of peers processes in the experiment.
- On the server instance, run *GroupMngr.py*
- On some of the peer instances: run *peerCommunicator.py*
- Back on the server instance: run *comparisonServer.py*

## Total-order implementation

The P-A6 implementation keeps the original baseline files and adds a separate
ordered execution path:

- `reliableFifoUDP.py`: stop-and-wait reliability and FIFO per directed UDP link.
- `totalOrderMulticast.py`: Lamport clock, holdback queue, explicit acknowledgments,
  deterministic ordering, and application delivery.
- `peerCommunicatorOrdered.py`: peer control, finite workload, and separate logs.
- `comparisonServerOrdered.py`: fixed-group orchestration and complete validation.
- `run_ordered_local.py`: reproducible multi-peer execution on one Windows host.
- `test_totalOrderMulticast.py`: deterministic state-machine tests.
- `test_reliableFifoUDP.py`: link fault-injection and recovery tests.

The coordinator freezes membership and assigns stable peer IDs before it sends
`GO`. It does not choose the order. Every peer orders DATA by:

```text
(lamport, sender_id, application_sequence)
```

A DATA message is delivered to the application only when it is at the head of
the holdback queue and has an `ORDER_ACK` from every peer. `LINK_ACK` belongs
only to the reliable UDP layer and must not be confused with `ORDER_ACK`.

### Environment

The validated local environment is:

```text
CPython 3.13.14
requests 2.34.2
```

No new package is required.

For a local three-peer run, `constMP.py` uses:

```python
N = 3
SERVER_ADDR = '127.0.0.1'
GROUPMNGR_ADDR = '127.0.0.1'
```

### Automated local execution

From the MPComm directory:

```powershell
python -B -m unittest -v `
  test_totalOrderMulticast.py test_reliableFifoUDP.py
python -B run_ordered_local.py 5 `
  "c:\temp\mpcomm-ordered" `
  127.0.0.1 127.0.0.2 127.0.0.3
```

The deterministic suite covers equal Lamport timestamps, delayed lower DATA,
delayed `ORDER_ACK`, duplicate protocol DATA, transient packet and link-ACK
loss, wire duplication, per-link reordering, and permanent-loss failure.

The first argument is `K`, the number of messages sent by each peer. With
`N = 3` and `K = 5`, every peer must deliver `X = N × K = 15` unique messages.

The runner starts, in order:

1. `GroupMngr.py`;
2. three instances of `peerCommunicatorOrdered.py`;
3. `comparisonServerOrdered.py`.

The coordinator sends `K` and then `0`, so the experiment terminates without
manual input. Success requires zero exit status from the coordinator, all peers,
and GroupMngr.

### Evidence files

Each peer writes:

```text
arrival-peer-<id>.log
delivery-peer-<id>.log
protocol-peer-<id>.log
```

Arrival order is the order in which each DATA enters the protocol. Delivery
order is the only order exposed to the application. Arrival sequences may
differ; all delivery sequences must be identical.

The coordinator validates all peers and all positions:

- length is exactly `N × K`;
- no duplicate identifier exists;
- no identifier is missing or unexpected;
- all delivery lists are identical;
- all peer and group processes terminate after the reliable queues drain.

### Manual multi-machine execution

Set `N`, `SERVER_ADDR`, and `GROUPMNGR_ADDR` identically on all machines. Allow:

- TCP 5680 to GroupMngr;
- TCP 5678 to the ordered comparison server;
- TCP 5679 to every peer;
- UDP 6789 between every pair of peers.

Start `GroupMngr.py`, then `peerCommunicatorOrdered.py` on every peer machine,
and finally `comparisonServerOrdered.py` on the coordinator. Do not enter `K`
until all registrations are visible in GroupMngr.

This version assumes a fixed, crash-free group for a successful round. Transient
UDP loss, duplication, reordering, and delay are handled. A crashed peer or
permanent partition causes an explicit timeout instead of partial success.

## Mapping to Figure 5.10

- `requestToEnter` corresponds to `TotalOrderMulticast.multicast`.
- `ackToEnter` corresponds to creation and multicast of `ORDER_ACK`.
- `allowedToEnter` corresponds to `_try_deliver_locked`.
- `receive` corresponds to `receive_network`, `_accept_data_locked`, and
  `_accept_order_ack_locked`.
- `release` corresponds to removing the safely delivered head and its ACK state.
- `cleanupQ` corresponds to heap ordering and cleanup of per-message state.

The figure provides the Lamport coordination mechanism. Its mutual-exclusion
messages are not copied literally into the message-delivery protocol.
