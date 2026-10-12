"""MPComm peer integrated with reliable FIFO and total-order multicast."""

from socket import *
import os
import pickle
import random
import threading
import time
import traceback

from requests import get

from constMP import *
from reliableFifoUDP import ReliableFifoUDP
from totalOrderMulticast import TotalOrderMulticast


BIND_ADDR = os.environ.get('MPCOMM_BIND_ADDR', '0.0.0.0')
ADVERTISE_ADDR = os.environ.get('MPCOMM_ADVERTISE_ADDR')


def recvAll(conn):
  """Read one complete pickled TCP request until the client half-closes."""
  chunks = []
  while True:
    data = conn.recv(65536)
    if not data:
      break
    chunks.append(data)
  return b''.join(chunks)


def getPublicIp():
  """Return the explicit advertised address or discover a public address."""
  if ADVERTISE_ADDR:
    ipAddr = ADVERTISE_ADDR
  elif BIND_ADDR != '0.0.0.0':
    ipAddr = BIND_ADDR
  else:
    ipAddr = get('https://api.ipify.org', timeout=5).content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr


def registerWithGroupManager():
  """Register this peer before the coordinator freezes group membership."""
  clientSock = socket(AF_INET, SOCK_STREAM)
  print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
  req = {
    "op": "register",
    "ipaddr": getPublicIp(),
    "port": PEER_UDP_PORT,
  }
  print('Registering with group manager: ', req)
  clientSock.sendall(pickle.dumps(req))
  clientSock.close()


class OrderedRound:
  """Own all transport and total-order state for one finite experiment."""

  def __init__(self, config):
    """Bind UDP and initialize workers before acknowledging PREPARE."""
    self.runId = config["run_id"]
    self.peerId = int(config["peer_id"])
    self.nMsgs = int(config["n_msgs"])
    self.peers = list(config["peers"])
    self.expected = len(self.peers) * self.nMsgs
    self.completed = False
    self.transportEvents = []
    self.transportEventLock = threading.Lock()

    groupIds = [int(peer["peer_id"]) for peer in self.peers]
    self.engine = TotalOrderMulticast(self.peerId, groupIds, self.runId)
    self.transport = ReliableFifoUDP(
      BIND_ADDR,
      PEER_UDP_PORT,
      self.peerId,
      self.peers,
      self.runId,
      self.engine.receive_network,
      self.recordTransportEvent,
    )
    self.engine.set_transport(self.transport)
    self.transport.start()

  def recordTransportEvent(self, event, details):
    """Append a thread-safe transport audit event."""
    record = {
      "monotonic_ns": time.monotonic_ns(),
      "event": event,
      "details": details,
    }
    with self.transportEventLock:
      self.transportEvents.append(record)

  def execute(self):
    """Multicast the finite workload, drain it, and send evidence."""
    error = None
    snapshot = None
    try:
      for msgNumber in range(self.nMsgs):
        time.sleep(random.randrange(10, 100) / 1000)
        messageId = self.engine.multicast((self.peerId, msgNumber))
        print('Ordered DATA created: ', messageId)

      self.engine.wait_for_deliveries(self.expected, ROUND_TIMEOUT)
      self.transport.raise_if_failed()
      self.transport.drain(ROUND_TIMEOUT)
      snapshot = self.engine.snapshot()
      if snapshot["pending_queue"]:
        raise AssertionError("Holdback queue was not drained")
      if snapshot["pending_ack_messages"]:
        raise AssertionError("Acknowledgment state was not drained")
      if len(snapshot["delivery_log"]) != self.expected:
        raise AssertionError("Delivery log has an unexpected length")
      if len(set(snapshot["delivery_log"])) != self.expected:
        raise AssertionError("Delivery log contains duplicate identifiers")
      self.writeLogs(snapshot)
      print(
        'Round delivered ',
        len(snapshot["delivery_log"]),
        ' messages in total order.',
      )
    except Exception as exc:
      error = '{}: {}'.format(type(exc).__name__, exc)
      traceback.print_exc()
      if snapshot is None:
        snapshot = self.engine.snapshot()
      self.writeLogs(snapshot)
    finally:
      self.completed = True
      self.sendReport(snapshot, error)

  def writeLogs(self, snapshot):
    """Persist network arrival, application delivery, and protocol evidence."""
    files = {
      'arrival-peer-{}.log'.format(self.peerId): snapshot["arrival_log"],
      'delivery-peer-{}.log'.format(self.peerId): snapshot["delivery_log"],
      'protocol-peer-{}.log'.format(self.peerId): {
        "engine": snapshot["protocol_log"],
        "transport": list(self.transportEvents),
      },
    }
    for path, content in files.items():
      with open(path, 'w', encoding='utf-8') as logFile:
        logFile.write(repr(content))

  def sendReport(self, snapshot, error):
    """Send both ordered and unordered observations to the coordinator."""
    report = {
      "peer_id": self.peerId,
      "run_id": self.runId,
      "arrival_log": snapshot["arrival_log"],
      "delivery_log": snapshot["delivery_log"],
      "engine_stats": snapshot["stats"],
      "transport_stats": self.transport.snapshot_stats(),
      "error": error,
    }
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    clientSock.sendall(pickle.dumps(report))
    clientSock.close()

  def shutdown(self):
    """Close the round only after the coordinator collected every report."""
    try:
      if self.completed:
        self.transport.drain(ROUND_TIMEOUT)
    finally:
      self.transport.close()


def sendResponse(conn, response):
  """Send one pickled control response."""
  conn.sendall(pickle.dumps(response))


def controlLoop():
  """Prepare, run, drain, and stop ordered rounds under coordinator control."""
  activeRound = None
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  serverSock.bind((BIND_ADDR, PEER_TCP_PORT))
  serverSock.listen(4)

  while True:
    print('Waiting for signal to start...')
    conn, _addr = serverSock.accept()
    executeRound = None
    stopProcess = False
    try:
      command = pickle.loads(recvAll(conn))
      operation = command["op"]
      if operation == "prepare":
        if activeRound is not None:
          raise RuntimeError("A previous ordered round is still active")
        activeRound = OrderedRound(command)
        sendResponse(
          conn,
          'Peer process {} prepared.'.format(activeRound.peerId),
        )
      elif operation == "go":
        if activeRound is None or activeRound.runId != command["run_id"]:
          raise RuntimeError("GO does not match the prepared round")
        sendResponse(
          conn,
          'Peer process {} started.'.format(activeRound.peerId),
        )
        executeRound = activeRound
      elif operation == "round_stop":
        if activeRound is None or activeRound.runId != command["run_id"]:
          raise RuntimeError("ROUND_STOP does not match the active round")
        activeRound.shutdown()
        sendResponse(
          conn,
          'Peer process {} drained.'.format(activeRound.peerId),
        )
        activeRound = None
      elif operation == "stop":
        if activeRound is not None:
          activeRound.shutdown()
          activeRound = None
        sendResponse(conn, 'Peer process stopping.')
        stopProcess = True
      else:
        raise ValueError('Unknown peer control operation: ' + operation)
    except Exception as exc:
      sendResponse(
        conn,
        'ERROR {}: {}'.format(type(exc).__name__, exc),
      )
      traceback.print_exc()
    finally:
      conn.close()

    if executeRound is not None:
      executeRound.execute()
    if stopProcess:
      serverSock.close()
      print('Terminating.')
      return


registerWithGroupManager()
controlLoop()
