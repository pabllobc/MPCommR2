"""Coordinate and validate finite total-order MPComm experiments."""

from socket import *
import pickle
import uuid

from constMP import *


serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(max(16, N * 2))


def recvAll(conn):
  """Read one complete TCP payload until the sender closes its write side."""
  chunks = []
  while True:
    data = conn.recv(65536)
    if not data:
      break
    chunks.append(data)
  return b''.join(chunks)


def groupRequest(operation):
  """Send one operation to GroupMngr and optionally decode its response."""
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
  clientSock.sendall(pickle.dumps({"op": operation}))
  if operation in ("list", "freeze"):
    response = clientSock.recv(65536)
    result = pickle.loads(response)
  else:
    result = None
  clientSock.close()
  return result


def peerRequest(peer, command):
  """Send one framed-by-close command and return the peer response."""
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((peer, PEER_TCP_PORT))
  clientSock.sendall(pickle.dumps(command))
  clientSock.shutdown(SHUT_WR)
  response = pickle.loads(recvAll(clientSock))
  clientSock.close()
  if str(response).startswith('ERROR'):
    raise RuntimeError(response)
  print(response)
  return response


def fixedPeers(peerList):
  """Build the common fixed-group snapshot passed to every peer."""
  return [
    {
      "peer_id": peerId,
      "ip": peer,
      "udp_port": PEER_UDP_PORT,
    }
    for peerId, peer in enumerate(peerList)
  ]


def prepareAndStart(peerList, nMsgs, runId):
  """Prepare every UDP receiver before releasing any application sender."""
  peers = fixedPeers(peerList)
  for peer in peers:
    command = {
      "op": "prepare",
      "run_id": runId,
      "peer_id": peer["peer_id"],
      "n_msgs": nMsgs,
      "peers": peers,
    }
    peerRequest(peer["ip"], command)
  for peer in peers:
    peerRequest(
      peer["ip"],
      {
        "op": "go",
        "run_id": runId,
      },
    )
  return peers


def stopRound(peers, runId):
  """Tell every peer to close UDP only after all reports were collected."""
  for peer in peers:
    peerRequest(
      peer["ip"],
      {
        "op": "round_stop",
        "run_id": runId,
      },
    )


def expectedMessageIds(nPeers, nMsgs):
  """Build every (sender, sequence) expected in arrival and delivery logs."""
  return {(peerId, seq) for peerId in range(nPeers) for seq in range(nMsgs)}


def analyzeLog(log, nPeers, nMsgs):
  """Validate one complete sequence for length, duplicates, and identities."""
  ids = [tuple(item) for item in log]
  unique = set(ids)
  expected = expectedMessageIds(nPeers, nMsgs)
  return {
    'length': len(ids),
    'expected_length': nPeers * nMsgs,
    'duplicates': len(ids) - len(unique),
    'missing': sorted(expected - unique),
    'unexpected': sorted(unique - expected),
  }


def waitForReportsAndCompare(nMsgs, runId):
  """Collect N reports and validate arrival sets and ordered deliveries."""
  reports = []
  while len(reports) < N:
    conn, _addr = serverSock.accept()
    try:
      report = pickle.loads(recvAll(conn))
      if report["run_id"] != runId:
        raise ValueError("Received a report from another run")
      reports.append(report)
      print('Received report from peer', report["peer_id"])
    finally:
      conn.close()

  reports.sort(key=lambda report: int(report["peer_id"]))
  deliveryLogs = []
  arrivalLogs = []
  valid = True
  for report in reports:
    peerId = int(report["peer_id"])
    if report["error"] is not None:
      print('Peer', peerId, 'reported error:', report["error"])
      valid = False

    arrival = report["arrival_log"]
    delivery = report["delivery_log"]
    arrivalReport = analyzeLog(arrival, N, nMsgs)
    deliveryReport = analyzeLog(delivery, N, nMsgs)
    arrivalLogs.append(arrival)
    deliveryLogs.append(delivery)
    print(
      'Peer', peerId,
      'arrival length', arrivalReport['length'],
      'duplicates', arrivalReport['duplicates'],
      'missing', arrivalReport['missing'],
      'unexpected', arrivalReport['unexpected'],
    )
    print(
      'Peer', peerId,
      'delivery length', deliveryReport['length'],
      'duplicates', deliveryReport['duplicates'],
      'missing', deliveryReport['missing'],
      'unexpected', deliveryReport['unexpected'],
    )
    if (
      arrivalReport['length'] != arrivalReport['expected_length']
      or arrivalReport['duplicates'] != 0
      or arrivalReport['missing']
      or arrivalReport['unexpected']
      or deliveryReport['length'] != deliveryReport['expected_length']
      or deliveryReport['duplicates'] != 0
      or deliveryReport['missing']
      or deliveryReport['unexpected']
    ):
      valid = False

  arrivalsIdentical = all(
    log == arrivalLogs[0] for log in arrivalLogs[1:]
  )
  deliveriesIdentical = all(
    log == deliveryLogs[0] for log in deliveryLogs[1:]
  )
  print('Arrival sequences identical:', arrivalsIdentical)
  print('Delivery sequences identical:', deliveriesIdentical)
  if deliveryLogs:
    print('Totally ordered delivery:', deliveryLogs[0])
  if not valid or not deliveriesIdentical:
    raise AssertionError("Total-order validation failed")
  print(
    'Total-order audit verified:',
    N,
    'peers and',
    N * nMsgs,
    'delivered messages per peer.',
  )
  return reports


def runRound(nMsgs):
  """Freeze membership, execute one run, compare it, and drain every peer."""
  peerList = groupRequest("freeze")
  if len(peerList) != N:
    raise AssertionError(
      'Expected {} registered peers, found {}'.format(N, len(peerList))
    )
  runId = uuid.uuid4().hex
  print('Frozen peers:', peerList)
  print('Run ID:', runId)
  peers = prepareAndStart(peerList, nMsgs, runId)
  try:
    waitForReportsAndCompare(nMsgs, runId)
  finally:
    stopRound(peers, runId)


def stopExperiment():
  """Stop every registered peer and then stop GroupMngr."""
  peerList = groupRequest("list")
  for peer in peerList:
    peerRequest(peer, {"op": "stop"})
  serverSock.close()
  groupRequest("stop")
  print('Stopping.')


def mainLoop():
  """Prompt for finite workloads until zero requests a clean shutdown."""
  while True:
    nMsgs = int(input(
      'Enter the number of messages for each peer to send '
      '(0 to terminate)=> '
    ))
    if nMsgs == 0:
      stopExperiment()
      return
    runRound(nMsgs)


mainLoop()
