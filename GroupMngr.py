"""In-memory fixed-membership manager for one MPComm experiment."""

from socket import *
import pickle

from constMP import *


port = GROUPMNGR_TCP_PORT
membership = []
membershipFrozen = False


def peerList():
  """Return peer IP addresses in stable registration order."""
  return [member[0] for member in membership]


def serverLoop():
  """Register peers, freeze one group snapshot, and serve control requests."""
  global membershipFrozen

  serverSock = socket(AF_INET, SOCK_STREAM)
  # Allow immediate reuse of the port after closing
  serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(max(16, N * 2))
  while True:
    (conn, addr) = serverSock.accept()
    try:
      msgPack = conn.recv(65536)
      req = pickle.loads(msgPack)
      if req["op"] == "register":
        member = (req["ipaddr"], int(req["port"]))
        if membershipFrozen:
          print('Rejected late peer registration: ', req)
        elif member not in membership:
          membership.append(member)
          print('Registered peer: ', req)
        else:
          print('Ignored duplicate peer registration: ', req)
      elif req["op"] == "list":
        peers = peerList()
        print('List of peers sent to client: ', peers)
        conn.sendall(pickle.dumps(peers))
      elif req["op"] == "freeze":
        membershipFrozen = True
        peers = peerList()
        print('Frozen peer list sent to coordinator: ', peers)
        conn.sendall(pickle.dumps(peers))
      elif req["op"] == "unregister":
        # Membership is deliberately fixed for the duration of the experiment.
        pass
      elif req["op"] == "stop":
        print("Stopping.")
        serverSock.close()
        break
    finally:
      conn.close()


serverLoop()
