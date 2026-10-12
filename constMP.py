"""Read shared MPComm settings from one deployment environment."""

import os


PEER_UDP_PORT = 6789
PEER_TCP_PORT = 5679
N = int(os.environ.get('MPCOMM_N', '3'))
SERVER_ADDR = os.environ.get('MPCOMM_SERVER_ADDR', '127.0.0.1')
SERVER_PORT = 5678
GROUPMNGR_ADDR = os.environ.get(
    'MPCOMM_GROUPMNGR_ADDR',
    SERVER_ADDR,
)
GROUPMNGR_TCP_PORT = 5680

# Reliable FIFO link and finite-round limits.
RELIABLE_RETRY_INTERVAL = 0.1
RELIABLE_MAX_ATTEMPTS = 100
ROUND_TIMEOUT = 60

