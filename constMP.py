"""Shared configuration for local and multi-region MPComm executions."""

PEER_UDP_PORT = 6789
PEER_TCP_PORT = 5679
N = 3   # Number of peers in the fixed group
SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 5678
GROUPMNGR_ADDR = '127.0.0.1'
GROUPMNGR_TCP_PORT = 5680

# Reliable FIFO link and finite-round limits.
RELIABLE_RETRY_INTERVAL = 0.1
RELIABLE_MAX_ATTEMPTS = 100
ROUND_TIMEOUT = 60

