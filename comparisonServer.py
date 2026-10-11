from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
# Allow immediate reuse of the port after closing
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def recvAll(conn):
	"""Read the complete TCP payload until the peer closes the connection."""
	chunks = []
	while True:
		data = conn.recv(65536)
		if not data:
			break
		chunks.append(data)
	return b''.join(chunks)

def expectedMessageIds(nPeers, nMsgs):
	"""Build the identifier set (sender, sequence) expected in every complete log."""
	return {(peerId, seq) for peerId in range(nPeers) for seq in range(nMsgs)}

def analyzeLog(log, nPeers, nMsgs):
	"""Validate length, duplicates and identifiers of one received log."""
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

def compareAllPositions(logs):
	"""Compare every peer and every position, including length mismatches."""
	unordered = 0
	divergent = []
	maxLen = max((len(log) for log in logs), default=0)
	for index in range(maxLen):
		column = []
		for log in logs:
			column.append(log[index] if index < len(log) else None)
		if any(item != column[0] for item in column[1:]):
			unordered += 1
			divergent.append(index)
	return unordered, divergent

def mainLoop():
	cont = 1
	while 1:
		nMsgs = promptUser()
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
		req = {"op":"list"}
		msg = pickle.dumps(req)
		clientSock.send(msg)
		msg = clientSock.recv(2048)
		clientSock.close()
		peerList = pickle.loads(msg)
		print("List of Peers: ", peerList)
		startPeers(peerList,nMsgs)
		if nMsgs != 0:
			print('Now, wait for the message logs from the communicating peers...')
			waitForLogsAndCompare(nMsgs)
		else:
			print('Stopping.')
			serverSock.close()
			
			# Tell group manager to stop
			cSock = socket(AF_INET, SOCK_STREAM)
			cSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
			req = {"op":"stop"}
			msg = pickle.dumps(req)
			cSock.send(msg)
			cSock.close()
			break

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	peerNumber = 0
	for peer in peerList:
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((peer, PEER_TCP_PORT))
		msg = (peerNumber,nMsgs)
		msgPack = pickle.dumps(msg)
		clientSock.send(msgPack)
		msgPack = clientSock.recv(512)
		print(pickle.loads(msgPack))
		clientSock.close()
		peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
	"""Collect every peer log completely and compare all peers and positions."""
	# Loop to wait for the message logs for comparison:
	numPeers = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Receive the logs of messages from the peer processes
	while numPeers < N:
		(conn, addr) = serverSock.accept()
		msgPack = recvAll(conn)
		print ('Received log from peer')
		conn.close()
		msgs.append(pickle.loads(msgPack))
		numPeers = numPeers + 1

	for index, log in enumerate(msgs):
		report = analyzeLog(log, N, N_MSGS)
		print('Log', index, 'length', report['length'], 'expected', report['expected_length'],
			'duplicates', report['duplicates'], 'missing', report['missing'],
			'unexpected', report['unexpected'])

	identical = all(log == msgs[0] for log in msgs[1:]) if msgs else True
	unordered, divergent = compareAllPositions(msgs)
	print('Sequences identical:', identical)
	print('Divergent positions:', divergent)
	print ('Found ' + str(unordered) + ' unordered message rounds')


# Initiate server:
mainLoop()
