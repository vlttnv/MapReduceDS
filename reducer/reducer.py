from socket import *
import thread, requests, threading, random
import time, argparse, sys, json, os


# Set up command line arguments
parser = argparse.ArgumentParser(description='Reducer. Hearbeat to Master. Wait for work from reducers. Send words to master when done.')

parser.add_argument('remote_address', help='Destination IP address of master.')
parser.add_argument('remote_port', help='Destination PORT of master.')
parser.add_argument('local_port', help='Local PORT of mapper.')
parser.add_argument('-hB', '--heartbeat', type=int, default=1, help='Heartbeat interval')
parser.add_argument('-s', '--silent', action='store_true', default=False, help='Enable silent mode')
parser.add_argument('-pF', '--failure', type=int, default=0, help='Failure chance (1-100).')
parser.add_argument('-pC', '--crash', type=int, default=0, help='Crash chance (1-100).')
args = parser.parse_args()

# Accumulate data
words = {}
lock = threading.Lock()
heard_from = {}

def handler(cl_socket, cl_addr):
	# Collect data
	dt = ""
	job_id = 0
	while 1:
		data = cl_socket.recv(1024*4)
		dt = dt + data
		if not data:
			break
	cl_socket.close()

	# Parse data
	dt = json.loads(dt)
	job_id = dt['job_id']
	dt = dt['data']

	# Keep track which master spoke to you
	if job_id not in heard_from:
		heard_from[job_id] = set()
		heard_from[job_id].add(str(cl_addr[0]) + str(cl_addr[1]))
	else:
		heard_from[job_id].add(str(cl_addr[0]) + str(cl_addr[1]))

	# Lock
	lock.acquire()
	# and load (data)
	try:
		if job_id not in words:
			words[job_id] = {}
		for word in dt:
			if word.keys()[0] not in words[job_id]:
				words[job_id][word.keys()[0]] = word.values()[0]
			else:
				words[job_id][word.keys()[0]] += word.values()[0]
	finally:
		lock.release()

	rq = requests.get("http://" + args.remote_address +":" + args.remote_port + "/get_mappers_num")

	# Chance to crash
	crash = random.randint(1,100)
	if crash <= args.crash:
		os._exit(1) # kill -9 more or less


	if len(heard_from[job_id]) == int(rq.text):
		print "[R "+ str(args.local_port) +"] Subjob done. Sending to master."
		payload = {
				"job_id": job_id,
				"data": words[job_id]
				}
		
		# Chance to fail
		fail = random.randint(1,100)
		if fail > args.failure:
			headers = {'content-type': 'application/json'}
			try:
				rq = requests.post("http://"+ args.remote_address  +":"+ args.remote_port  +"/complete_subjob", headers=headers, data=json.dumps(payload))
			except requests.exceptions.ConnectionError:
				time.sleep(args.heartbeat)
				rq = requests.post("http://"+ args.remote_address  +":"+ args.remote_port  +"/complete_subjob", headers=headers, data=json.dumps(payload))

"""
Hearbeat to master.
Try to reconnect if you lose connection and
do not crash on Ctrl+C
"""
def heartbeat(m_addr, m_port, l_port):
	try:
		r = requests.get('http://'+ m_addr +':'+ m_port +'/register_reducer/'+ l_port)
	except requests.exceptions.ConnectionError:
		print "[R "+ str(args.local_port) +"]Master down. Attempting to reconnect..."
		time.sleep(args.heartbeat)
		heartbeat(m_addr, m_port, l_port)

	while 1:
		time.sleep(args.heartbeat)
		try:
			r = requests.get('http://'+ m_addr +':'+ m_port + '/heartbeat/1/'+ l_port)
		except requests.exceptions.ConnectionError:
			print "[R "+ str(args.local_port) +"]Master down. Attempting to reconnect..."
			heartbeat(m_addr, m_port, l_port)


if __name__ == "__main__":
	# listen on
	host = '0.0.0.0'
	port = int(args.local_port)
	buf = 1024
	
	# HB thread
	try:
		thread.start_new_thread(heartbeat, (args.remote_address, args.remote_port, args.local_port))
	except KeyboardInterrupt:
		try:
			rq = requests.get("http://" + args.remote_address +":" + args.remote_port + "/deregister_reducer/" + args.local_port)
		except requests.exceptions.ConnectionError:
			print "[R "+ str(args.local_port) +"]Master is dead."


		sys.exit(0)

	addr = (host, port)
	
	sv_socket = socket(AF_INET, SOCK_STREAM)
	sv_socket.bind(addr)
	sv_socket.listen(0)


	try:
		while 1:
			cl_socket, cl_addr = sv_socket.accept()
			thread.start_new_thread(handler, (cl_socket, cl_addr))
		sv_socket.close()
	except KeyboardInterrupt:
		try:
			rq = requests.get("http://" + args.remote_address +":" + args.remote_port + "/deregister_reducer/" + args.local_port)
		except requests.exceptions.ConnectionError:
			print "[R "+ str(args.local_port) +"]Master is dead."

		sys.exit(0)
	finally:
		sv_socket.close()

