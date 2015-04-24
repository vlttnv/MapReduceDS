from socket import *
import thread, requests, hashlib
import time, argparse, json, sys, random, os

# Set up command line arguments
parser = argparse.ArgumentParser(description='Mapper. Get data from Master, work on it and sends it to Reducers.')

parser.add_argument('remote_address', help='Destination IP address of master.')
parser.add_argument('remote_port', help='Destination PORT of master.')
parser.add_argument('local_port', help='Local PORT of mapper.')
parser.add_argument('-hB', '--heartbeat', type=int, default=1, help='Heartbeat interval')
parser.add_argument('-s', '--silent', action='store_true', default=False, help='Enable silent mode')
parser.add_argument('-pF', '--failure', type=int, default=0, help='Failure chance (1-100).')
parser.add_argument('-pC', '--crash', type=int, default=0, help='Crash chance (1-100).')
args = parser.parse_args()

"""
Receives the file. Writes it to a buffer.
Decodes the message and creates the buckets for the reducers.
Sends to the reducers.
"""
def handler(cl_socket, cl_addr):
	print "[M "+ str(args.local_port) +"] Accepted connection from: ", cl_addr

	# Receive and save data
	data = ""
	while 1:
		dt = cl_socket.recv(1024*4)
		data = data + dt
		if not dt:
			break
	cl_socket.close()
	
	# Parse data
	data2 = json.loads(data)
	reducers = data2['reducers']
	job_id = data2['job_id']
	sent_list = " ".join(data2['data'])
	words =  sent_list.split(" ")
	
	# Count words
	word_count = {}
	for word in words:
		import re
		word = re.sub(r'[^a-zA-Z]', "", word)
		word = word.lower()
		if word in word_count:
			if word != "":
				word_count[word.lower()] += 1
		else:
			if word != "":
				word_count[word.lower()] = 1

	# Prepare to shuffle
	shuffled = {}
	r_id = 0
	for r in reducers:
		shuffled[str(r_id)] = []
		r_id += 1
	
	# Shuffle 
	reducers = reducers.keys()
	for word in word_count:
		which = int(int(hashlib.sha1(word).hexdigest(), 16) % len(reducers))
		shuffled[str(which)].append({word:word_count[word]})

	# DIstribute buckets to reducers
	for s in shuffled:
		host = reducers[int(s)].split(":")[0]
		port = int(reducers[int(s)].split(":")[1])
		buf = 1024
		addr = (host, port)

		to_reducer = socket(AF_INET, SOCK_STREAM)
		
		# Roll to crash
		crash = random.randint(1,100)
		if crash <= args.crash:
			os._exit(1) # kill -9 more or less

		# Roll to fail
		# If not then send
		fail = random.randint(1,100)
		if fail > args.failure:
			to_reducer.connect(addr)
			data_to_send = {}
			data_to_send['job_id'] = job_id
			data_to_send['data'] = shuffled[s]
			to_reducer.send(json.dumps(data_to_send))
			to_reducer.close()
			r = requests.get('http://'+ args.remote_address +':'+ args.remote_port +'/schedule_subjob/'+str(job_id))
		else:
			print "[M "+ str(l_port) +"] Failed to send message."
 
"""
Hearbeat to master.
Try to reconnect if you lose connection and
do not crash on Ctrl+C
"""
def heartbeat(m_addr, m_port, l_port):
	try:
		r = requests.get('http://'+ m_addr +':'+ m_port +'/register_mapper/'+ l_port)
	except requests.exceptions.ConnectionError:
		print "[M "+ str(l_port) +"] Master down. Attempting to reconnect..."
		time.sleep(args.heartbeat) # Be nice, don't spam
		heartbeat(m_addr, m_port, l_port)

	# HB forever
	while 1:
		time.sleep(args.heartbeat)
		try:
			r = requests.get('http://'+ m_addr +':'+ m_port + '/heartbeat/0/'+ l_port)
		except requests.exceptions.ConnectionError:
			print "[M " + str(l_port) +"] Master down. Attempting to reconnect..."
			heartbeat(m_addr, m_port, l_port)


if __name__ == "__main__":
	# listen on
	host = '0.0.0.0'
	port = int(args.local_port)
	buf = 1024
	
	# Make a thread to heartbeat
	try:
		thread.start_new_thread(heartbeat, (args.remote_address, args.remote_port, args.local_port))
	except KeyboardInterrupt:
		try:
			rq = requests.get("http://" + args.remote_address +":" + args.remote_port + "/deregister_mapper/" + args.local_port)
		except requests.exceptions.ConnectionError:
			print "[M "+ str(l_port) +"] Master is dead."
		exit(0)

	addr = (host, port)
	
	sv_socket = socket(AF_INET, SOCK_STREAM)
	sv_socket.bind(addr)
	sv_socket.listen(0)
	
	# Liste for work
	try:
		while 1:
			cl_socket, cl_addr = sv_socket.accept()
			thread.start_new_thread(handler, (cl_socket, cl_addr))
		sv_socket.close()
	except KeyboardInterrupt:
		try:
			rq = requests.get("http://" + args.remote_address +":" + args.remote_port + "/deregister_mapper/" + args.local_port)
		except requests.exceptions.ConnectionError:
			print "[M "+ str(l_port) +"] Master is dead."
		sys.exit(0)



