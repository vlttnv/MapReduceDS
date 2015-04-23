from socket import *
import thread, requests, hashlib
import time, argparse, json




# Set up command line arguments
# TODO: writeup
parser = argparse.ArgumentParser(description='Mapper')

parser.add_argument('remote_address', help='Destination IP address of master.')
parser.add_argument('remote_port', help='Destination PORT of master.')
parser.add_argument('local_port', help='Local PORT of mapper.')
parser.add_argument('-hB', '--heartbeat', type=int, default=1, help='Heartbeat interval')
parser.add_argument('-s', '--silent', action='store_true', default=False, help='Enable silent mode')

args = parser.parse_args()
 
def handler(cl_socket, cl_addr):
	print "Accepted connection from: ", cl_addr
	data = ""
	while 1:
		dt = cl_socket.recv(1024*4)
		data = data + dt


		# TODO: Close here
		if not dt:
			break
	cl_socket.close()
	
	data2 = json.loads(data)
	reducers = data2['reducers']
	#data_data = " ".join(data2['data'])
	job_id = data2['job_id']
	sent_list = " ".join(data2['data'])
	words =  sent_list.split(" ")

	word_count = {}
	for word in words:
		word = word.strip(".\n")
		word = word.lower()
		if word in word_count:
			if word != "":
				word_count[word.lower()] += 1
		else:
			if word != "":
				word_count[word.lower()] = 1

	shuffled = {}
	r_id = 0
	for r in reducers:
		shuffled[str(r_id)] = []
		r_id += 1
	
	reducers = reducers.keys()
	for word in word_count:
		which = int(int(hashlib.sha1(word).hexdigest(), 16) % len(reducers))
		shuffled[str(which)].append({word:word_count[word]})


	for s in shuffled:
		host = reducers[int(s)].split(":")[0]
		port = int(reducers[int(s)].split(":")[1])
		buf = 1024
		addr = (host, port)

		to_reducer = socket(AF_INET, SOCK_STREAM)
		to_reducer.connect(addr)
		data_to_send = {}
		data_to_send['job_id'] = job_id
		data_to_send['data'] = shuffled[s]
		to_reducer.send(json.dumps(data_to_send))
		to_reducer.close()
		r = requests.get('http://'+ args.remote_address +':'+ args.remote_port +'/schedule_subjob/'+str(job_id))

	
			# TODO: split into dict and count
	# import hashlib
	# int*hashlib.sha1(a).hexdigest(), 16) % num of red

 
def heartbeat(m_addr, m_port, l_port):
	r = requests.get('http://'+ m_addr +':'+ m_port +'/register_mapper/'+ l_port)
	while 1:
		time.sleep(args.heartbeat)
		r = requests.get('http://'+ m_addr +':'+ m_port + '/heartbeat/0/'+ l_port)

if __name__ == "__main__":
	# listen on
	host = '0.0.0.0'
	port = int(args.local_port)
	buf = 1024
	

	thread.start_new_thread(heartbeat, (args.remote_address, args.remote_port, args.local_port))

	addr = (host, port)
	
	sv_socket = socket(AF_INET, SOCK_STREAM)
	sv_socket.bind(addr)
	sv_socket.listen(0)
	
	while 1:
		cl_socket, cl_addr = sv_socket.accept()
		thread.start_new_thread(handler, (cl_socket, cl_addr))
	sv_socket.close()



