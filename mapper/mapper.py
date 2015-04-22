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
	while 1:
		data = cl_socket.recv(1024*4)
		# TODO: Close here
		if not data:
			break
		else:
			msg = "You sent me: %s" % "ok"

			data = json.loads(data)
			reducers = data['reducers']
			print reducers
			words =  data['data'].split(" ")
			word_count = {}
			for word in words:
				if word in word_count:
					if word != "":
						word_count[word] += 1
				else:
					if word != "":
						word_count[word] = 1

			for word in word_count:
				which = int(hashlib.sha1(word).hexdigest(), 16) % len(reducers)

				host = reducers[which].split(":")[0]
				port = int(reducers[which].split(":")[1])
				buf = 1024
				addr = (host, port)

				to_reducer = socket(AF_INET, SOCK_STREAM)
				to_reducer.connect(addr)
				to_reducer.send(json.dumps({word:word_count[word]}))
				to_reducer.close()
			"""
			# get address
			host = mp.split(":")[0]
			port = int(mp.split(":")[1])
			buf = 1024
			addr = (host, port)
			# create socket
			to_reducer = socket(AF_INET, SOCK_STREAM)
			to_reducer.connect(addr)
		
			# send file
			data = "HEHE"

			if not data:
				break
			to_reducer.send(data)

			data = to_reducer.recv(buf)
			to_reducer.close()
			"""
			# TODO: split into dict and count
			# import hashlib
			# int*hashlib.sha1(a).hexdigest(), 16) % num of red
			cl_socket.send(msg)
	cl_socket.close()

 
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



