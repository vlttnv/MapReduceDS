from socket import *
import thread, requests
import time, argparse


# Set up command line arguments
# TODO: writeup
parser = argparse.ArgumentParser(description='Reducer')

parser.add_argument('remote_address', help='Destination IP address of master.')
parser.add_argument('remote_port', help='Destination PORT of master.')
parser.add_argument('local_port', help='Local PORT of mapper.')
parser.add_argument('-hB', '--heartbeat', type=int, default=1, help='Heartbeat interval')
parser.add_argument('-s', '--silent', action='store_true', default=False, help='Enable silent mode')

args = parser.parse_args()

words = {}

def handler(cl_socket, cl_addr):
	dt = ""
	while 1:
		data = cl_socket.recv(1024*4)
		dt = dt + data
		# TODO: Close here
		if not data:
			break
	cl_socket.close()
	
	print dt

 
def heartbeat(m_addr, m_port, l_port):
	r = requests.get('http://'+ m_addr +':'+ m_port +'/register_reducer/'+ l_port)
	while 1:
		time.sleep(args.heartbeat)
		r = requests.get('http://'+ m_addr +':'+ m_port + '/heartbeat/1/'+ l_port)

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



