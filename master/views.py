from master import master_app
from flask import request
import time
import json
import os
from socket import *

mappers = dict()
reducers = dict()

@master_app.route('/')
def index():
	return "Python rocks"

@master_app.route('/register_mapper/<int:port>')
def register_mapper(port):
	mappers[request.remote_addr+":"+str(port)] = {"stamp":int(time.time())}

	return "success", 200

@master_app.route('/register_reducer/<int:port>')
def register_reducer(port):
	reducers[request.remote_addr+":"+str(port)] = {"stamp":int(time.time())}

	return "success", 200

@master_app.route('/heartbeat/<int:type>/<int:port>')
def heardbeat(type,port):
	if type == 0:
		mappers[request.remote_addr+":"+str(port)]["stamp"] = int(time.time())
	elif type == 1:
		reducers[request.remote_addr+":"+str(port)]["stamp"] = int(time.time())
	else:
		return "Invlaid type", 500

	return "OK", 200

@master_app.route('/get_mappers')
def get_mappers():
	return json.dumps(mappers), 200

@master_app.route('/accept_file', methods=['POST'])
def accept_file():
	if request.method == 'POST':
		fl_keys = request.files.keys()
		fl = request.files[fl_keys[0]]
		fl.save(os.path.join(master_app.config['UPLOAD_FOLDER'], fl.filename))
		distribute_file("test")
	return "lel", 200

"""
HELPER FUNCTION
"""
def distribute_file(name):
	host = 'localhost'
	port = 55567
	buf = 1024
	addr = (host, port)
	clientsocket = socket(AF_INET, SOCK_STREAM)
	clientsocket.connect(addr)
	data = "Hello"
	clientsocket.send(data)
	data = clientsocket.recv(buf)
	print data
	clientsocket.close()
