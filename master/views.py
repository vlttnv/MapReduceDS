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

@master_app.route('/get_reducers')
def get_reducers():
	return json.dumps(reducers), 200

@master_app.route('/accept_file', methods=['POST'])
def accept_file():
	if request.method == 'POST':
		fl_keys = request.files.keys()
		fl = request.files[fl_keys[0]]
		fl.save(os.path.join(master_app.config['UPLOAD_FOLDER'], fl.filename))
		distribute_file(fl.filename)
	return "lel", 200

"""
HELPER FUNCTION
"""
def distribute_file(name):
	from itertools import cycle
	file_size = os.path.getsize(os.path.join(master_app.config['UPLOAD_FOLDER'], name))

	robin = cycle(mappers)
	f = open((os.path.join(master_app.config['UPLOAD_FOLDER'], name)), "r")
	chunk = 0

	for mp in robin:
		host = mp.split(":")[0]
		port = int(mp.split(":")[1])
		buf = 1024
		addr = (host, port)
		clientsocket = socket(AF_INET, SOCK_STREAM)
		clientsocket.connect(addr)
		
		# send file
		chunk = chunk + file_size/3

		data = f.readline(chunk)
		if not data:
			break
		send_to_mapper = {}
		send_to_mapper["reducers"] = reducers
		send_to_mapper["data"] = data
		clientsocket.send(json.dumps(send_to_mapper))

		data = clientsocket.recv(buf)
		clientsocket.close()
	print "IM OUTTTT"
