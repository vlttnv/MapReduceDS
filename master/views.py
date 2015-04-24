from master import master_app, models, db
from flask import request, render_template, redirect, url_for
import time
import json
import os
from socket import *

# Keep track of components
mappers = dict()
reducers = dict()

# Save state when a job is created
state = dict()

"""
Index page displaying all the jobs
and allowing the user to create a new job
"""
@master_app.route('/')
def index():
	jobz = models.Job.query.all()
	jobs = {}
	for job in jobz:
		jobs[job.id] = job.status
	return render_template('index.html', jobs=jobz, mappers=mappers, reducers=reducers)

"""
Register a mapper on its startup
Record a timestamp
"""
@master_app.route('/register_mapper/<int:port>')
def register_mapper(port):
	mappers[request.remote_addr+":"+str(port)] = {"stamp":int(time.time())}
	return "success", 200

"""
Register a reducer on its startup
Record a timestamp
"""
@master_app.route('/register_reducer/<int:port>')
def register_reducer(port):
	reducers[request.remote_addr+":"+str(port)] = {"stamp":int(time.time())}
	return "success", 200

"""
Deregister a mapper on clean exit
"""
@master_app.route('/deregister_mapper/<int:port>')
def deregister_mapper(port):
	del mappers[request.remote_addr+":"+str(port)]
	return "success", 200

"""
Deregister a reducer on clean exit
"""
@master_app.route('/deregister_reducer/<int:port>')
def deregister_reducer(port):
	del reducers[request.remote_addr+":"+str(port)]
	return "success", 200

"""
Receive a hearbeat from a mapper or
a reducer (0 or 1) and record the address and port
"""
@master_app.route('/heartbeat/<int:type>/<int:port>')
def heardbeat(type,port):
	if type == 0:
		mappers[request.remote_addr+":"+str(port)]["stamp"] = int(time.time())
	elif type == 1:
		reducers[request.remote_addr+":"+str(port)]["stamp"] = int(time.time())
	else:
		return "Invlaid type", 500

	return "OK", 200

"""
Record a subjob.
This is sent by a mapper when a bucket is sent to the reducer
"""
@master_app.route('/schedule_subjob/<int:job_id>')
def schedule_subjob(job_id):
	job = models.Job().query.filter_by(id=job_id).one()
	job.expected += 1
	db.session.add(job)
	db.session.commit()
	return "OK", 200

@master_app.route('/get_mappers')
def get_mappers():
	return json.dumps(mappers), 200

"""
Live mappers
"""
@master_app.route('/get_mappers_num')
def get_mappers_num():
	return str(len(mappers)), 200

@master_app.route('/get_reducers')
def get_reducers():
	return json.dumps(reducers), 200

"""
Generate and return the HTML page to display the 
data for a job
"""
@master_app.route('/single_job/<int:id>')
def single_job(id):
	all_data = models.Subjob().query.filter_by(job_id=id).all()
	final = {}
	for sub in all_data:
		sub = json.loads(sub.data)
		for word in sub:
			final[word] = int(sub[word])
	return render_template('single_job.html', jobs=final)

"""
Used by a reducer to send its final data
"""
@master_app.route('/complete_subjob', methods=['POST'])
def complete_subjob():
	if request.method == 'POST':

		# Safety first
		if not request.json:
			print "Not json"
			return "Need JSON", 400
		if not ('job_id' and 'data' in request.json):
			print "invalid"
			return "Invalid JSON", 400
	
		post_data = {
				'job_id': int(request.json['job_id']),
				'data': request.json['data']
				}

		# Check if something died in the middle
		if state[post_data['job_id']]["mappers"] != mappers or state[post_data['job_id']]["reducers"] != reducers:
			print "Invalid job"

		sj = models.Subjob()
		sj.job_id = int(post_data['job_id'])
		sj.data = json.dumps(post_data['data'])

		db.session.add(sj)
		db.session.commit()

		return "Thank you, come again", 200
	return "Needs to be POST", 400

"""
Receive the file for processing
Used by client and web interface
"""
@master_app.route('/accept_file', methods=['POST'])
def accept_file():
	if request.method == 'POST':
		fl_keys = request.files.keys()
		fl = request.files[fl_keys[0]]
		fl.save(os.path.join(master_app.config['UPLOAD_FOLDER'], fl.filename))

		job = models.Job()
		job.status = 0
		db.session.add(job)
		db.session.commit()

		distribute_file(fl.filename, job.id)
	return "<a href='/single_job/" +str(job.id) +"'>Go to job</a>", 200

"""
Distribute the file to mappers
and create job
"""
def distribute_file(name, job_id):
	file_size = os.path.getsize(os.path.join(master_app.config['UPLOAD_FOLDER'], name))

	f = open((os.path.join(master_app.config['UPLOAD_FOLDER'], name)), "r")
	chunk = 0

	for mp in mappers:
		host = mp.split(":")[0]
		port = int(mp.split(":")[1])
		buf = 1024
		addr = (host, port)
		clientsocket = socket(AF_INET, SOCK_STREAM)
		clientsocket.connect(addr)
		
		# Partition file
		chunk = chunk + file_size/len(mappers)
		data = f.readlines(chunk)
		send_to_mapper = {}
		send_to_mapper["job_id"] = job_id

		# Check for dead mappers/reducers
		t =time.time()
		try:
			for r in reducers:
				if t- int(reducers[r]["stamp"]) > 2:
					print t- int(reducers[r]["stamp"])
					del reducers[r]
		except RuntimeError:
			print "Caught"

		
		# Build message and send it to mappers
		send_to_mapper["reducers"] = reducers
		send_to_mapper["data"] = data
		clientsocket.send(json.dumps(send_to_mapper, ensure_ascii=True))
		clientsocket.close()
		state[job_id] = dict()
		state[job_id]["mappers"] = mappers
		state[job_id]["reducers"] = reducers
	print "[Ma] Job started"
