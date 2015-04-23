from master import master_app, models, db
from flask import request, render_template
import time
import json
import os
from socket import *

mappers = dict()
reducers = dict()

@master_app.route('/')
def index():
	jobz = models.Job.query.all()
	jobs = {}
	for job in jobz:
		jobs[job.id] = job.status
	return render_template('index.html', jobs=jobz)

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

@master_app.route('/get_reducers')
def get_reducers():
	return json.dumps(reducers), 200

@master_app.route('/single_job/<int:id>')
def single_job(id):
	jobz = models.Final().query.filter_by(job_id=id).one()
	return render_template('single_job.html', jobs=jobz)

@master_app.route('/complete_subjob', methods=['POST'])
def complete_subjob():
	if request.method == 'POST':
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
		sj = models.Subjob()
		sj.job_id = int(post_data['job_id'])
		sj.data = json.dumps(post_data['data'])

		db.session.add(sj)
		db.session.commit()

		j = models.Job().query.filter_by(id=post_data['job_id']).one()
		j.expected -= 1
		db.session.add(j)
		db.session.commit()

		if j.expected == 0:
			j.status = 2
			final = {}
			all_data = models.Subjob.query.filter_by(job_id=int(post_data['job_id'])).all()
			for sub in all_data:
				sub = json.loads(sub.data)
				for word in sub:
					if word not in final:
						final[word] = int(sub[word])
					else:
						final[word] += int(sub[word])
			fn = models.Final()
			fn.job_id = post_data['job_id']
			fn.data = json.dumps(final)
			db.session.add(fn)
			db.session.commit()

			print "Job done"


		return "Thank you, come again", 200
	return "Need to be post", 400


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
	return str(job.id), 200

"""
HELPER FUNCTION
"""
def distribute_file(name, job_id):
	from itertools import cycle
	file_size = os.path.getsize(os.path.join(master_app.config['UPLOAD_FOLDER'], name))

	#robin = cycle(mappers)
	f = open((os.path.join(master_app.config['UPLOAD_FOLDER'], name)), "r")
	chunk = 0

	for mp in mappers:
		host = mp.split(":")[0]
		port = int(mp.split(":")[1])
		buf = 1024
		addr = (host, port)
		clientsocket = socket(AF_INET, SOCK_STREAM)
		clientsocket.connect(addr)
		
		# send file
		chunk = chunk + file_size/len(mappers)

		data = f.readlines(chunk)
		send_to_mapper = {}
		send_to_mapper["job_id"] = job_id
		send_to_mapper["reducers"] = reducers
		send_to_mapper["data"] = data
		clientsocket.send(json.dumps(send_to_mapper, ensure_ascii=True))
		clientsocket.close()
	print "IM OUTTTT"
