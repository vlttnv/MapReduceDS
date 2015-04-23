from master import db

class Job(db.Model):
	id				= db.Column(db.Integer, primary_key=True)
	status		= db.Column(db.Integer)
	expected	= db.Column(db.String(128))

