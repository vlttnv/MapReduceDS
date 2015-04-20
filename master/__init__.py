from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

master_app = Flask(__name__)
master_app.config.from_object('config')
db = SQLAlchemy(master_app)

from master import views, models
