=== Installation ===
pip install flask-sqlalchemy==2.0
pip install flask==0.10.1
pip install requests==2.6.0
pip install sqlalchemy-migrate==0.9.6
pip install sqlalchemy==0.9.9

SQLite3
Python 2.7

=== Setup ===
run the python script 'db_create.py' to create the DB

=== Running ===
run the python script 'run_master.py' to start the master
By default the server will listen on all interface and port 8080. 
This can be changed from the run_master.py file

The Mappers and Reducers can be run after that (or before that).
use '-h' to view the help menu and available paramenter.
Exiting is done using Ctrl-C.

All the components can be ran locally or on different machines.


=== Web Interface ===
Although the terminal client is provided, it is suggested
that the web UI is used.
It can be accessed on <address of master machine>:8080/
