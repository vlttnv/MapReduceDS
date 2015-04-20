import requests, json, argparse, sys, time

def main():
	with open('test.txt', 'rb') as payload:
		rq = requests.post("http://vt3.host.cs.st-andrews.ac.uk:5001/accept_file", files={'test.txt': open('test.txt', 'rb')})


if __name__ == "__main__":
	main()
