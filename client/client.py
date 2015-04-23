import requests, json, argparse, sys, time

# Set up command line arguments
# TODO: writeup
parser = argparse.ArgumentParser(description='Client')
parser.add_argument('address', help='Destination IP address')
parser.add_argument('port', help='Destionation PORT number')
parser.add_argument('file', help='File name')
args = parser.parse_args()

def main():
	rq = requests.post("http://"+ args.address  +":"+ args.port  +"/accept_file", files={args.file: open(args.file, 'rb')})
	#print rq.text
	print "Job " + str(rq.text) + " created."


if __name__ == "__main__":
	main()
