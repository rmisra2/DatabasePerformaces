import sys, subprocess
def filterfile(filename):
	with open('output-'+filename,'w') as fout:
		with open(filename,'r') as fin:
			for line in fin:
				if 'DAGScheduler' and ' finished:' in line or 'sparkquery' in line:
					fout.write(line + "\n")
	subprocess.call(["rm "+filename],shell=True)
	print("Fout generated..")
filterfile(sys.argv[-1])
