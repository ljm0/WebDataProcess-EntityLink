#import commands
import os
import subprocess

#(status, domain) = commands.getstatusoutput('sh ./getElasticsearchURL.sh')
#domain = os.system('sh ./getElasticsearchURL.sh')
#tempP = os.popen('sh ./getElasticsearchURL.sh')
#domain=tempP.read()

#print(domain)
#tempP.close()

#domain = subprocess.getoutput('sh ./getElasticsearchURL.sh')
#print(domain)

pipe = subprocess.Popen('sh ./getElasticsearchURL.sh', shell=True, stdout=subprocess.PIPE).stdout
print(pipe.read())
pipe.close()
