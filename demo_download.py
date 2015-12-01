import csv
import json
from pymongo import MongoClient
import requests
from requests_oauthlib import OAuth1
import cnfg
import time
from random import randint
import os
import inspect, os
import glob, os
import arrow
import time
from datetime import date
from datetime import timedelta
import shutil
from pyunpack import Archive
from multiprocessing import Process, Queue
import time
from random import randint

'''
Determine file path so script can run in cron
'''
cwd = '/Users/johnwalters/ds/metis/projects/cs_go_stats'
#cwd = inspect.getfile(inspect.currentframe())
#file_name = 'demo_download.py'
#cwd = cwd[0:len(cwd)-len(file_name)]

def download_file(url, demo_number):
    local_filename = cwd + '/compressed_demos/' + str(demo_number) + '.rar'
    r = requests.get(url, stream=True)
    #while r.status_code != 200:
	#    print "Waiting for webpage to respond"
	#    print date
	#    time.sleep(randint(1,10))
	#    response = r.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                f.write(chunk)
    return local_filename

def extract_demo_info(demo_number):
    Archive(cwd + '/compressed_demos/' + str(demo_number) + '.rar').extractall(cwd + '/uncompressed_demos')
    os.chdir(cwd + '/uncompressed_demos')
    i = 1
    for demo in glob.glob("*.dem"):
        demoinfogo = cwd + '/demoinfogo-linux/demoinfogo'
        demoinfo_params = '-gameevents -extrainfo'
        demo_location = cwd + '/uncompressed_demos/' + demo
        output_loc = '/Users/johnwalters/ds/metis/projects/cs_go_stats/demos/demo' + str(demo_number) + '_events' + str(i) + '.txt'
        full_text = "arch -i386 " + demoinfogo + " " + demo_location + " " + demoinfo_params + " > " + output_loc
        os.system(full_text)
        demoinfo_params = '-deathscsv -nowarmup'
        output_loc = '/Users/johnwalters/ds/metis/projects/cs_go_stats/demos/demo' + str(demo_number) + '_deaths' + str(i) + '.csv'
        full_text = "arch -i386 " + demoinfogo + " " + demo_location + " " + demoinfo_params + " > " + output_loc
        os.system(full_text)
        i += 1

### Remove contents of folder
def clean_folders(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception, e:
            print e

for demo_number in range(19107, 19000, -1):
    url = 'http://www.hltv.org/interfaces/download.php?demoid='+ str(demo_number)
    download_file(url, demo_number)
    extract_demo_info(demo_number)
    clean_folders(cwd + '/uncompressed_demos')
    clean_folders(cwd + '/compressed_demos')