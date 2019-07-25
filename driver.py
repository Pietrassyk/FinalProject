#This is the driver for Batching the subseqent Pipelines

import datetime
import os
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(timestamp)


print("Starting driver")

print("""
##################
#Step 1 : ETL    #
##################""")
os.system("python3 etl_pipeline.py >>log.txt")
#exec(open("etl_pipeline.py").read())

print("""
##################
#Step 2 : NLP    #
##################""")
os.system("python3 nlp_pipeline.py >>log.txt")
#exec(open("nlp_pipeline.py").read())

print("""
##################
#    FINISHED    #
##################""")