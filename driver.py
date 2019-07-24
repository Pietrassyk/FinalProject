#This is the driver for Batching the subseqent Pipelines

print("Starting driver")

print("""
##################
#Step 1 : ETL    #
##################""")
exec(open("etl_pipeline.py").read())

print("""
##################
#Step 2 : NLP    #
##################""")
exec(open("nlp_pipeline.py").read())

print("""
##################
#    FINISHED    #
##################""")