import pandas as pd
import requests

import mysql.connector
import tpclean.tpclean as tp

from sqlalchemy import create_engine

#custom imports
from Scripts.config import role, bucket_name, prefix, bucket_path, sub_path
from Scripts.etl_functions import find_audios , transcribe_wav , get_pause

#establish connection
from Scripts.config import host, db
from Private.private import user , password 


#SETUP
##########################
conn_kwargs = {"host":host, 
               "user":user, 
               "password":password}
conn = mysql.connector.Connect(database = "debater", **conn_kwargs)
c = conn.cursor()

#connecting via sqlalchemy because pandas needs an engine to store data in an mysql DB
engine = create_engine(f'mysql+pymysql://{user}:{password}@{conn_kwargs["host"]}:3306/{db}')

#get content of content table to check if files exist
try:
    files_in_content = set(tp.sql("SELECT * FROM content").origin.unique())
except:
    files_in_content = []
                    
audio_files = find_audios(bucket_name)
i=0

print("=======Setup Complete========")

                       
                       
#EXTRACT TRANSFORM LOAD
#############################
for filename in audio_files:
    i+=1
    print(f"Performing Job {i}/{len(audio_files)}")
    print(f"Current File: {filename}")
    job_uri = f"{bucket_path}/{sub_path}/{filename}"
    trans_json_uri = transcribe_wav(job_uri)[1]

    #load json from URL
    r = requests.get(trans_json_uri)

    #store json
    explore = r.json()

    #store full text
    fulltext = explore["results"]["transcripts"][0]["transcript"]

    #create Dataframe
    df = pd.DataFrame(explore["results"]["items"])
    
    #unnest the data using tpclean
    df = tp.unnest_df_list(df,["alternatives"])
    df = tp.unnest_df_dict(df,["alternatives_1"])
    df.rename({"alternatives_1_confidence":"confidence", 
               "alternatives_1_content": "content"}, 
              axis = "columns", inplace = True)
    
    #convert columns containing numbers into float datatype
    for col in df.columns:
        try:
            df[col] = df[col].astype("float")
        except:
            continue
            
    #engineer length of word and pauses between words
    df["length"] = df.end_time-df.start_time
    get_pause(df,"start_time","end_time");

    #append filename
    df["origin"] = filename

    #append default speaker for now
    df["speaker"] = "speaker_default"

    #append word 
    df = df.reset_index().rename({"index":"pos_in_conv"},axis = "columns");
            
    #check whether file already in the DB
    if not len(set(df.origin.unique()).intersection(files_in_content)):
        #append data to DB
        df.to_sql("content",engine, if_exists="append")
    else:
        print("File transcription already in Database -> will not append!")
        print("--------Job complete--------")
        continue
    
    #load metadata and text into database
    #querry for columms
    c.execute("DESCRIBE conversations")
    columns = ", ".join(pd.DataFrame(c.fetchall()).iloc[1:,0])
        
    #extract information from filename
    name_split = filename[:-4].split("_")
    
    #fill values                   
    values = [filename,
                name_split[-2].lower(), 
                int(name_split[-1].lower() == "pro"), 
                trans_json_uri, 
                fulltext]
    
    #Querry Database
    querry = """INSERT INTO conversations({}) VALUES ("{}","{}",{},"{}","{}");""".format(columns,*values)
    c.execute(querry)
    conn.commit()
    print("--------Job complete--------")

#close connection
conn.close()
print("========Finished ETL========")