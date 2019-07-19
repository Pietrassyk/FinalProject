#imports
import numpy as np
import pandas as pd
import boto3
import time



def find_audios(bucket_name, dtype = "wav"):
    """Get Audiofiles from an s3 bucket. This is meant to run on a Sagemaker instance
    Params:
    --------
    bucket_name : str
    dtype : str
        fileformat to look for e.g. wav """
    
    #connect to S3
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)  
    
    s3files=[]
    for my_bucket_object in my_bucket.objects.all():
        filename = my_bucket_object.key.split("/")[-1]
        #check whether object is a wav file
        if dtype in filename.split(".")[-1]:
            s3files.append(filename)
    return s3files

def files_in_table(c, table, file_column):
    """Finds unique entries in a column
    Params
    --------
    c : cursor Object 
        Cursor with established connection to database, to execute Querry
    table : str
        Table name to be checked
    file_columm : str
        Name of the column where the filenames are stored

    Returns
    --------
    files : set
        set of filenames that are found by the querry"""
    try:
        c.execute(f"""SELECT DISTINCT {file_column} FROM {table}""")
        files = set([x[0] for x in c.fetchall()])
        print(f"Found {len(files)} files in {table} table")
    except:
        print(f"Could not find Files in {table} Table")
        files = {}
    return files

def transcribe_wav(job_uri, dtype="wav" , lang = 'en-US' ,enforce = False, **kwargs):
    """Transcribe a wav file using a AWS trancribe web API call
    
    Params:
    --------
    job_uri : str
        path to aufiofile in an s3 bucket
    dtype : str
        file format of the audio file
    lang : str
        language spoken in the audiofile
    enforce : bool
        whether or not to enforce doing the transcription job when filename already found in prior joblist
    
    Returns:
    --------
    trans_json : Json-Object
        return from the API Call
    trans_json_uri : str
        url to the transcriptionjob json"""
      
    #Call API
    transcribe = boto3.client('transcribe')
    
    #create Jobname from Filename
    job_name = job_uri.split("/")[-1]
    
    #Check whether file is already transcribed
    jobs = transcribe.list_transcription_jobs()['TranscriptionJobSummaries']
    job_names = [job['TranscriptionJobName'] for job in jobs]
    
    if job_name in job_names:
        print("File already transcribed")
        go_on = enforce
    else:
        go_on = True
    
    #Call for Transcription Job
    if go_on:

        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat= dtype,
            LanguageCode= lang, 
            **kwargs)
    
        #print status update
        while True:
            status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            print("Not ready yet...", end="\r")
            time.sleep(5)
        print(status)
    
    #cache outputs
    trans_json = transcribe.get_transcription_job(TranscriptionJobName=job_name)
    trans_json_uri = trans_json["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    
    #Insert JSON to DataBase here!
    
    
    
    
    print("Transcription succesfull")
    return trans_json , trans_json_uri

def get_pause(df,start_time,end_time):
    """Converts the end time of a word and the start time of the next word into the pause between these words
    Params:
    --------
    df : pandas DataFrame
        Dataframe containing the timestamps
    start_time : str
        Columnn name ot the start_time stamps
    end_time : str
        Columnn name ot the end_time stamps
    
    Returns:
    --------
    df : pandas DataFrame
        updated Dataframe containing a "pause_after" column
    """
    pause_after = []
    
    for i in range(len(df)-1):
        j=1
        
        #if next item i nan keep looking forward until it isn't
        while (np.isnan(df[start_time][i+j])) and (i+j <len(df)-1):
            j +=1
        pause_after.append(df[start_time][i+j]-df[end_time][i])
    
    #add zero to the end and push to dataframe
    df["pause_after"] = pd.Series(pause_after).append(pd.Series({len(pause_after):0}))
    return df