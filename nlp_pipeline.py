#IMPORTS
########
import pandas as pd
import numpy as np
import mysql.connector
import tpclean.tpclean as tp

#custom imports
from Scripts.config import role, bucket_name, prefix, bucket_path, sub_path
from Scripts.nlp_functions import generate_wordcloud , get_summary

#establish connection
from Scripts.config import host, db
from Private.private import user , password

#NLTK
from nltk import FreqDist
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer

#Imports for Bigrams
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from Scripts.nlp_functions import bigram_to_single_word
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#API Imports
from Private.private import aylien_app_id,aylien_API_KEY

print("========SETUP========")
#load credentials
conn_kwargs = {"host":host, 
               "user":user, 
               "password":password}
conn = tp.sql_connect(db,db_type="mysql",**conn_kwargs)
#conn = mysql.connector.Connect(database = db, **conn_kwargs)
c = conn.cursor()

#get all the files that dont have frequencies yet
words_df = tp.sql("""SELECT origin
FROM content
GROUP BY origin
HAVING COUNT(freq) = 0""")

i=0
try:
    job_list = list(words_df.iloc[:,0])
    print("========SETUP COMPLETE========")
except:
    print("No Jobs available")
    conn.close()
    exit()

print("========Starting JOBs========")

for file in job_list:
    #VERBOSE
    i += 1
    print(f"Starting Job {i}/{len(job_list)}")
    print(f"Current File: {file}")
    
    #get full text
    text_df = tp.sql(f"""SELECT full_text FROM conversations WHERE filename = "{file}" """)
    text = text_df["full_text"][0]
    
    #get the words from the content table
    file_df = tp.sql(f"""SELECT pos_in_conv,
                    LOWER(content) as content
                    FROM content
                    WHERE type = "pronunciation" and origin = "{file}" """)
    
    ##REMOCE STOPWORDS AND LEMMATISE
    stopword_list = stopwords.words("english")
    lemmatizer = WordNetLemmatizer()
    
    #define function for stopping and lemmatizing and apply it to the DataFrame
    stoplem = lambda x: np.NaN if x in stopword_list else lemmatizer.lemmatize(x)
    file_df["lemmatized"] = file_df["content"].apply(stoplem)
    
    #create freqency dict
    fDist_lemm = FreqDist(file_df["lemmatized"].dropna(),)
    freqs = pd.DataFrame.from_dict(fDist_lemm,orient = "index", columns = ["freq"])
    
    #calculate freq ranks
    sort_freq = freqs.sort_values(by="freq", ascending = False).reset_index()
    unique_freqs = sort_freq.freq.unique()
    freq_ranks = pd.DataFrame(list(zip(unique_freqs,range(len(unique_freqs)))),columns = ["freq","freq_rank"])
    
    #append frequencies snd ranks to lemmatized words
    file_df = pd.merge(file_df,freqs, how = "left", left_on = "lemmatized", right_index = True)
    file_df = pd.merge(file_df, freq_ranks, how = "left", on = "freq")
    
    ## CREATE BIGRAMS
    finder = BigramCollocationFinder.from_words(file_df["lemmatized"].dropna())
    finder.nbest(BigramAssocMeasures.likelihood_ratio, 10)
    bigrams_fd = finder.ngram_fd
    
    #visualize in a wordcloud
    testfd = bigram_to_single_word(bigrams_fd)
    #generate_wordcloud(testfd)
    
    #SENTIMENT ANALYSIS
    sia = SentimentIntensityAnalyzer()
    sc = lambda x: sia.polarity_scores(x)["compound"]
    file_df["sentiment_score"] = file_df["lemmatized"].dropna().apply(sc)
    file_df["weight"] = file_df["freq"]*file_df["sentiment_score"]
    
    #update DB
    updates = file_df.dropna()
    for i in range(len(updates)):
        row = updates.iloc[i]
        
        #build UPDATE querry
        out =[f'{key} = "{row[key]}"' for key in row.keys()[1:]]
        set_ = ", ".join(out)
        querry = f"""UPDATE content
                     SET {set_}
                     WHERE pos_in_conv = {row['pos_in_conv']}
                     AND origin = "{file}"
                 """
        c.execute(querry)
    conn.commit()
    print("Inserted frequency dict in content")
    
    #GET SUMMARY
    summary_text , summary_bullets = get_summary(file, text, aylien_app_id, aylien_API_KEY)
    
    #write Summary to DB
    c.execute(f"""UPDATE conversations
            SET summary = "{summary_text}"
            WHERE filename = "{file}" """)
    conn.commit()

    #write bulletpoints to DB
    print("Writing Bulletpoints to summary_bullets table")
    i = 1
    for bulletpoint in summary_bullets:
        c.execute(f"""INSERT INTO summary_bullets(origin, bullet_pos, bullet)
            VALUES ("{file}", {i}, "{bulletpoint}")
            """)
        i += 1
    conn.commit()

    print("--------Job complete--------")
conn.close()
print("========ALL JOBS FINISHED========")