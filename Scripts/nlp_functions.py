#IMPORTS
from wordcloud import WordCloud
from aylienapiclient import textapi
import matplotlib.pyplot as plt
import boto3
import io

from .config import cfg_summary_len ,cfg_summary_lang , image_folder , bucket_name ,bucket_path

def bigram_to_single_word(bigrams_fd):
  """Concatenate Bigramds to one word seperated by a Space
	Params
	-------
	bigrams_df : dict
		Dictionary or NLTK FreqDict object
	Returns
	--------
	out : dict
		Frequency dictionary containing the concatted bigrams and their frequencies
  """
  out = dict(zip([x[0]+" "+x[1] for x in bigrams_fd.keys()],bigrams_fd.values()))
  return out

def generate_wordcloud(text, file, **kwargs): # optionally add: stopwords=STOPWORDS and change the arg below
    """Generates A wordcloud from Frequency Dict
    Params
    --------
    text: dict ofr nltk FDict Object
    	Frequency dictionary of the text to be processed
    kwargs: dict
    	keyword arguments for genrating the wordcloud. Used with the WodCllud Object from the wordcloud library

    Returns
    --------
    image_url : str
      S3-Bucket URL of the stored wordcloud"""
    print("Generating wordcloud")
    wordcloud = WordCloud(font_path="Flask/Verdana.ttf",
                          random_state = 42,
                          background_color = None,
                          width=900,
                          height=600,
                          scale = 1.5,
                          max_words = 15,
                          mode = "RGBA",
                          relative_scaling = 1.0, # set or space-separated string
                          **kwargs
                          ).generate_from_frequencies(text)

    #cache figure as a file in memory
    img_data = io.BytesIO()
    plt.figure(figsize = (14,10))
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.savefig(img_data, format='png', transparent = True)
    img_data.seek(0)
    
    #load to s3 bucket
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    Key = f"{image_folder}/{file}.png"
    my_bucket.put_object(Body=img_data,ContentType='image/png', ACL = "public-read", Key=Key)

    #build url
    url_split = bucket_path.split("//")
    url = f"{url_split[0]}//{bucket_name}.{url_split[1].split('/')[0]}"
    image_url = f"{url}/{Key}"
    
    return image_url

def get_summary(file, text, aylien_app_id, aylien_API_KEY, ):
    """
    Params
    --------

    Returns
    --------
    summary_text : str
      Summarized text
    summary_bullerts : list
      List that contains each scentence of the summary as individual entries
    """
    print("Calling Aylien API", end = "\r")
    aylien = textapi.Client(aylien_app_id,aylien_API_KEY)
    summary = aylien.Summarize({"title": file, 
                    "text": text,
                   "sentences_number": cfg_summary_len, 
                   "language": cfg_summary_lang })
    summary_text = " ".join(summary["sentences"])
    summary_bullets = summary["sentences"]
    print("Summary Successfull")
    return summary_text , summary_bullets
