#IMPORTS
from wordcloud import WordCloud
from aylienapiclient import textapi
import matplotlib.pyplot as plt


from .config import cfg_summary_len ,cfg_summary_lang

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

def generate_wordcloud(text, **kwargs): # optionally add: stopwords=STOPWORDS and change the arg below
    """Generates A wordcloud from Frequency Dict
    Params
    --------
    text:
    	##
    kwargs: dict
    	##

    Returns
    --------
    	None"""
    print("Generating wordcloud")
    wordcloud = WordCloud(font_path='/Library/Fonts/Verdana.ttf',
                          random_state = 42,
                          background_color = "white",
                          width=800,
                          height=400,
                          scale = 1,
                          max_words = 15,
                          relative_scaling = 1.0, # set or space-separated string
                          **kwargs
                          ).generate_from_frequencies(text)
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.show()

def get_summary(file, text, aylien_app_id, aylien_API_KEY, ):
  """
  Params
  --------

  Returns
  --------
  """
  print("Calling Aylien API", end = "\r")
  aylien = textapi.Client(aylien_app_id,aylien_API_KEY)
  summary = aylien.Summarize({"title": file, 
                  "text": text,
                 "sentences_number": cfg_summary_len, 
                 "language": cfg_summary_lang })
  summary_text = " ".join(summary["sentences"])
  print("Summary Successfull")
  return summary_text
