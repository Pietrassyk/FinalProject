#imports

import os
import mysql.connector
from flask import Flask, render_template
from flask import Flask, render_template, request, redirect

from pydub import AudioSegment

from werkzeug import secure_filename
from werkzeug import FileStorage
from helpers import *
import time
from Private.flask_credentials import host, db , user , password



#DEMO/DEBUG

#This is quick and dirty: Make File Class 
class File:
	def __init__(self,name):
		self.filename = name

#Setup for DB Connection
conn_kwargs = {"host":host, 
               "user":user, 
               "password":password}

app = Flask(__name__)
app.config.from_object("Private.flask_credentials")

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/", methods=["POST"])

def upload_file():
	##connect to DB
	conn = mysql.connector.Connect(database = db, **conn_kwargs)
	c = conn.cursor()

	timestamp = time.time()
	with open("log.txt","a") as f:
		f.write(f"{timestamp}\n")
	# A
	if "user_file" not in request.files:
		#DEMO
		#Get last submitted File
		c.execute("""	SELECT filename 
						FROM conversations
						ORDER BY idconversations DESC
						LIMIT 1""")
		last_audio = c.fetchall()[0][0]
		file = File(last_audio)
		
		#get bullets ,transcription text, image , audio_path
		c.execute(f"""	SELECT bullet
						FROM summary_bullets
						WHERE origin = "{file.filename}"
						ORDER BY bullet_pos""")
		bullets = [entry[0] for entry in c.fetchall()]
		

		c.execute(f"""	SELECT full_text , bigram_cloud_url , audio_path 
						FROM conversations
						WHERE filename = '{file.filename}'""")
		transcription, image, audio_path  = c.fetchall()[0]
		
		conn.close()
		return render_template("success.html", 
			bullets = bullets, image = image, 
			transcription = transcription, 
			file_name = file.filename, 
			audio_path = audio_path)

		#KEEP Below after Debugging
		return "No user_file key in request.files"

	# B
	file = request.files["user_file"]

	# C.
	if file.filename == "":
		return "Please select a file"

	# D.
	if file and allowed_file(file.filename):
		
		#convert file if necessary
		if file.filename[-4:] != ".wav":
			
			#temporarily store file to avoid No Header bug		
			temp_name = file.filename
			file.save(temp_name)

			#Read and convert bytes then store temp file
			to_convert = AudioSegment.from_file(temp_name)
			new_name = temp_name.rsplit(".", 1)[0]+".wav"
			os.remove(temp_name)
			to_convert.export(new_name, format="wav")

			#Open temp file and wrap in werkzeug FileStorage Object, so Boto3 can properly handle it
			converted = open(new_name, "rb")
			file = FileStorage(converted, content_type="audio/wav")
			os.remove(new_name)
			
		file.filename = secure_filename(file.filename)
		output   	  = upload_file_to_s3(file, app.config["S3_BUCKET"])

		#run the pipelines
		os.system("cd .. && python3 driver.py >> log.txt")

		##connect to DB
		#conn = mysql.connector.Connect(database = db, **conn_kwargs)
		#c = conn.cursor()
		
		#get bullets ,transcription text, image , audio_path
		c.execute(f"""	SELECT bullet
						FROM summary_bullets
						WHERE origin = "{file.filename}"
						ORDER BY bullet_pos""")
		bullets = [entry[0] for entry in c.fetchall()]
		

		c.execute(f"""	SELECT full_text , bigram_cloud_url , audio_path 
						FROM conversations
						WHERE filename = '{file.filename}'""")
		transcription, image, audio_path  = c.fetchall()[0]
		
		conn.close()
		return render_template("success.html", 
			bullets = bullets, image = image, 
			transcription = transcription, 
			file_name = file.filename, 
			audio_path = audio_path)

		#return str(output)
	else:
		return redirect("/")

if __name__ == "__main__":
	app.run(host = "0.0.0.0", port = 80)
