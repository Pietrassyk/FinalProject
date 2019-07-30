#imports

import os
import mysql.connector
from flask import Flask, render_template
from flask import Flask, render_template, request, redirect
from werkzeug import secure_filename
from helpers import *
import time
from Private.flask_credentials import host, db , user , password

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

	timestamp = time.time()
	with open("log.txt","a") as f:
		f.write(f"{timestamp}\n")
	# A
	if "user_file" not in request.files:
		return "No user_file key in request.files"

	# B
	file = request.files["user_file"]

	"""
        These attributes are also available

        file.filename               # The actual name of the file
        file.content_type
        file.content_length
        file.mimetype

	"""

	# C.
	if file.filename == "":
		return "Please select a file"

	# D.
	if file and allowed_file(file.filename):
		file.filename = secure_filename(file.filename)
		output   	  = upload_file_to_s3(file, app.config["S3_BUCKET"])

		#run the pipelines
		os.system("cd .. && python3 driver.py >> log.txt")

		#create answer as website

		##connect to DB
		conn = mysql.connector.Connect(database = db, **conn_kwargs)
		c = conn.cursor()
		
		#get bullets image and transcription text
		c.execute(f"""	SELECT bullet
						FROM summary_bullets
						WHERE origin = "{file.filename}"
						ORDER BY bullet_pos""")
		bullets = [entry[0] for entry in c.fetchall()]
		

		c.execute(f"""	SELECT full_text 
						FROM conversations
						WHERE filename = '{file.filename}'""")
		transcription = c.fetchall()[0][0]
		
		image = "https://www.mathworks.com/help/examples/matlab/win64/CreateWordCloudFromTableExample_01.png"

		conn.close()
		return render_template("success.html", bullets = bullets, image = image, transcription = transcription, file_name = file.filename)

		#return str(output)
	else:
		return redirect("/")

if __name__ == "__main__":
	app.run(host = "0.0.0.0", port = 80)
