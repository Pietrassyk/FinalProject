#imports

import os
import mysql.connector
from flask import Flask, render_template
from flask import Flask, render_template, request, redirect
from werkzeug import secure_filename
from helpers import *
import time
from Private.flask_credentials import host, db , user , password

from pydub import AudioSegment
from werkzeug import FileStorage

#DEMO/DEBUG

#This is quick and dirty: Make File Class 
class File:
	def __init__(self,name,data):
		self.filename = name
		self.data = data

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
	file = request.files["user_file"]
	temp_name = file.filename
	file.save(temp_name)
	to_convert = AudioSegment.from_file(temp_name)
	new_name = temp_name.rsplit(".", 1)[0]+".wav"
	os.remove(temp_name)
	to_convert.export(new_name, format="wav")
	
	converted = open(new_name, "rb")
	file = FileStorage(converted, content_type="audio/wav")

	output = upload_file_to_s3(file, app.config["S3_BUCKET"])

	return (file.filename)

if __name__ == "__main__":
	app.run(host = "0.0.0.0", port = 80)
