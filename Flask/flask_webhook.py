from flask import Flask, render_template
from flask import Flask, render_template, request, redirect
from werkzeug import secure_filename
from helpers import *
import time

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
	file    = request.files["user_file"]

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
		return str(output)
	else:
		return redirect("/")

if __name__ == "__main__":
	app.run()
