import boto3, botocore
from Private.flask_credentials import S3_KEY, S3_SECRET, S3_BUCKET ,S3_LOCATION , sub_path

ALLOWED_EXTENSIONS = ["wav","flac", "mp3", "aac", "m4a" , "ac3", "aif", "aiff"]



s3 = boto3.client(
   "s3",
   aws_access_key_id=S3_KEY,
   aws_secret_access_key=S3_SECRET
)


def upload_file_to_s3(file, bucket_name, acl="public-read"):

    try:

        s3.upload_fileobj(
            file,
            bucket_name,
            sub_path+"/"+file.filename,
            ExtraArgs={
                "ACL": acl,
                "ContentType": file.content_type
            }
        )

    except Exception as e:
        # This is a catch all exception, edit this part to fit your needs.
        print("Something Happened: ", e)
        return e

    return "{}{}/{}".format(S3_LOCATION, sub_path, file.filename)

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS