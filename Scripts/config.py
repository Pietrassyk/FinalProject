#ETL Config
role = "arn:aws:iam::599281969166:role/service-role/AmazonSageMaker-ExecutionRole-20190716T181300"
bucket_name='finaldebatebucket'
prefix = 'sagemaker/FinalProject'
bucket_path = 'https://s3-us-east-2.amazonaws.com/finaldebatebucket'
sub_path = "cache-data"

#Connection settings RDS
host  = 'debaterdb.c7oenlqovcjd.us-east-2.rds.amazonaws.com'
db = "debater"

#Summary config:
cfg_summary_len = 3
cfg_summary_lang = "english"