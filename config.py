from dotenv import load_dotenv
import os


if os.environ.get("IS_TEST"):
    load_dotenv()

DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_NAME = os.getenv("DATABASE_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "deliver-ar-s3-app-1fc9ba991cadc757")
POSTGRES_JAR_PATH = os.getenv("POSTGRES_JAR_PATH", "/home/ec2-user/jars/postgresql-42.7.5.jar")
