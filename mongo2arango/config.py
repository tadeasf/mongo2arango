import os
from dotenv import load_dotenv

load_dotenv()
ARANGODB_HOST = os.getenv("ARANGODB_HOST")
ARANGODB_USER = os.getenv("ARANGODB_USER")
ARANGODB_PW = os.getenv("ARANGODB_PW")
ARANGODB_DB = os.getenv("ARANGODB_DB")
