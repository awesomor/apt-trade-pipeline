import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


# 프로젝트 루트 기준
BASE_DIR = os.getenv("BASE_DIR")


# 데이터 파일
DATA_DIR = os.path.join(BASE_DIR, "data")
SGG_FILENAME = os.getenv("SGG_FILENAME")
 
# API 정보
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL") 
SGG_API_URL = os.getenv("SGG_API_URL")

