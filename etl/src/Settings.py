import os
from dotenv import load_dotenv

from pydantic import BaseSettings


load_dotenv()


class Dsn(BaseSettings):
    dbname: str = os.environ.get('DB_NAME')
    user: str = os.environ.get('DB_USER')
    password: str = os.environ.get('DB_PASSWORD')
    host: str = os.environ.get('DB_HOST')
    port: int = os.environ.get('DB_PORT')
    options: str = '-c search_path=content'

    class Config:
        env_file = '../.env'
        env_file_encoding = 'utf-8'
