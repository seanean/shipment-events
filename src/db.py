from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

_engine = None
# Singleton engine, so you always get the same one
def get_engine():
    global _engine
    if _engine is None:
        load_dotenv()
        db_user = 'shrw'
        db_password = os.getenv('POSTGRES_RW_PW')
        db_host = os.getenv('POSTGRES_HOST')
        db_port = os.getenv('POSTGRES_PORT')
        db_name = os.getenv('POSTGRES_DB')
        db_url = f'postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        _engine = create_engine(db_url)
    return _engine