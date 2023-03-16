from sqlalchemy import create_engine
from datetime import datetime
import dotenv, os
from dotenv import dotenv_values

def get_database_conn():
    dotenv.load_dotenv('C:/Users/wales/Documents/Fullstack_Data_Analytics/10Alytics/Data Engineering/Data_Engineering_Projects/.secret_keys/.env')
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
