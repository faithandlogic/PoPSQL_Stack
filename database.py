# database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# necessary modules

SQLALCHEMY_DATABASE_URL = 'postgresql://postgres:1219@localhost:5432/fastapi'
# Creating a Database URL, name of db will be fastapi

engine = create_engine(SQLALCHEMY_DATABASE_URL)
# creates a database engine, param specificies connection

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# configures db session to be used, autcommit, autoflush unknown


Base = declarative_base()
# base class for db table models

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
# creates a db session, closes session 
