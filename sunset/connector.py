from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import OperationalError
import logging

class DBConnection():

    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.error = ""

    def __enter__(self):
        self.engine = create_engine(self.db_connection)
        
        try:
            self.engine.connect()
        except OperationalError as e:
            self.error += e.orig.args[1]
            
        self.session_ = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_)
        return self.session, self.engine
    

    def __exit__(self, *args):
        try:
            self.session.close()
        except AttributeError as e:
            logging.warning(f'\n Database Never connected \n {self.error}')