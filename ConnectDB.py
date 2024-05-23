import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

class ConnectDB:
    def __init__(self):
        self.username = 'wy.jeon' 
        self.password = '1234' 
        self.ip_address = 'poomgo-production-cluster.cluster-cfsilzqil2ni.ap-northeast-2.rds.amazonaws.com'
        self.port_number = 5432 
    
    def engine_backend(self): 
        db_name = 'poomgo_backend' 
        try :
            engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return engine
            
    def engine_lms(self): 
        db_name = 'poomgo_lms' 
        try :
            engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return engine
            
    def engine_now(self): 
        db_name = 'poomgo_now' 
        try :
            engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return engine

    def engine_auth(self): 
        db_name = 'poomgo_authentication' 
        try :
            engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return engine
            
    def engine_auth(self): 
        db_name = 'poomgo_authentication' 
        try :
            engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return engine
    
    
    def read_data(self, query, engine):
        #connect = engine
        data = pd.read_sql(query, engine)
        return data