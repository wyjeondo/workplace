#적재
import pandas as pd
import psycopg2 #PostgreSQL을 사용하기 위한 python라이브러리
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

class INSERT:    

    def test_insert(self):
        print('import success!')
    
    def engine(self): #출고정보 db 접속
        self.username = 'wy.jeon' 
        self.password = 'dndud1234!'
        self.ip_address = '172.27.224.201' 
        self.port_number = 5432  
        self.db_name = 'datateam' 
        try :
            self.engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{self.db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return self.engine
    
    def engine_od(self): #주문정보 db 접속
        self.username = 'wy.jeon' 
        self.password = 'dndud1234!'
        self.ip_address = '172.27.224.201' 
        self.port_number = 5432
        self.db_name = 'datateam'
        try :
            self.engine = create_engine(f"postgresql+psycopg2://{self.username}:{self.password}@{self.ip_address}:{self.port_number}/{self.db_name}")
        except Exception as e:
            return print('Exception : ', e)
        else:
            return self.engine
        
    def read_data(self, query): #데이터 불러오기
        connect = self.engine()
        data = pd.read_sql(query, connect)
        return data
    #'''
    def insert_data(self, df, table_name): #출고정보 적재
        engine = self.engine()
        tableName = table_name
        df.to_sql(name=f'{tableName}', con=engine, schema='public', if_exists='append', index=False)
        print('Completed to Insert DATA...')

    def insert_data_od(self, df, table_name): #주문정보 적재
        engine = self.engine_od()
        tableName = table_name
        df.to_sql(name=f'{tableName}', con=engine, schema='orders', if_exists='append', index=False)
        print('Completed to Insert DATA...')
    #'''
    def insert_data_pb(self, df, table_name): #주문정보 적재
        engine = self.engine_od()
        tableName = table_name
        df.to_sql(name=f'{tableName}', con=engine, schema='public', if_exists='replace', index=False)
        print('Completed to Insert DATA...')

    # 출고율 : main_releaserate.py
    def insert_data_pb_append(self, df, table_name): #주문정보 적재
        engine = self.engine_od()
        tableName = table_name
        df.to_sql(name=f'{tableName}', con=engine, schema='public', if_exists='append', index=False)
        print('Completed to Insert DATA...')

    # 출고율 : main_releaserate.py
    def insert_data_pb_replace(self, df, table_name): #주문정보 적재
        engine = self.engine_od()
        tableName = table_name
        df.to_sql(name=f'{tableName}', con=engine, schema='public', if_exists='replace', index=False)
        print('Completed to Insert DATA...')