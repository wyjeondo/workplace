import ConnectDB
import InsertDB
import query
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class GetProcessData:

    def __init__(self, s_date,e_date):
        self.s_date = s_date
        self.e_date = e_date

    # 출고율 마감내역 초기 / 백업활용
    def get_releaserate_backup(self):
        # 데이터 추출
        q = query.Query(self.s_date, self.e_date)
        query_release_rate = q.query_inoutrate()
        db = ConnectDB.ConnectDB()
        engine_backend = db.engine_backend() 
        invoice_df = db.read_data(query_release_rate, engine_backend)
        invoice_df['마감일시'] = pd.to_datetime(invoice_df['마감일시'])
        invoice_df['마감일시'] = invoice_df['마감일시'].dt.tz_convert('Asia/Seoul')
        # 조건 dt.hour<4가 참이면 1*(invoice_df['마감일시'].dt.hour + 4)이고 , 거짓이면 0*(invoice_df['마감일시'].dt.hour + 4)
        # 즉 04시 이하이면 +4이고 아니면 +0을 한다.
        invoice_df['출고일자'] = (
            invoice_df['마감일시'] - pd.to_timedelta((invoice_df['마감일시'].dt.hour < 4) * 
                                             (invoice_df['마감일시'].dt.hour + 4), unit='h')
        )
        invoice_df['출고일자'] = invoice_df['출고일자'].dt.date
        return invoice_df
    
    # 출고율 주문정보 query_ordercount
    def get_orderscount(self):
        # 데이터 추출
        q = query.Query(self.s_date, self.e_date)
        query_ordercount = q.query_ordercount()
        db = ConnectDB.ConnectDB()
        engine_backend = db.engine_backend() 
        orders_df = db.read_data(query_ordercount, engine_backend)
        return orders_df
    
    # 출고율 마감내역 데이터 호출 (출고정보)
    def get_releaserate(self):
        # 데이터 추출
        q = query.Query(self.s_date, self.e_date)
        query_release_rate = q.query_inoutrate()
        db = ConnectDB.ConnectDB()
        engine_backend = db.engine_backend() 
        invoice_df = db.read_data(query_release_rate, engine_backend)
        return invoice_df
        
# no need to update below codes which is already moved to datateam server!!!!
    # datateam release 테이블에서 월별물동량 불러오기
    def get_monthlyrelease(self):
        q = query.Query(self.s_date, self.e_date)
        query_monthlyrelease = q.query_monthlyrelease()
        db = InsertDB.INSERT()
        raw_data = db.read_data(query_monthlyrelease)
        return raw_data
    
    # b2c 마감내역 데이터 호출 (출고정보)
    def get_b2crelease(self):
        # 데이터 추출
        q = query.Query(self.s_date, self.e_date)
        query_release = q.query_release()
        db = ConnectDB.ConnectDB()
        engine_release = db.engine_backend() 
        raw_data = db.read_data(query_release, engine_release)
        
        # 데이터 가공
        df = raw_data.drop_duplicates(['송장번호','센터명','고객사명'])    
        master = pd.pivot_table(df, index = ["information"], 
                            values = ["송장번호"], aggfunc = ["count"], fill_value = 0, margins = True)    
        master.reset_index(inplace=True)
        master.columns = master.columns.droplevel(1)
        master = master[:-1]
        master.rename(columns={'count':'마감송장건수'}, inplace=True)
        master[['마감일자', '마감일자_일자', '마감일자_연', '마감일자_월', '마감일자_일', '마감일자_시', 
                '송장번호', '센터명', '고객사명']] = master['information'].str.split(',', expand=True)
        master['마감일자'] = pd.to_datetime(master['마감일자'])
        master['출고일자'] = (
            master['마감일자'] - pd.to_timedelta((master['마감일자'].dt.hour < 4) * 
                                             (master['마감일자'].dt.hour + 4), unit='h')
        )
        master['출고일자'] = master['출고일자'].dt.date
        master.drop(columns=['information'], inplace=True)
        
        print('물동량 데이터 추출 완료!')
        return master

    # b2c 주문내역 데이터 호출 (주문정보)
    def get_b2corder(self):
        # 데이터 추출
        q = query.Query(self.s_date, self.e_date)
        query_orders = q.query_orders()
        db = ConnectDB.ConnectDB()
        engine_orders = db.engine_backend() 
        raw_data = db.read_data(query_orders, engine_orders)
        
        # 데이터 가공
        df = raw_data.drop_duplicates(['주문번호','센터명','고객사명','채널명'])    
        master = pd.pivot_table(df, index = ["information"], 
                            values = ["주문번호"], aggfunc = ["count"], fill_value = 0, margins = True)    
        master.reset_index(inplace=True)
        master.columns = master.columns.droplevel(1)
        master = master[:-1]
        master.rename(columns={'count':'주문번호건수'}, inplace=True)
        master[['주문일자', '주문일자_일자', '주문일자_연', '주문일자_월', '주문일자_일', '주문일자_시', 
                '주문번호', '채널명', '센터명', '고객사명']] = master['information'].str.split(',', expand=True)
        master.drop(columns=['information'], inplace=True)
    
        print('주문량 데이터 추출 완료!')
        return master