import GetProcessData
import InsertDB
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
import utils_googlesheet as ugs
import warnings
warnings.filterwarnings('ignore')

class Crawling:
    
    def __init__(self):
        self.json_file_google = "python-sheet-automation.json"
        self.spreadsheet_url = "https://docs.google.com/spreadsheets/d/1immxcyuiJ2y_aHl4fkNwzHR0UJV8_6jKa8Tmrr1PQkM/edit#gid="
        
    def get_targetdata(self, sheet_name):
        gs = ugs.GoogleSheet()
        get_sheet = gs.get_googlesheet(sheet_name, self.spreadsheet_url)
        return get_sheet

class SubTools:
    def xyconversion(self, df):
        df = df.rename(columns={df.columns[0]: '구분'})
        df_transformed = pd.melt(df, id_vars=['구분'], var_name='월별', value_name='출고건수')
        df_transformed['월'] = df_transformed['월별'].apply(lambda x: datetime.strptime(f'2024-{x.split("월")[0].strip()}-01', '%Y-%m-%d').strftime('%Y-%m-%d'))
        return df_transformed
        
    def get_thisyear_datelist(self):
        from datetime import datetime, timedelta
        
        # 시작 날짜와 끝 날짜 설정
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 1)
        
        # 날짜 리스트 초기화
        date_list = []
        
        # 시작 날짜부터 끝 날짜까지 날짜 생성
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # 생성된 날짜 리스트 출력
        #print(date_list)
        
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        # 시작 날짜 설정 (2024년 1월 1일)
        start_date = datetime(2024, 1, 1)
        
        # 종료 날짜 설정 (2024년 12월 1일)
        end_date = datetime(2024, 12, 1)
        
        # 날짜 리스트 초기화
        date_list = []
        
        # 시작 날짜부터 종료 날짜까지 월 단위로 날짜 생성
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += relativedelta(months=1)  # 한 달씩 증가
        
        # 생성된 날짜 리스트
        #print(date_list)
    
        return date_list
    
    def extrapolate_missing_values(self, df_origin, extrapolation_dates):
        df = df_origin.copy()
        # 모든 날짜 범위 생성
        all_dates = pd.date_range(start=df['date'].min(), end=df['date'].max())
        
        # 날짜에 따른 count 값 매핑
        date_to_count = dict(zip(df['date'], df['count']))
        
        # 외삽을 위한 선형 회귀 모델 학습
        model = LinearRegression()
        
        # 'date' 컬럼을 DateTime 형식으로 변환
        df['date'] = pd.to_datetime(df['date'])
        
        # 날짜를 정수형 Unix 타임스탬프로 변환하여 모델 학습
        X = df['date'].apply(lambda x: x.timestamp()).values.reshape(-1, 1)  # Unix 타임스탬프로 변환 (초 단위)
        y = df['count'].values
        model.fit(X, y)
        
        # 외삽을 통한 누락된 값 예측
        missing_dates = [pd.to_datetime(date) for date in extrapolation_dates]
        for date in missing_dates:
            if date not in date_to_count:
                date_int = date.timestamp()  # Unix 타임스탬프로 변환 (초 단위)
                date_int = np.array(date_int).reshape(1, -1)
                predicted_count = model.predict(date_int)[0]
                df.loc[len(df)] = {'date': date, 'count': predicted_count}
        
        # 날짜에 따라 정렬
        df.sort_values(by='date', inplace=True)
        
        return df_origin, df  # df와 df_extrapolated 모두 반환

    def generate_date_range(self, start_year, num_years):
        # 시작 연도의 다음 해 1월 1일
        start_date = datetime(2022, 1, 1)
    
        # 현재 날짜를 가져와서 3년 후의 연도를 계산
        current_date = datetime.today()
        end_year = current_date.year + 3
        # 종료 날짜 설정 (현재 날짜에서 3년 후의 연도의 마지막 달로 설정)
        end_date = datetime(end_year, 12, 1)   
        
        # 날짜 리스트 초기화
        date_list = []
        
        # 시작 날짜부터 종료 날짜까지 월별 날짜 생성
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            # 다음 달 계산
            year = current_date.year + (current_date.month // 12)
            month = ((current_date.month % 12) + 1) if (current_date.month % 12) < 12 else 1
            current_date = datetime(year, month, 1)
        
        return date_list