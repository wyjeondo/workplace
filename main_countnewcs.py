import InsertDB
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

def prc_datacus(data, cus_ls):
    #data = data[data['시작출고월']==value]
    new_customers_release = data[data['고객사명'].isin(cus_ls)]
    new_customers_release = new_customers_release.groupby('고객사명')['마감송장건수'].sum().reset_index()    
    return new_customers_release

if __name__ == '__main__':
    print('16개월 동안의 각 월별 신규고객 물동량 확인을 시작합니다.')
    db = InsertDB.INSERT() 
    
    print('16개월 전의 날짜 계산을 시작합니다.')
    # 현재 날짜를 가져옵니다.
    today = datetime.now()
    
    # 16개월 전의 날짜 계산
    #start_date = today - relativedelta(months=29)
    
    #print(f'현재 날짜: {today.strftime("%Y-%m-%d")}')
    #print(f'16개월 전의 날짜: {start_date.strftime("%Y-%m-%d")}')
    
    # 'yyyy-mm' 형식의 문자열로 변환
    #start_date_str = start_date.strftime('%Y-%m')
    #today_str = today.strftime('%Y-%m')
    
    # 쿼리문으로 데이터베이스에서 데이터를 가져옵니다.
    query = f"SELECT * FROM release_information" 
    # WHERE 마감일자 >= '{start_date_str}-01' AND 마감일자 <= '{today_str}-31';"
    df_release_info = db.read_data(query)
    
    df = df_release_info.copy()
    # 처리과정
    df['시작출고일'] = pd.to_datetime(df['출고일자'])  # 날짜 형식으로 변환
    # 시작출고일에서 '%Y%m' 형식의 연월 추출
    df['출고년월'] = df['시작출고일'].dt.strftime('%Y%m')
    # 코호트 분석을 위해 고객사명과 출고년월로 그룹화하여 고유한 인덱스 할당
    df['idx'] = df.groupby(['고객사명', '시작출고일']).ngroup() + 1    
    # 신규 고객사 추출
    new_customers = df.groupby('고객사명')['시작출고일'].min().reset_index()
    df_sorted = new_customers.sort_values(by='시작출고일')
    # "시작출고일" 열을 datetime 형식으로 변환
    df_sorted['시작출고일'] = pd.to_datetime(df_sorted['시작출고일'])

    # 시작출고일 만들기
    dict_newdate = dict(zip(df_sorted['고객사명'].values,df_sorted['시작출고일'].values)) 
    df_release_info['시작출고일'] = df_release_info['고객사명'].map(dict_newdate)
    cus_release_info = df_release_info[['출고일자', '고객사명', '마감송장건수', '시작출고일']]

    # 최종결과 만들기
    cus_release_info['d+30'] = cus_release_info['시작출고일'] + pd.DateOffset(days=30)
    # "시작출고일" 열을 '%Y%m' 형식의 문자열로 변환 : groupby 합계를 위해서.
    cus_release_info['시작출고월'] = cus_release_info['시작출고일'].dt.strftime('%Y%m')
    filtered_df = cus_release_info[(cus_release_info['출고일자'] >= cus_release_info['시작출고일']) 
    & (cus_release_info['출고일자'] <= cus_release_info['d+30'])]
    result = filtered_df.groupby(['고객사명', '시작출고월'])['마감송장건수'].sum().reset_index()
    
    # 결과 입력
    table_name = 'release_of_new_partners_b'
    db.insert_data_pb(result, table_name)