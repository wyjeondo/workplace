import time
import datetime 
start = time.time()
time.sleep(1)
import pandas as pd
import GetProcessData
import InsertDB
import utils_extrapolation as ue
from datetime import datetime, timedelta


if __name__ == '__main__':
    print('업데이트를 시작합니다.')
    
    # 구글시트 크롤링 = {물동량 목표, 경생사1, 경생사2}
    sheet_name = "물동량 목표"
    crawling = ue.Crawling()
    get_sheet_release = crawling.get_targetdata(sheet_name)
    tool = ue.SubTools()
    master = tool.xyconversion(get_sheet_release)
    df_target = master[['구분','월','출고건수']]
    df_target = df_target[df_target['구분']=='물동량 목표']
    df_target = df_target.rename(columns={'월':'date', '출고건수':'count'})
    df_target = df_target[['date', 'count']]
    target_date = df_target['date'].unique()   

    # 현재 연도를 기준으로 3개년치의 월별 일자 리스트 생성
    current_year = datetime.now().year
    num_years = 3
    tool = ue.SubTools()
    date_range = tool.generate_date_range(current_year, num_years)

    # 외삽법(extrapolation)으로 2022년부터 현재연도기준 next 3년치 값 추출
    tool = ue.SubTools()
    extrapolation_dates = set(date_range).difference(set(target_date))
    extrapolatedate_list = list(extrapolation_dates)
    df_target['date'] = pd.to_datetime(df_target['date'])
    df_target.reset_index(inplace=True)
    df_target = df_target.drop(columns='index')
    df, df_extrapolated = tool.extrapolate_missing_values(df_target, extrapolation_dates)
    df_extrapolated['count'] = df_extrapolated['count'].astype('int64')
    df_extrapolated.reset_index(inplace=True)
    df_extrapolated = df_extrapolated.drop(columns='index')   

    df_extrapolated['구분'] = '목표치(외삽법_포함)'
    df_extrapolated = df_extrapolated.rename(columns={'date':'월', 'count':'출고건수'})

    df_extrapolated['월'] = df_extrapolated['월'].dt.strftime('%Y-%m-%d')
    df_compet = master[['구분','월','출고건수']]
    df_compet = df_compet[~df_compet['구분'].isin(['물동량 목표'])]

    df_master = pd.concat([df_extrapolated,df_compet], axis=0)

    table_name = 'target_extrapolation'
    db = InsertDB.INSERT()  
    db.insert_data_pb(df_master, table_name)
    print('업데이트를 종료합니다.')

import datetime
sec = time.time()-start
time_1 = str(datetime.timedelta(seconds=sec)).split(".")[0]
print(f"데이터호출 시간 : {time_1} sec")
