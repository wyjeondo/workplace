import time
import datetime 
start = time.time()
time.sleep(1)

import GetProcessData
import InsertDB
from datetime import datetime, timedelta

if __name__ == '__main__':
    print('업데이트를 시작합니다.')
    e_date = datetime.today()
    s_date = e_date - timedelta(days=1)
    e_date = e_date.strftime("%Y-%m-%d")
    s_date = s_date.strftime("%Y-%m-%d")
    #'''
    print(f'물동량 업데이트 일자 : {s_date}, {e_date}')
    getdef = GetProcessData.GetProcessData(s_date, e_date)
    master = getdef.get_b2crelease()
    table_name = 'release_information'
    db = InsertDB.INSERT()  
    db.insert_data(master, table_name)
    #'''
    print(f'주문량 업데이트 일자 : {s_date}, {e_date}')
    getdef = GetProcessData.GetProcessData(s_date, e_date)
    master_or = getdef.get_b2corder()
    table_name = 'orders_information'
    db = InsertDB.INSERT()  
    db.insert_data_od(master_or, table_name)
    print('업데이트를 종료합니다.')
    #'''
import datetime
sec = time.time()-start
time_1 = str(datetime.timedelta(seconds=sec)).split(".")[0]
print(f"데이터호출 시간 : {time_1} sec")
