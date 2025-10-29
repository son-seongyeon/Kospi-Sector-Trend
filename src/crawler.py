import requests
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import os

# # For crawling data within the specified date range
# start_date = datetime(2025, 1, 1)
# end_date = datetime(2025, 10, 29)
# date_list = [(start_date + timedelta(days=x)).strftime('%Y%m%d') 
#              for x in range((end_date - start_date).days + 1)]

today = datetime.today().strftime('%Y%m%d')
date_list = [today]

all_data = []
url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020505",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
}

for date in date_list:
    params = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
        "mktId": "STK", # KOSPI
        "trdDd": date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false"
    }
    response = requests.post(url, data=params, headers=headers)
    data = response.json()

    # 업종별 시가총액 추출
    temp_df = pd.DataFrame(data['block1'])

    # 휴장일 데이터 제거 ('-' 포함된 행)
    temp_df = temp_df[~temp_df['MKTCAP'].str.contains('-')]

    # 영업일 데이터만 처리
    if not temp_df.empty:
        temp_df['MKTCAP'] = temp_df['MKTCAP'].str.replace(',', '').astype('Int64')
        temp_df['DATE'] = date
        all_data.append(temp_df)

# 수집된 데이터를 하나의 DataFrame으로 합치기
full_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# 업종별 시가총액 계산
if not full_df.empty:
    sector_df = full_df.groupby(['DATE', 'IDX_IND_NM'])['MKTCAP'].sum().reset_index()
    sector_df = sector_df.sort_values(['DATE', 'MKTCAP'], ascending=[True, False])

# # 데이터베이스에 연결
# db = pymysql.connect(
#     host = 'localhost',
#     port = 3306,
#     user = 'root',
#     passwd = 'Psw98!!2',
#     db = 'kospi_sector_trend'
# )
# cursor = db.cursor()

# # 데이터베이스에 삽입
# for _, row in sector_df.iterrows():
#     sql = '''
#     INSERT INTO KRX_sector_mktcap (date, sector_name, market_cap)
#     VALUES (%s, %s, %s)
#     ON DUPLICATE KEY UPDATE market_cap = VALUES(market_cap)
#     '''
#     cursor.execute(sql, (row['DATE'], row['IDX_IND_NM'], row['MKTCAP']))

# db.commit()
# db.close()

excel_file = 'data/KRX_sector_mktcap.xlsx'

if not full_df.empty:
    if os.path.exists(excel_file):
        existing_data = pd.read_excel(excel_file)
        combined_data = pd.concat([existing_data, full_df], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=['DATE', 'IDX_IND_NM'])
    else:
        combined_data = full_df
    combined_data.to_excel(excel_file, index=False)
    print(f"{len(sector_df)} rows appended to {excel_file}")
else:
    print("No data to append today.")

# Excel로 저장 (기존 파일 덮어쓰기)
combined_data.to_excel(excel_file, index=False)
print(f"{len(full_df)} rows appended to {excel_file}")