import requests
import pandas as pd
from datetime import datetime, timedelta
import os

today = datetime.today().strftime('%Y%m%d')

all_data = []
url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020505",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
}

params = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
    "mktId": "STK", # KOSPI
    "trdDd": today,
    "share": "1",
    "money": "1",
    "csvxls_isNo": "false"
}
response = requests.post(url, data=params, headers=headers)
data = response.json()

temp_df = pd.DataFrame(data['block1'])
temp_df = temp_df[["ISU_ABBRV", "IDX_IND_NM", "MKTCAP"]]
temp_df = temp_df.sort_values(by="MKTCAP", ascending=False)
df = temp_df.rename(columns={
    "ISU_ABBRV": "종목명",
    "IDX_IND_NM": "업종명",
    "MKTCAP": "시가총액"
})

file_path = os.path.join(os.path.dirname(__file__), '../data/KRX_sector_company.csv')

try:
    existing_df = pd.read_csv(file_path)
except FileNotFoundError:
    existing_df = pd.DataFrame()

combined_df = pd.concat([existing_df, df], ignore_index=True)
combined_df.to_csv(file_path, index=False, encoding='utf-8-sig')