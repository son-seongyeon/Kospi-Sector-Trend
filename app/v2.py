import streamlit as st
from datetime import datetime
import pandas as pd
import numpy as np
from io import BytesIO
import plotly.express as px

df = pd.read_csv("../data/KRX_sector_mktcap.csv")

st.set_page_config(layout="wide")

# 제목
st.title("업종별 시가총액 분석")
st.text("")
st.text("")
st.text("")

# Sidebar
period_unit = st.sidebar.selectbox(
    "기간 단위",
    options=["년", "월", "주", "일"]
)

# 'DATE' 행 datetime으로 변환
df['DATE'] = df['DATE'].astype(str)
df['DATE'] = pd.to_datetime(df['DATE'], format='%Y%m%d', errors='coerce')

default_start_date = datetime(2020, 1, 2)
default_end_date = df['DATE'].max()

selected_range = st.sidebar.date_input(
    "기간",
    [default_start_date, default_end_date]
)

# Warning
selected_start_date = pd.to_datetime(selected_range[0])
selected_end_date = pd.to_datetime(selected_range[1])

if (
    selected_start_date not in df['DATE'].values 
    or selected_end_date not in df['DATE'].values
):
    st.sidebar.warning(
        "⚠️ 선택하신 기간의 시작일 또는 종료일은 거래일이 아닙니다. 유효한 영업일 범위로 기간을 다시 설정해 주세요."
    )

sector_options = ["전체"] + sorted(df['IDX_IND_NM'].dropna().unique().tolist())
sector = st.sidebar.selectbox(
    "업종",
    options = sector_options,
    index=0   # '전체'을 기본값으로 설정
)

# 데이터프레임 필터링
if sector == "전체":
    filtered_df = df[
        (df['DATE'] >= selected_start_date) &
        (df['DATE'] <= selected_end_date)
    ]
    disparity_df = df.copy()
else:
    filtered_df = df[
        (df['DATE'] >= selected_start_date) &
        (df['DATE'] <= selected_end_date) &
        (df['IDX_IND_NM'] == sector)
    ]
    disparity_df = df[df['IDX_IND_NM'] == sector]

# 헤더 이름 변환
filtered_df = filtered_df.rename(columns={
    "DATE": "DATE",
    "IDX_IND_NM": "SECTOR",
    "MARKET_CAP": "MKTCAP"
})

disparity_df = disparity_df.rename(columns={
    "DATE": "DATE",
    "IDX_IND_NM": "SECTOR",
    "MARKET_CAP": "MKTCAP"
})

# 'Export to Excel' 버튼
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="KRX_sector_mktcap")
    processed_data = output.getvalue()
    return processed_data

excel_data = to_excel(filtered_df)

st.sidebar.download_button(
    label = "Export to Excel",
    data = excel_data,
    file_name = "KRX_sector_mktcap.xlsx",
    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# 기간 단위 행 생성
filtered_df['YEAR'] = filtered_df['DATE'].dt.year
filtered_df['MONTH'] = filtered_df['DATE'].dt.to_period('M')
filtered_df['WEEK'] = filtered_df['DATE'].dt.to_period('W')

# 기간 단위에 따라 집계
if period_unit == "년":
    agg_df = filtered_df.groupby(['YEAR', 'SECTOR'])['MKTCAP'].sum().reset_index()
elif period_unit == "월":
    agg_df = filtered_df.groupby(['MONTH', 'SECTOR'])['MKTCAP'].sum().reset_index()
elif period_unit == "주":
    agg_df = filtered_df.groupby(['WEEK', 'SECTOR'])['MKTCAP'].sum().reset_index()
else:   # '일' 단위
    agg_df = filtered_df.groupby(['DATE', 'SECTOR'])['MKTCAP'].sum().reset_index()
    agg_df['DATE'] = agg_df['DATE'].dt.strftime('%Y-%m-%d')

# 이격도를 위한 데이터프레임
disparity_df['RATE OF CHANGE'] = round((disparity_df['MKTCAP'] - disparity_df['MKTCAP'].shift(1)) / disparity_df['MKTCAP'].shift(1) * 100)

agg_df['RATE OF CHANGE'] = round((agg_df['MKTCAP'] - agg_df['MKTCAP'].shift(1)) / agg_df['MKTCAP'].shift(1) * 100)
agg_df['MKTCAP'] = (agg_df['MKTCAP'] / 1e8).astype(int)



# '전체' 업종 선택 시
if (
    sector == "전체"
    and selected_start_date in df['DATE'].values
    and selected_end_date in df['DATE'].values
):
    start_mktcap = filtered_df[filtered_df['DATE'] == selected_start_date]['MKTCAP'].sum()
    end_mktcap = filtered_df[filtered_df['DATE'] == selected_end_date]['MKTCAP'].sum()
    e2e_vol = round((end_mktcap-start_mktcap)/start_mktcap*100,2)
    e2e_vol_color = "red" if e2e_vol > 0 else "blue"

    # Summary
    st.markdown(
        f"""
        <div style="
            font-size:30px;
        ">
            <b>{sector}</b> 시가총액은 
            <b>{selected_start_date.strftime('%Y-%m-%d')}</b>부터 
            <b>{selected_end_date.strftime('%Y-%m-%d')}</b>까지 
            <span style="color:{e2e_vol_color}; font-weight:bold;">{round(e2e_vol,2)}%</span> 변동했습니다.
        </div>
        """,
        unsafe_allow_html=True
    )
    st.text("")
    st.text("")
    st.text("")

    # 시가총액 상승률 Top 5 업종
    start_sec_mktcap = (
        filtered_df[filtered_df['DATE'] == selected_start_date]
        .groupby('SECTOR')['MKTCAP']
        .sum()
    )
    end_sec_mktcap = (
        filtered_df[filtered_df['DATE'] == selected_end_date]
        .groupby('SECTOR')['MKTCAP']
        .sum()
    )
    e2e_sec_vol = round(((end_sec_mktcap-start_sec_mktcap)/start_sec_mktcap*100),2).reset_index()
    e2e_sec_vol.columns = ['업종명', '변동률 (%)']
    agg_df_top5 = e2e_sec_vol.sort_values(by='변동률 (%)', ascending=False).head(5)
    agg_df_top5.index = range(1, len(agg_df_top5) + 1)
    agg_df_top5.index.name = '순위'

    st.markdown("### 시가총액 변동률 Top 5 업종")
    st.dataframe(agg_df_top5)

    # 트리맵
    top16_text = e2e_sec_vol.nlargest(16, '변동률 (%)')['업종명']
    e2e_sec_vol['top16_text'] = e2e_sec_vol.apply(
        lambda x: f"{x['변동률 (%)']}%" if x['업종명'] in top16_text.values else '',
        axis=1
    )

    fig = px.treemap(
        e2e_sec_vol,
        path=['업종명'],
        values='변동률 (%)',
        color_discrete_sequence=['rgba(0,0,255,0.2)']
    )

    fig.update_traces(
        text=e2e_sec_vol['top16_text'],
        textposition='middle center',
        textfont_size=14,
        hovertemplate = '<b>업종명: </b> %{label}<br><b>변동률 (%): </b> %{value}%<extra></extra>'
    )

    st.plotly_chart(fig, use_container_width=True)

elif (
    selected_start_date in df['DATE'].values
    and selected_end_date in df['DATE'].values
):
    agg_df = agg_df.rename(columns={
        'YEAR': '기간단위',
        'MONTH': '기간단위',
        'WEEK': '기간단위',
        'DATE': '기간단위',
        'SECTOR': '업종명',
        'MKTCAP' : '시가총액 (억)',
        'RATE OF CHANGE': '변동률 (%)'
    })
    agg_df = agg_df.reindex(columns=['업종명', '기간단위', '시가총액 (억)', '변동률 (%)'])
    st.markdown(f"### {sector} 업종 시가총액 변동성 추이")
    if agg_df.empty:
        st.markdown("선택하신 기간에 해당하는 데이터가 없습니다.")
    else:
        st.dataframe(agg_df)
    st.text("")
    st.text("")
    st.text("")
    st.markdown(f"### {sector} 업종 이격도 추이")

    # 이동평균선
    disparity_df['MA200'] = disparity_df['MKTCAP'].rolling(window=200).mean()
    # 이격도 (%) = (종가 - MA200) / MA200 * 100
    disparity_df['이격도 %'] = round((disparity_df['MKTCAP'] - disparity_df['MA200']) / disparity_df['MA200'] * 100, 2)
    disparity_df = disparity_df[
        (df['DATE'] >= selected_start_date) &
        (df['DATE'] <= selected_end_date)]

    # '일' 기간단위 이격도
    fig = px.line(
        disparity_df,
        x='DATE',
        y='RATE OF CHANGE',
        markers=False,
        labels={'RATE OF CHANGE': '변동률 (%)'}
    )

    fig.add_scatter(
        x=disparity_df['DATE'],
        y=disparity_df['이격도 %'],
        mode='lines',
        name='이격도 (%)',
        line=dict(color='red', width=2),
        hovertemplate='<b>날짜: </b> %{x}<br><b>이격도: </b> %{y} %<extra></extra>'
    )
    fig.data[0].hovertemplate = '<b>날짜: </b> %{x}<br><b>변동률: </b> %{y} %<extra></extra>'

    fig.update_layout(
        xaxis_title='날짜',
        yaxis_title='변동률 (%)'
    )

    st.plotly_chart(fig, use_container_width=True)

