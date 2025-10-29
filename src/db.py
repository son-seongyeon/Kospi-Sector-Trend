import pymysql

db = pymysql.connect(
    host = 'localhost',
    port = 3306,
    user = 'root',
    passwd = 'Psw98!!2',
    db = 'kospi_sector_trend'
)
cursor = db.cursor()

sql = '''
CREATE TABLE KRX_sector_mktcap (
    date DATE NOT NULL,
    sector_name VARCHAR(100) NOT NULL,
    market_cap BIGINT NOT NULL,
    PRIMARY KEY (date, sector_name)
);
'''
cursor.execute(sql)

db.commit()
db.close()