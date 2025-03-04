import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import pytz

tz_bangkok = pytz.timezone('Asia/Bangkok')

curr_datetime = datetime.datetime.now(tz_bangkok).strftime("%Y-%m-%d %H:%M:%S")
print(curr_datetime)

query = """
    SELECT tb1.zone,tb1.provcode,tb1.provname,tb1.hcode,tb1.hname_th,tb1.typename,
	SUM(CASE WHEN tb1.category_questId = 1 THEN tb1.sub_quest_total_point END) AS point_total_cat1,
    SUM(CASE WHEN tb1.category_questId = 1 THEN tb1.sub_quest_require_point END) AS point_require_cat1,	
    SUM(CASE WHEN tb1.category_questId = 2 THEN tb1.sub_quest_total_point END) AS point_total_cat2,
    SUM(CASE WHEN tb1.category_questId = 2 THEN tb1.sub_quest_require_point END) AS point_require_cat2,	
    SUM(CASE WHEN tb1.category_questId = 3 THEN tb1.sub_quest_total_point END) AS point_total_cat3,
    SUM(CASE WHEN tb1.category_questId = 3 THEN tb1.sub_quest_require_point END) AS point_require_cat3,	
    SUM(CASE WHEN tb1.category_questId = 4 THEN tb1.sub_quest_total_point END) AS point_total_cat4,
        tb1.cyber_level, tb1.cyber_levelname
    FROM (SELECT a.zone,a.provcode,a.provname,a.hcode,a.hname_th,a.typename,
                c.category_questId,c.sub_quest_total_point,c.sub_quest_require_point,
                b.cyber_level, b.cyber_levelname
    FROM Hospitals AS a 
    LEFT JOIN Cyber_risk_level AS b 
    ON a.hcode = b.hcode
    LEFT JOIN Sum_approve_evaluate AS c 
    ON a.hcode COLLATE utf8mb4_unicode_ci = c.hcode
    WHERE a.typename IN ('โรงพยาบาลศูนย์', 'โรงพยาบาลทั่วไป', 'โรงพยาบาลชุมชน')) tb1
    GROUP BY tb1.zone,tb1.provcode,tb1.provname,tb1.hcode,tb1.hname_th,tb1.typename,tb1.cyber_level, tb1.cyber_levelname
    ORDER BY CAST(tb1.zone AS UNSIGNED), tb1.provcode ASC"""


conn_str = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine = sa.create_engine(conn_str)
conn = engine.connect()
results = pd.read_sql(query, conn)
conn.close()

results['updatedAt'] = curr_datetime

conn_str2 = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine2 = sa.create_engine(conn_str2)
conn2 = engine2.connect()
results.to_sql("Report_evaluate_all", conn2, index=None, if_exists="replace")
conn2.close()

print("status: insert data ok!")