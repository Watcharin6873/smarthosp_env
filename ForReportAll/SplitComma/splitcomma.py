import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import pytz

tz_bangkok = pytz.timezone('Asia/Bangkok')

curr_datetime = datetime.datetime.now(tz_bangkok).strftime("%Y-%m-%d %H:%M:%S")
print(curr_datetime)


query = """SELECT h.zone,h.provcode,h.provname,t.hcode,h.hname_th,t.category_questId,t.questId,t.sub_questId,
	SUBSTR(sq.sub_quest_name,1,6) AS sub_quest_name,
	SUBSTRING_INDEX(SUBSTRING_INDEX(t.check, ',', n.n), ',', -1) AS c_check 
FROM Evaluate t 
CROSS JOIN (SELECT a.N + b.N * 10 + 1 n FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) a ,
(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) b ORDER BY n ) n 
INNER JOIN Hospitals AS h ON t.hcode = h.hcode
INNER JOIN Sub_quest AS sq ON t.sub_questId = sq.id
WHERE n.n <= 1 + (LENGTH(t.check) - LENGTH(REPLACE(t.check, ',', ''))) AND h.typename IN ('โรงพยาบาลศูนย์','โรงพยาบาลทั่วไป','โรงพยาบาลชุมชน')
ORDER BY h.zone,h.provcode,t.category_questId,t.questId ASC"""


conn_str = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine = sa.create_engine(conn_str)
conn = engine.connect()
results = pd.read_sql(query, conn)
conn.close()


results['sub_quest_name'] = results['sub_quest_name'].str.rstrip(')')

results['updatedAt'] = curr_datetime


conn_str2 = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine2 = sa.create_engine(conn_str2)
conn2 = engine2.connect()
results.to_sql("For_report_all", conn2, index=None, if_exists="replace")
conn2.close()

print("status: insert data ok!")