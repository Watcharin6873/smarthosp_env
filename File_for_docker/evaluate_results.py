import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import pytz

tz_bangkok = pytz.timezone('Asia/Bangkok')

curr_datetime = datetime.datetime.now(tz_bangkok).strftime("%Y-%m-%d %H:%M:%S")
print(curr_datetime)

query = """
    SELECT 
        t1.category_questId,t1.hcode,
        SUM(t2.sub_quest_total_point) AS sub_quest_total_point,
        SUM(t2.sub_quest_require_point) AS sub_quest_require_point,
        t1.ssj_approve,t1.zone_approve,t3.cyber_level,t3.cyber_levelname 
    FROM (SELECT t.id,t.category_questId,t.questId,t.sub_questId,t.hcode,t.userId,t.ssj_approve,t.zone_approve,
    SUBSTRING_INDEX(SUBSTRING_INDEX(t.check, ',', n.n), ',', -1) AS c_check FROM Evaluate t CROSS JOIN (SELECT a.N + b.N * 10 + 1 n FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) a ,(SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) b ORDER BY n ) n WHERE n.n <= 1 + (LENGTH(t.check) - LENGTH(REPLACE(t.check, ',', ''))) ORDER BY id,c_check) AS t1 
    LEFT JOIN Sub_quest_list AS t2 ON t1.sub_questId = t2.sub_questId AND t1.c_check = t2.choice 
    LEFT JOIN Cyber_risk_level AS t3 ON t1.hcode = t3.hcode 
    WHERE t1.t1.ssj_approve = TRUE
    GROUP BY t1.category_questId,t1.hcode,t3.cyber_level, t3.cyber_levelname,t1.ssj_approve,t1.zone_approve
    ORDER BY t1.category_questId ASC
"""

conn_str = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine = sa.create_engine(conn_str)
conn = engine.connect()
results = pd.read_sql(query, conn)
conn.close()


results['ssj_approve'] = results['ssj_approve'].astype('boolean')
results['zone_approve'] = results['zone_approve'].astype('boolean')


results['updatedAt'] = curr_datetime


conn_str2 = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine2 = sa.create_engine(conn_str2)
conn2 = engine2.connect()
results.to_sql("Sum_approve_evaluate", conn2, index=None, if_exists="replace")
conn2.close()


print("status: insert data ok!")

