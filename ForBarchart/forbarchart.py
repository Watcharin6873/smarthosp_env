import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import pytz

tz_bangkok = pytz.timezone('Asia/Bangkok')

curr_datetime = datetime.datetime.now(tz_bangkok).strftime("%Y-%m-%d %H:%M:%S")
print(curr_datetime)


query = """
    SELECT tb1.zone,tb1.provcode,tb1.provname,
        SUM(CASE WHEN (tb1.sub_quest_total_point >= 800 AND tb1.sub_quest_require_point = 510 AND tb1.cyber_level = 'GREEN') THEN 1 ELSE 0 END) AS gemlevel,
        SUM(CASE WHEN (tb1.sub_quest_total_point >= 700 AND tb1.sub_quest_total_point < 800 AND tb1.sub_quest_require_point = 510) OR (tb1.sub_quest_total_point >= 800 AND tb1.sub_quest_require_point = 510 AND tb1.cyber_level != 'GREEN') THEN 1 ELSE 0  END) AS goldlevel,
        SUM(CASE WHEN (tb1.sub_quest_total_point >= 600 AND tb1.sub_quest_total_point < 700) OR (tb1.sub_quest_total_point >= 700 AND tb1.sub_quest_total_point < 800 AND tb1.sub_quest_require_point < 510) OR (tb1.sub_quest_total_point >= 800 AND tb1.sub_quest_require_point < 510) THEN 1 ELSE 0  END) AS silverlevel,
        SUM(CASE WHEN (tb1.sub_quest_total_point < 600 && tb1.sub_quest_require_point < 510) THEN 1 ELSE 0  END) AS notpasslevel,
        COUNT(DISTINCT tb1.hcode) AS total_hosp
    FROM (SELECT t2.zone,t2.provcode,t2.provname,t1.hcode,t2.hname_th,SUM(sub_quest_total_point) AS sub_quest_total_point,
                SUM(sub_quest_require_point) AS sub_quest_require_point,t1.cyber_level, t1.cyber_levelname
          FROM Hospitals AS t2 LEFT JOIN Sum_approve_evaluate AS t1 ON t1.hcode = t2.hcode COLLATE utf8mb4_unicode_ci
    WHERE t2.typename IN ('โรงพยาบาลศูนย์','โรงพยาบาลทั่วไป','โรงพยาบาลชุมชน')
    GROUP BY t2.zone,t2.provcode,t2.provname,t1.hcode,t2.hname_th,t1.cyber_level, t1.cyber_levelname
    ORDER BY CAST(t2.zone AS UNSIGNED), t2.provcode ASC) AS tb1
    GROUP BY tb1.zone, tb1.provcode,tb1.provname ORDER BY CAST(tb1.provcode AS UNSIGNED) ASC
"""


conn_str = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine = sa.create_engine(conn_str)
conn = engine.connect()
results = pd.read_sql(query, conn)
conn.close()

results['gemlevel'] = results['gemlevel'].astype('Int32')
results['goldlevel'] = results['goldlevel'].astype('Int32')
results['silverlevel'] = results['silverlevel'].astype('Int32')
results['notpasslevel'] = results['notpasslevel'].astype('Int32')
results['total_hosp'] = results['total_hosp'].astype('Int32')

results['updatedAt'] = curr_datetime


results2 = results[['zone','provcode', 'provname', 'gemlevel', 'goldlevel', 'silverlevel','notpasslevel', 'total_hosp', 'updatedAt']]


conn_str2 = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine2 = sa.create_engine(conn_str2)
conn2 = engine2.connect()
results2.to_sql("Result_evaluate_forchart", conn2, index=None, if_exists="replace")
conn2.close()


print("status: insert data ok!")