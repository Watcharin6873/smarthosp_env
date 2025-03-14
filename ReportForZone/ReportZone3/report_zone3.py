import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import pytz
import os
import openpyxl

tz_bangkok = pytz.timezone('Asia/Bangkok')

curr_datetime = datetime.datetime.now(tz_bangkok).strftime("%Y-%m-%d %H:%M:%S")
print(curr_datetime)


query = """SELECT zone,provcode,provname,hcode,hname_th,category_questId,questId,sub_questId,
           sub_quest_name,c_check FROM For_report_all WHERE zone='03'
        """

conn_str = f"mysql+pymysql://bdh:{quote('P@ssword1234')}@127.0.0.1:3306/smarthosp_quest"
engine = sa.create_engine(conn_str)
conn = engine.connect()
results = pd.read_sql(query, conn)
conn.close()

results.duplicated().sum()

results2 = results[~results.index.duplicated()]


data = results2.pivot_table(index=['zone', 'provcode', 'provname', 'hcode', 'hname_th'], columns='sub_quest_name',values='c_check', aggfunc='sum')


data2 = data.reset_index()

data2["วันที่ประมวลผล"] = curr_datetime

new_data = data2.rename(columns={"zone":"เขตสุขภาพ","provcode":"รหัสจังหวัด","provname":"ชื่อจังหวัด","hcode":"รหัสหน่วยบริการ","hname_th":"ชื่อหน่วยบริการ"})



field_name = ['2.1.3.3','2.1.4.3','2.1.5.5', '2.2.1.1', '2.4.1.4', '3.3.1.3', '3.4.1.2', '3.4.1.5']



for col in field_name:
    if col not in new_data.columns:
        new_data[col] = ''



data3 = new_data[['เขตสุขภาพ', 'รหัสจังหวัด', 'ชื่อจังหวัด', 'รหัสหน่วยบริการ', 'ชื่อหน่วยบริการ',
'1.1.1','1.1.2','1.1.3','1.1.4','1.1.5','1.1.6','1.1.7','1.1.8','1.1.9','1.1.10','1.1.11','1.1.12','1.1.13','1.1.14','1.1.15',
'1.2.1','1.2.2','1.2.3','1.2.4','1.2.5','1.2.6','1.2.7','1.2.8','1.2.9','1.2.10','1.2.11','1.2.12',
'1.3.1','1.3.2','1.3.3','1.3.4','1.3.5','1.3.6','1.4.1','1.4.2','1.5.1','1.5.2','1.5.3','1.5.4','1.5.5','1.5.6','1.5.7','1.5.8','1.5.9','1.5.10','1.5.11',
'1.5.12','1.5.13','1.5.14','1.5.15','1.5.16','1.5.17','1.6.1','1.6.2','1.6.3','1.6.4','1.6.5','1.6.6','1.6.7','1.7.1','1.7.2','1.7.3',
'1.8.1','1.8.2','1.8.3','1.8.4','1.8.5',
'2.1.1','2.1.2',
'2.1.3.1','2.1.3.2','2.1.3.3','2.1.4.1','2.1.4.2','2.1.4.3',
'2.1.5.1','2.1.5.2','2.1.5.3','2.1.5.4','2.1.5.5',
'2.2.1.1','2.2.1.2','2.2.1.3',
'2.2.2.1','2.2.2.2','2.2.2.3',
'2.2.3','2.2.4','2.2.5','2.3.1','2.3.2',
'2.4.1.1','2.4.1.2','2.4.1.3','2.4.1.4',
'2.4.2.1','2.4.2.2','2.4.2.3','2.4.2.4',
'2.4.3','2.4.4','2.5.1','2.5.2','2.5.3','2.5.4','2.6.1','2.6.2','2.6.3','2.6.4','2.7.1',
'2.7.2','2.7.3','2.8.1','2.8.2','2.9.1','2.9.2','2.10.1','2.10.2','2.10.3','2.10.4','2.10.5','2.10.6',
'2.11.1.1','2.11.1.2','2.11.1.3','2.11.1.4',
'2.11.2',
'2.11.3.1','2.11.3.2','2.11.3.3','2.11.3.4',
'2.11.4','2.11.5',
'3.1.1','3.1.2','3.1.3','3.1.4','3.1.5','3.2.1','3.2.2','3.2.3','3.2.4',
'3.3.1.1','3.3.1.2','3.3.1.3',
'3.3.2','3.3.3',
'3.4.1.1','3.4.1.2','3.4.1.3','3.4.1.4','3.4.1.5',
'3.4.2','3.4.3',
'3.5.1.1','3.5.1.2','3.5.1.3','3.5.1.4',
'3.5.2','3.5.3','3.5.4','3.5.5',
'3.6.1','3.6.2','3.6.3','3.6.4','3.6.5',
'3.7.1','3.7.2','3.7.3','3.7.4','3.7.5','3.7.6','3.7.7','3.8.1',
'3.8.2.1','3.8.2.2','3.8.2.3','3.8.2.4',
'3.8.3','3.8.4','3.8.5','3.8.6','3.9.1','3.9.2',
'3.10.1.1','3.10.1.2',
'3.11.1.1','3.11.1.2','3.11.1.3',
'3.11.2.1','3.11.2.2','3.11.2.3',
'3.11.3','4.1.1','4.2.1','4.2.2','4.2.3','4.2.4','4.2.5','4.2.6',
'4.2.7','4.2.8','4.3.1','4.3.2','4.3.3','4.4.1','4.4.2','วันที่ประมวลผล']]


directory ='/data'
filename = 'Report_zone3.xlsx'
filepath = os.path.join(directory, filename)

os.makedirs(directory, exist_ok=True)

data3.to_excel(filepath, index=False, engine='openpyxl')

print(f"File saved to: {filepath}")