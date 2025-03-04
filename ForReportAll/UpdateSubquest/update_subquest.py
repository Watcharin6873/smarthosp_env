import pandas as pd
import datetime
import pytz
import mysql.connector


# Establish connection
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="bdh",
    password="P@ssword1234",
    database="smarthosp_quest"
)

cursor = conn.cursor()


update_query = """
UPDATE For_report_all
SET sub_quest_name = CASE
         WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check = 'C1') THEN '2.1.3.1'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check = 'C2') THEN '2.1.3.2'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check IN ('C359','undefined')) THEN '2.1.3.3'									
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check = 'C3') THEN '2.1.4.1'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check = 'C4') THEN '2.1.4.2'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check IN ('C360','undefined')) THEN '2.1.4.3'								
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C5') THEN '2.1.5.1'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C6') THEN '2.1.5.2'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C7') THEN '2.1.5.3'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C8') THEN '2.1.5.4'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check IN ('C354','undefined')) THEN '2.1.5.5'								
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C9') THEN '2.2.1.1'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C10') THEN '2.2.1.2'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C11') THEN '2.2.1.3'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check IN ('C361','undefined')) THEN '2.2.1.4'				
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C12') THEN '2.2.2.1'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C13') THEN '2.2.2.2'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C14') THEN '2.2.2.3'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check IN ('C362','undefined')) THEN '2.2.2.4'				 
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C15') THEN '2.4.1.1'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C16') THEN '2.4.1.2'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C17') THEN '2.4.1.3'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check IN ('C355','undefined')) THEN '2.4.1.4'				 
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C18') THEN '2.4.2.1'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C19') THEN '2.4.2.2'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C20') THEN '2.4.2.3'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check IN ('C356','undefined')) THEN '2.4.2.4'				 
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C21') THEN '2.11.1.1'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C22') THEN '2.11.1.2'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C23') THEN '2.11.1.3'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check IN ('C351','undefined')) THEN '2.11.1.4'				 
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C24') THEN '2.11.3.1'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C25') THEN '2.11.3.2'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C26') THEN '2.11.3.3'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check IN ('C352','undefined')) THEN '2.11.3.4'				 
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check = 'C27') THEN '3.3.1.1'
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check = 'C28') THEN '3.3.1.2'
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check IN ('C358','undefined')) THEN '3.3.1.3'				 
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C29') THEN '3.4.1.1'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C30') THEN '3.4.1.2'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C31') THEN '3.4.1.3'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C32') THEN '3.4.1.4'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check IN ('C357','undefined')) THEN '3.4.1.5'				 
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C33') THEN '3.5.1.1'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C34') THEN '3.5.1.2'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C35') THEN '3.5.1.3'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C36') THEN '3.5.1.4'				 
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C37') THEN '3.8.2.1'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C38') THEN '3.8.2.2'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C39') THEN '3.8.2.3'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check IN ('C353','undefined')) THEN '3.8.2.4'				 
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check = 'C40') THEN '3.10.1.1'
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check = 'C41') THEN '3.10.1.2'
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check IN ('C350','undefined')) THEN '3.10.1.2'				 
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check = 'C42') THEN '3.11.1.1'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check = 'C43') THEN '3.11.1.2'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check IN ('C349','undefined')) THEN '3.11.1.3'				 
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check = 'C44') THEN '3.11.2.1'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check = 'C45') THEN '3.11.2.2'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check IN ('C348','undefined')) THEN '3.11.2.3'									 
         ELSE sub_quest_name
       END
"""

cursor.execute(update_query)
conn.commit()

print(f"{cursor.rowcount} record(s) updated")

cursor.close()
conn.close()