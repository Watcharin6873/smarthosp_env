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
SET c_check = CASE
         WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check = 'C1') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check = 'C2') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 70 && c_check IN ('C359','undefined')) THEN 'false'									
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check = 'C3') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check = 'C4') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 73 && c_check IN ('C360','undefined')) THEN 'false'									
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C5') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C6') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C7') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check = 'C8') THEN 'true'
				 WHEN (category_questId = 2 && questId = 9 && sub_questId = 76 && c_check IN ('C354','undefined')) THEN 'false'									
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C9') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C10') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check = 'C11') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 81 && c_check IN ('C361','undefined')) THEN 'false'				
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C12') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C13') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check = 'C14') THEN 'true'
				 WHEN (category_questId = 2 && questId = 10 && sub_questId = 85 && c_check IN ('C362','undefined')) THEN 'false'				 
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C15') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C16') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check = 'C17') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 94 && c_check IN ('C355','undefined')) THEN 'false'				 
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C18') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C19') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check = 'C20') THEN 'true'
				 WHEN (category_questId = 2 && questId = 12 && sub_questId = 98 && c_check IN ('C356','undefined')) THEN 'false'				 
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C21') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C22') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check = 'C23') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 125 && c_check IN ('C351','undefined')) THEN 'false'				 
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C24') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C25') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check = 'C26') THEN 'true'
				 WHEN (category_questId = 2 && questId = 19 && sub_questId = 130 && c_check IN ('C352','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check = 'C27') THEN 'true'
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check = 'C28') THEN 'true'
				 WHEN (category_questId = 3 && questId = 22 && sub_questId = 145 && c_check IN ('C358','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C29') THEN 'true'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C30') THEN 'true'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C31') THEN 'true'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check = 'C32') THEN 'true'
				 WHEN (category_questId = 3 && questId = 23 && sub_questId = 150 && c_check IN ('C357','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C33') THEN 'true'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C34') THEN 'true'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C35') THEN 'true'
				 WHEN (category_questId = 3 && questId = 24 && sub_questId = 157 && c_check = 'C36') THEN 'true'				 
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C37') THEN 'true'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C38') THEN 'true'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check = 'C39') THEN 'true'
				 WHEN (category_questId = 3 && questId = 27 && sub_questId = 179 && c_check IN ('C353','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check = 'C40') THEN 'true'
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check = 'C41') THEN 'true'
				 WHEN (category_questId = 3 && questId = 29 && sub_questId = 189 && c_check IN ('C350','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check = 'C42') THEN 'true'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check = 'C43') THEN 'true'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 192 && c_check IN ('C349','undefined')) THEN 'false'				 
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check = 'C44') THEN 'true'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check = 'C45') THEN 'true'
				 WHEN (category_questId = 3 && questId = 30 && sub_questId = 195 && c_check IN ('C348','undefined')) THEN 'false'									 
         ELSE c_check
       END
"""

cursor.execute(update_query)
conn.commit()

print(f"{cursor.rowcount} record(s) updated")

cursor.close()
conn.close()