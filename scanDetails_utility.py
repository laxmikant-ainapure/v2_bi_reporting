import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from BI_utility import get_db_connections
import logging
import mailer
import constant as cs

today=str(datetime.strftime(datetime.today(), '%Y-%m-%d'))

def generate_scan_details_report(bi_connection):
    file_name=None
    try:        
        scan_cursor=bi_connection.cursor()
        pr_cursor=bi_connection.cursor()
        user_cursor=bi_connection.cursor()
        deleted_account_cursor=bi_connection.cursor()
        if scan_cursor is not None:

            scan_cursor.execute('select id as "Repository id", name as "Repository Name",uid as "Repository UID",url as "Repo URL",scan_count as "Scan count", '
            ' status as "Status",last_step as "Current Step",type as "Repository type",scan_date::date as "Date of scan"  '
            ' from (select r.id, r.name,r.uid,(r.repo_meta::json->>'"'url'"')::varchar as url, '
            ' r."type",rsh.status,(case when rsh.status ='"'FAIL'"' then rsh.current_step else '"''"' end)  as last_step, '
            ' rsh.started_on as scan_date, '
            ' row_number() over (PARTITION BY rsh.repository_id order by started_on desc) as rn, '
            ' count(rsh.id) over (PARTITION BY rsh.repository_id) as scan_count '
            ' from  repository r inner join repository_scan_history rsh on r.id=rsh.repository_id )s where rn=1 order by 1;') 

            pr_cursor.execute('select distinct r.id as repo_id,r.name as repo_name,r.uid as repo_uid,(r.repo_meta::json->>'"'url'"')::varchar as repo_url, '
            ' count (rrq.review_request_id) as "Total PRs scanned", '
            ' count(rrq.review_request_id) filter (where status='"'SUCCESS'"') as "SUCCESSFUL", '
            ' count(rrq.review_request_id) filter (where status='"'FAIL'"') as "FAILED", '
            ' count(rrq.review_request_id) filter (where status='"'NO_FILES'"') as "NO FILES" '
            ' from repository r '
            ' inner join review_request rr on r.uid=rr.repository_uid '
            ' inner join review_request_queue rrq on rrq.review_request_id = rr.id  and rr.repository_uid =rrq.repository_uid  '
            ' group by 1,2,3,4;')

            user_cursor.execute('select s.tenant_uid,organization,role_name,email as "Email",Signed_up_date::date as "Sign Up Date",repocount as "Repository Count",loc as "LOC",scancount as "Number Of Scan",replace(array_to_string(array(select distinct unnest (string_to_array(lang,'"','"'))),'"','"'),'"',,'"','"','"') as "Languages" ,account_type as "Type","Last Login Date"::date ,user_status as "User Status",package_name as "Package Name" , license_expiry_date::date as "License Expiry Date",'
            '(case when license_expiry_date>current_date and user_status='"'Deleted'"' then '"'User Deleted'"' '
            'when license_expiry_date>current_date and user_status='"'Active'"' then '"'Subscription will expire in '"'|| DATE_PART('"'day'"',license_expiry_date-current_timestamp)||'"' days '"'||DATE_PART('"'Hours'"',license_expiry_date-current_timestamp)||'"' Hours '"' '
            'when license_expiry_date is null and user_status='"'Deleted'"' then '"'User Deleted'"' '
            'when license_expiry_date is null and user_status='"'Active'"' then '"'Free package subscription'"' '
            'when license_expiry_date<current_date then '"'Expired'"' '
            'else '"'Subscription expired'"' end) as "Subscription details" ' 
            'from (select s.tenant_uid ,organization,email,role_name,array_to_string(array_agg(distinct languages),'"','"') as lang,sum(repocount) as repocount,sum(scancount) as scancount,'
            ' sum(loc) as loc,max(signed_up_date) as signed_up_date,max(account_type)account_type,max(last_login_dt) as "Last Login Date" ,max(user_status) as user_status,max(package_name) as package_name,(license_expiry_date)as license_expiry_date from ( select t.uid as tenant_uid,u.email,lm.value as loc,array_to_string( array_agg(distinct l."name"),'"','"') as languages,max(u.created_dt) as signed_up_date,count(distinct r.id) as repocount, '
            ' count(rsh.id) as scancount ,max(o."type") as account_type,max(ui.last_login_dt) as last_login_dt , (case when max(ou.status) ='"'D'"' then '"'Deleted'"' else '"'Active'"' end) as  user_status,max(o.package_name) as package_name,(o.license_expiry_date) as license_expiry_date ,o.slug as organization ,ou.role_name from repository r '
            ' right join repository_language rl on r.id=rl.repository_id' 
            ' right join "language" l on rl.language_id =l.id'
            ' right join repository_scan_history rsh on r.id=rsh.repository_id '
            ' right join license_metrics lm on r.uid = lm.ident and  metric ='"'loc'"''   
            ' right join organization as o on  r.organization_id=o.id '
            ' right join tenant t on t.tenant_id=o.tenant_id '
            ' right join organization_user as ou on ou.organization_id=o.id'
            ' right join user_integration as ui on ui.id=ou.user_integration_id'
            ' right join "user" as u on u.id=ui.user_id '
            ' where u.email  not ilike '"'%embold.io'"' and u.email not ilike '"'%acellere.com'"' and u.email not ilike '"'%mygamma.io'"' and '
            ' email not in('"'abhijit.parkhi@gmail.com'"','"'allison.ember@gmail.com'"', '
            ' '"'patwardhan.supriya@gmail.com'"','"'sudarshan.bhide@gmail.com'"', '
            ' '"'vikram.fugro@gmail.com'"','"'kochumvk@gmail.com'"', '
            ' '"'vikrantphadtare9@gmail.com'"','"'phoenixelite265@gmail.com'"','"'repoadder@gmail.com'"', '
            ' '"'embcheck1@gmail.com'"','"'shekharsvbhosale@gmail.com'"','"'shahajipatil22@gmail.com'"') ' 
            ' group by 2,3,license_expiry_date,o.slug,t.uid ,ou.role_name'
            ')s group by 1,2,3,4,license_expiry_date)s order by 4;')  

            deleted_account_cursor.execute('select customer_id as "Chargebee Customer ID" , '
	        'user_name as "Embold User name" , email as "Email", churn_info as "Churn Details", account_created_dt as "Account Created Date", '
	        'account_deleted_dt as "Account Deleted Date", (case when (account_deleted_dt- account_created_dt)::varchar='"'0'"' then '"'1'"' else (account_deleted_dt- account_created_dt)::varchar end) as "Account Used Period(Total Days)" , '
            ' (case  '
            ' when DATE_PART('"'month'"', AGE(account_deleted_dt, account_created_dt))::varchar = '"'0'"' then '"''"' '
		    ' else DATE_PART('"'month'"', AGE(account_deleted_dt, account_created_dt))::varchar || '"' Month '"'	end) '
	        ' || '
	        ' (case when DATE_PART('"'day'"', AGE(account_deleted_dt, account_created_dt))::varchar = '"'0'"' then '"'1 Day '"' '
		    ' else DATE_PART('"'day'"', AGE(account_deleted_dt, account_created_dt))::varchar || '"' Day '"'	end) '
	        ' as "Account Used Period(Months)" from  deleted_account_details d '
            ' where d.email  not ilike '"'%embold.io'"' and d.email not ilike '"'%acellere.com'"' and d.email not ilike '"'%mygamma.io'"' and '
            ' d.email not in('"'abhijit.parkhi@gmail.com'"','"'allison.ember@gmail.com'"', '
            ' '"'patwardhan.supriya@gmail.com'"','"'sudarshan.bhide@gmail.com'"', '
            ' '"'vikram.fugro@gmail.com'"','"'kochumvk@gmail.com'"', '
            ' '"'vikrantphadtare9@gmail.com'"','"'phoenixelite265@gmail.com'"','"'repoadder@gmail.com'"', '
            ' '"'embcheck1@gmail.com'"','"'shekharsvbhosale@gmail.com'"','"'shahajipatil22@gmail.com'"') '             
            ';');  

            scan_columns=[desc[0] for desc in scan_cursor.description]    
            pr_columns=[desc[0] for desc in pr_cursor.description]  
            user_details_columns=[desc[0] for desc in user_cursor.description]
            deleted_account_columns=[desc[0] for desc in deleted_account_cursor.description]      
            df1 = pd.DataFrame(scan_cursor.fetchall(),index=None,columns=scan_columns)
            df2 = pd.DataFrame(pr_cursor.fetchall(),index=None,columns=pr_columns)  
            df3 = pd.DataFrame(user_cursor.fetchall(),index=None,columns=user_details_columns)
            df4 = pd.DataFrame(deleted_account_cursor.fetchall(),index=None,columns=deleted_account_columns)            
            file_name="V2_scanDetails_"+str(today)+".xlsx"
            with pd.ExcelWriter(file_name) as writer:
                df1.to_excel(writer,sheet_name='Scan Details',index=False)
                df2.to_excel(writer,sheet_name='PR Details',index=False) 
                df3.to_excel(writer,sheet_name='User Data Analysis',index=False)
                df4.to_excel(writer,sheet_name='Deleted Account Details',index=False)
            scan_cursor.close()
            pr_cursor.close()
            user_cursor.close()
            deleted_account_cursor.close()
            return file_name         
    except Exception as error:        
        logging.error("Error:An exception has occured in report generating:", error)   
        return file_name

def scan_details_job():
    file_name=None
    try:
        logging.info("Job start : V2 Scan details report")            
        # get db connection
        try:
            gamma_connection,bi_connection=get_db_connections()
            if gamma_connection is not None:
                file_name=generate_scan_details_report(bi_connection)               
            else:
                logging.info("Job End with exceptions")
                sys.exit(1)
        except:
            sys.exit(1)
        
        if file_name is not None:
            logging.info("Info:V2 scan details report file generated successfully")
            status_flag=True
            try:
                status_flag=mailer.send_mail(file_name,cs.internal_team_list,cs.internal_team_subject) 
                if status_flag==True:                   
                    logging.info("Info: File sent to team via email successfully") 
                else:                    
                    logging.error("Error:Report not sent")  
            except Exception as error:
                logging.error("Error:An exception has occured in send email function",error)           
        else:
            logging.error("Error:Missing Scan details file")
            sys.exit(1)
        logging.info("Job End : V2 Scan details report")
    except Exception as error:
        logging.error("Error:An exception has occured in main function", error)
    finally:
            if file_name is not None:
                os.remove(file_name)
                logging.info("Info:V2 scan details report file removed successfully")
            if gamma_connection is not None and bi_connection is not None:        
                bi_connection.close()
                gamma_connection.close()               
                logging.info("Database connection's closed")
                        
