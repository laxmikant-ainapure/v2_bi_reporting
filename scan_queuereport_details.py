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
import constant as cs
import mailer

today=str(datetime.strftime(datetime.today(), '%Y-%m-%d'))

def generate_scan_queue_details_report(gamma_connection):
    file_name=None
    try:        
        scan_cursor=gamma_connection.cursor()
        pr_cursor=gamma_connection.cursor()
        fail_scan_cursor=gamma_connection.cursor()
        fail_pr_cursor=gamma_connection.cursor()
        
        if scan_cursor is not None:

            scan_cursor.execute('select rq.scan_id ,r.uid as repository_uid, r.name as repository_name,rq.current_step , rq.status,rq.module_name,rq.started_on,rq.updated_on, '
            '	o.slug as organization_slug ,t.uid as tenant_uid, '
            '	(DATE_PART('"'day'"',current_timestamp::timestamp - started_on::timestamp) * 24 + '
            '	 DATE_PART('"'hour'"',  current_timestamp::timestamp-started_on::timestamp ) ) as scan_age '
            '   from repository_scan_queue rq '
            '   inner join repository r on r.id=rq.repository_id '
            '   inner join organization o on r.organization_id=o.id '
            '   inner join tenant t on t.id=o.tenant_id '
            '   where (DATE_PART('"'day'"',current_timestamp::timestamp - started_on::timestamp) * 24 + '
            ' 	 DATE_PART('"'hour'"',  current_timestamp::timestamp-started_on::timestamp ) )>=12;') 

            pr_cursor.execute('select distinct review_request_id,rq.status,session_id,r.uid as repository_uid,r.name as repository_name, '
            '  o.slug as organization_slug ,t.uid as tenant_uid,rq.created_on , rq.updated_on, '
            '	(DATE_PART('"'day'"',current_timestamp::timestamp - created_on::timestamp) * 24 + '
            ' 	 DATE_PART('"'hour'"',  current_timestamp::timestamp - created_on::timestamp ) ) as scan_age '
            '   from  review_request_queue rq '
            '   inner join repository r on r.uid=rq.repository_uid '
            '   inner join organization o on r.organization_id=o.id '
            '   inner join tenant t on t.id=o.tenant_id '
            '  where  '
            '  rq.status not in ('"'ABORT'"','"'FAIL'"','"'NO_FILES'"','"'SUCCESS'"') and  '
            '  (DATE_PART('"'day'"',current_timestamp::timestamp - created_on::timestamp) * 24 + '
            '  	 DATE_PART('"'hour'"',  current_timestamp::timestamp - created_on::timestamp ) )>=12;')

            fail_scan_cursor.execute('select t.uid as tenant_uid,o.slug as organization,scan_id,r.uid as repository_uid, '
            ' repository_id , r.name as repository_name,current_step as reason,rh.status from repository_scan_history  rh  '
            '   inner join repository r on r.id=rh.repository_id '
            '   inner join organization o on r.organization_id=o.id '
            '   inner join tenant t on t.id=o.tenant_id '
            '   where rh.status='"'FAIL'"'  '
            '   and started_on between ((current_timestamp - INTERVAL '"'1 DAY'"')::timestamp) and current_timestamp;')
            
            fail_pr_cursor.execute('select t.uid as tenant_uid,o.slug as organization,repository_uid,r.name as repository_name ,review_request_id,session_id , '
            '   source_commit_id,destination_commit_id,rq.status '
            '   from review_request_queue  rq '
            '   inner join repository r on r.uid=rq.repository_uid '
            '   inner join organization o on r.organization_id=o.id '
            '   inner join tenant t on t.id=o.tenant_id '
            '   where rq.status='"'FAIL'"'  '
            '   and created_on between ((current_timestamp - INTERVAL '"'1 DAY'"')::timestamp) and current_timestamp;')

     
            scan_columns=[desc[0] for desc in scan_cursor.description]    
            pr_columns=[desc[0] for desc in pr_cursor.description]  
            fail_scan_columns=[desc[0] for desc in fail_scan_cursor.description]  
            fail_pr_columns=[desc[0] for desc in fail_pr_cursor.description]  
            
            df1 = pd.DataFrame(scan_cursor.fetchall(),index=None,columns=scan_columns)
            df2 = pd.DataFrame(pr_cursor.fetchall(),index=None,columns=pr_columns)  
            df3 = pd.DataFrame(fail_scan_cursor.fetchall(),index=None,columns=fail_scan_columns)  
            df4 = pd.DataFrame(fail_pr_cursor.fetchall(),index=None,columns=fail_pr_columns)              
            file_name="V2_FullscanPRdetails_"+str(today)+".xlsx"
            with pd.ExcelWriter(file_name) as writer:
                df1.to_excel(writer,sheet_name='Full Scan stuck Details',index=False)
                df2.to_excel(writer,sheet_name='PR scan stuck Details',index=False)
                df3.to_excel(writer,sheet_name='Full scan failed Details',index=False)
                df4.to_excel(writer,sheet_name='PR scan failed Details',index=False) 
            scan_cursor.close()
            pr_cursor.close()
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
                file_name=generate_scan_queue_details_report(gamma_connection)               
            else:
                logging.info("Job End with exceptions")
                sys.exit(1)
        except:
            sys.exit(1)
        
        if file_name is not None:
            logging.info("Info:V2 scan details report file generated successfully")
            status_flag=True
            try:
                status_flag=mailer.send_mail(file_name,cs.developer_list,cs.developer_team_subject) 
                if status_flag==True:                   
                    logging.info("Info: File sent to team via email successfully") 
                else:                    
                    logging.error("Error:Report not sent")  
            except Exception as error:
                logging.error("Error:An exception has occured in send email function",error)           
        else:
            logging.error("Error:Missing Scan stuck details file")
            sys.exit(1)
        logging.info("Job End : V2 Scan details report")
    except Exception as error:
        logging.error("Error:An exception has occured in main function", error)
    finally:
            if file_name is not None:
                os.remove(file_name)
                logging.info("Info:V2 scan stcuk details report file removed successfully")
            if gamma_connection is not None and bi_connection is not None:        
                bi_connection.close()
                gamma_connection.close()               
                logging.info("Database connection's closed")
                        
