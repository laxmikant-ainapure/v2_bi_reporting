import pandas as pd
import psycopg2
import datetime
from datetime import datetime, timedelta
import sys
import os
import logging
import numpy as np

working_dir = os.getcwd()

today=str(datetime.strftime(datetime.today(), '%Y-%m-%d'))

yesterday=(datetime.strftime(datetime.today()-timedelta(days=1), '%Y-%m-%d'))

if os.path.isfile('python_utility_log_'+str(yesterday)+'.log'):
    os.remove('python_utility_log_'+str(yesterday)+'.log')

logging.basicConfig(filename='python_utility_log_'+str(today)+'.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)


def get_db_connections():
    gamma_connection=None
    bi_connection=None
    try:
        gamma_connection = psycopg2.connect(database="gamma", user="gamma_pg", password="nT7qU5m4AymRE9h7WvGp", host="v2-gamma-pg-prod2.ck3qzzly633v.eu-central-1.rds.amazonaws.com", port="5432")
        bi_connection = psycopg2.connect(database="bi_test", user="postgres", password="postgres", host="127.0.0.1", port="5432")
    except Exception as error:       
        logging.error(str(error))
    return gamma_connection,bi_connection


def truncate_bi_tables(bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        table_list=['repository_scan_history','repository_language','repository','organization','organization_user','user_integration','language','license_metrics','user','usage_history','review_request','review_request_queue','deleted_account_details']
        if bi_cursor is not None:
            for table_name in table_list:               
                bi_cursor.execute("Truncate table public."+table_name+";")
                bi_connection.commit()   
            return True
        else:
            return False           
    except Exception as error:
        logging.error("Error:An exception has occured in truncate_bi_tables", error)
        
        bi_cursor.execute("rollback")
           
def insert_bi_language(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, "name", now() FROM public."language" order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO language (id, name, last_job_date) VALUES(%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_language", error)
        

def insert_bi_license_metrics(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, tenant_uid, metric, value, ident, now() FROM public.license_metric order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO license_metrics (id, tenant_uid, metric, value, ident, last_job_date) VALUES(%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_license_metrics", error)
        

def insert_bi_organization(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('select distinct o.id, o.uid, o."name", o.slug, o."type", o.status, o.deleted_dt::date,t.id ,p.name as package,li.created_dt::date as license_created_date,li.expiry_dt::date as license_expiry_date, now() '
                                'from tenant t '
                                'inner join license_info li on t.uid =li.tenant_uid ' 
                                'inner join package p on p.id=li.package_id '
                                'inner join organization o on o.tenant_id = t.id '
                                'order by o.id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)            
            df=(df.replace({pd.NaT: None, np.NaN: None}))     
            df = df.where(pd.notnull(df), None)
            
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO organization (id, uid, name, slug, type, status, deleted_dt,tenant_id,package_name, license_created_date, license_expiry_date,last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_organization", error)
        


def insert_bi_organization_user(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, organization_id, user_integration_id, status, created_dt, now() FROM public.organization_user order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]      
            query  = "INSERT INTO organization_user (id, organization_id, user_integration_id, status, created_dt, last_job_date) VALUES(%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_organization_user", error)
        

def insert_bi_repository(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, organization_id, uid, name, has_snapshot, created_dt, updated_dt, is_deleted, repo_meta::text, type, now() FROM public.repository order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO repository (id, organization_id, uid, name, has_snapshot, created_dt, updated_dt, is_deleted, repo_meta, type, last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_repository", error)
        

def insert_bi_repository_language(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, repository_id, language_id, now() FROM public.repository_language order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO repository_language (id, repository_id, language_id,last_job_date) VALUES(%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_repository_language", error)
        

def insert_bi_repository_scan_history(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, scan_id, repository_id, started_on, ended_on, current_step, status, is_completed, now() FROM public.repository_scan_history order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO repository_scan_history (id, scan_id, repository_id, started_on, ended_on, current_step, status, is_completed,last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_repository_scan_history", error)
        


def insert_bi_user(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, email, created_dt, now() FROM public.user order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.user (id, email, created_dt,last_job_date) VALUES(%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_user", error)
        

def insert_bi_user_integration(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, user_id, master_integration_id, status, created_dt, updated_dt, last_login_dt, first_login_dt, now() FROM public.user_integration order by id;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.user_integration (id, user_id, master_integration_id, status, created_dt, updated_dt, last_login_dt, first_login_dt,last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in insert_bi_user_integration", error)

def insert_bi_review_request_queue(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, review_request_id, repository_uid, status, updated_on, created_on, now() FROM public.review_request_queue;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.review_request_queue (id, review_request_id, repository_uid, status, updated_on, created_on,last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in review request queue", error)

def insert_bi_review_requests(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, review_request_id, listened_on, repository_uid,now() FROM public.review_request;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.review_request (id, review_request_id, listened_on, repository_uid, last_job_date) VALUES(%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in review_requests", error)

def insert_bi_usage_history(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute('SELECT id, repository_uid, tenant_uid, snapshot_id, snapshot_time, (data::jsonb->>'"'eloc'"')::decimal as loc,now() FROM public.usage_history;')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.usage_history (id, repository_uid, tenant_uid, snapshot_id, snapshot_time, loc, last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in usage_history", error)
        
def insert_bi_deleted_account_details(gamma_connection , bi_connection):
    try:
        bi_cursor=bi_connection.cursor()
        gamma_cursor=gamma_connection.cursor()
        if gamma_cursor is not None:
            gamma_cursor.execute(' select event_type,(el.metadata::json->'"'old_value'"'->>'"'user_name'"')::varchar as user_name,(el.metadata::json->'"'old_value'"'->>'"'customer_id'"')::varchar as customer_id,  '
            '(el.metadata::json->'"'old_value'"'->>'"'email'"')::varchar as email, '
            ' replace(replace((el.metadata::json->'"'old_value'"'->>'"'reasons'"')::varchar,'"'['"','"''"'),'"']'"','"''"') as churn_info,'
            '((el.metadata::json->'"'old_value'"'->>'"'created_dt'"')::varchar) as created_dt ,created_dt::date '
            ' ,now() from event_log el  where event_type='"'ACCOUNT_DELETED'"';')
            df = pd.DataFrame(gamma_cursor.fetchall(),index=None)
            df=(df.replace({pd.NaT: None, np.NaN: None}))            
            df = df.where(pd.notnull(df), None)
            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO public.deleted_account_Details (event_type, user_name, customer_id, email,churn_info,account_created_dt,account_deleted_dt,last_job_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
            bi_cursor.executemany(query, tuples)               
            bi_connection.commit()
            return True
        else:
            return False
    except Exception as error:
        logging.error("Error:An exception has occured in deleted_account_details", error)


def data_load_job():
    try:       
        logging.info("Job start : BI data load")
        # get gamma and bi db connection
        try:
            gamma_connection,bi_connection=get_db_connections()
        except Exception as error:
            logging.error("Database connection failed ,Please check your database configuration")
            sys.exit(1)
        if gamma_connection is not None and bi_connection is not None:

            # truncate all bi tables
            try:   
                truncate_flag=truncate_bi_tables(bi_connection)                
                if truncate_flag==True:
                    logging.info("Info:All BI table's truncated")
                else:
                    logging.error("Error:BI table's truncating failed")
                    sys.exit(1)
            except:                
                sys.exit(1)

            # insert bi language data
            try:
                language_flag=insert_bi_language(gamma_connection , bi_connection)
                if language_flag==True:
                    logging.info("Info:Production Gamma language data inserted into BI table")           
                else:
                    logging.error("Error:Production Gamma language data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma language data inserting into BI table failed")
                pass
            
            # insert bi license_metrics data
            try:
                license_metrics_flag=insert_bi_license_metrics(gamma_connection , bi_connection)
                if license_metrics_flag==True:
                    logging.info("Info:Production Gamma license_metrics data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma license_metrics data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma license_metrics data inserting into BI table failed")
                pass

            # insert bi organization data
            try:
                organization_flag=insert_bi_organization(gamma_connection , bi_connection)
                if organization_flag==True:
                    logging.info("Info:Production Gamma organization data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma organization data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma organization data inserting into BI table failed")
                pass

            # insert bi organization_user data
            try:
                organization_user_flag=insert_bi_organization_user(gamma_connection , bi_connection)
                if organization_user_flag==True:
                    logging.info("Info:Production Gamma organization_user data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma organization_user data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma organization_user data inserting into BI table failed")
                pass


            # insert bi repository data
            try:
                repository_flag=insert_bi_repository(gamma_connection , bi_connection)
                if repository_flag==True:
                    logging.info("Info:Production Gamma repository data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma repository data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma repository data inserting into BI table failed")
                pass

            # insert bi repository_language data
            try:
                repository_language_flag=insert_bi_repository_language(gamma_connection , bi_connection)
                if repository_language_flag==True:
                    logging.info("Info:Production Gamma repository_language data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma repository_language data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma repository_language data inserting into BI table failed")
                pass

            # insert bi repository_scan_history data
            try:
                repository_scan_history_flag=insert_bi_repository_scan_history(gamma_connection , bi_connection)
                if repository_scan_history_flag==True:
                    logging.info("Info:Production Gamma repository_scan_history data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma repository_scan_history data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma repository_scan_history data inserting into BI table failed")
                pass

            # insert bi user data
            try:
                user_flag=insert_bi_user(gamma_connection , bi_connection)
                if user_flag==True:
                    logging.info("Info:Production Gamma user data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma user data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma user data inserting into BI table failed")
                pass

            # insert bi user_integration data
            try:
                user_integration_flag=insert_bi_user_integration(gamma_connection , bi_connection)
                if user_integration_flag==True:
                    logging.info("Info:Production Gamma user_integration data inserted into BI table")                        
                else:
                    logging.error("Error:Production Gamma user_integration data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma user_integration data inserting into BI table failed")
                pass

            try:
                user_flag=insert_bi_review_request_queue(gamma_connection , bi_connection)
                if user_flag==True:
                    logging.info("Info:Production Gamma review_request_queue data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma review_request_queue data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma review_request_queue data inserting into BI table failed")
                pass

            try:
                user_flag=insert_bi_review_requests(gamma_connection , bi_connection)
                if user_flag==True:
                    logging.info("Info:Production Gamma review_requests data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma review_requests data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma review_requests data inserting into BI table failed")
                pass

            try:
                user_flag=insert_bi_usage_history(gamma_connection , bi_connection)
                if user_flag==True:
                    logging.info("Info:Production Gamma usage_history data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma usage_history data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma usage_history data inserting into BI table failed")
                pass

            try:
                user_flag=insert_bi_deleted_account_details(gamma_connection , bi_connection)
                if user_flag==True:
                    logging.info("Info:Production Gamma deleted account data inserted into BI table")            
                else:
                    logging.error("Error:Production Gamma deleted account data inserting into BI table failed")
            except:
                logging.error("Error:Production Gamma deleted account data inserting into BI table failed")
                pass
    
            logging.info("Job end: BI data load ")
        else:
            logging.error("Error:Job Failed")
    except Exception as error:
        logging.error("Error:Job failed")
        logging.error("Error:Error message:",error)
        logging.error ("Exception TYPE:", type(error))
    finally:
        if gamma_connection is not None and bi_connection is not None:        
            bi_connection.close()
            gamma_connection.close()
            logging.info("Info:Database connection closed successfully")

#if __name__ == "__main__":
#    main()

   