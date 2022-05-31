import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import logging
from datetime import datetime, timedelta
import constant as cs

today=str(datetime.strftime(datetime.today(), '%Y-%m-%d'))

def send_mail(file_name,to_mail_list,mail_subject):
    status_flag=None
    try:
        filename = file_name
        SourcePathName  = file_name 
        to_mail_list=to_mail_list
        mail_subject=mail_subject
        msg = MIMEMultipart()
        msg['From'] = cs.from_mail #'laxmikant.ainapure@embold.io'
        msg['To'] = to_mail_list
        #msg['To'] = 'laxmikant.ainapure@embold.io'
        msg['Subject'] = mail_subject
        body = ' Hi All, \n\n Please find attached file for v2 Production repositories and scans details as of date '+str(today)+ '.  \n\n Please let me know if anyone has suggestions. \n\n Thank you,\n\n Laxmikant.'
        msg.attach(MIMEText(body, 'plain'))
        # ATTACHMENT PART OF THE CODE IS HERE
        attachment = open(SourcePathName, 'rb')
        part = MIMEBase('application', "octet-stream")
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(part)
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(cs.from_mail, cs.from_mail_password)
        server.send_message(msg)        
        server.quit()
        status_flag=True
    except Exception as error:
       logging.error("Error:An exception has occured in email sending:", error)
    return status_flag