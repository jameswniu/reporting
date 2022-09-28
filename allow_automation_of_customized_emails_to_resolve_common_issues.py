import os
import sys
import re

import pytz
import smtplib

from datetime import datetime as dt


#----
# specify receivers and names
#----
tz = pytz.timezone('America/New_York')
Ymd = dt.now(tz=tz).strftime('%Y%m%d')
md = Ymd[4:]
sender = 'james.niu@medlytix.com'
receivers = ['james.niu@medlytix.com', 'wendi_niu@yahoo.com.sg']
names = ['James Niu', 'Wendi Niu']

frm = 'James Niu <{}>'.format(sender)
l = []
for receiver, name in zip(receivers, names):
    entry = '{} <{}>'.format(name, receiver)
    l.append(entry)
to = ', '.join(l)


#----
# write message and subject
#----
message_accident = """From: {}
To: {}
Subject: Update VNEX Accident Date in Pre-Billing Table - {}

Yi,

Please update VNEX accident date(s) in pre-billing table for billing:
/tmp/update_{}_accident.sql

Sincerely,


James Niu
Data Analyst
Medlytix, LLC

675 Mansell Road
Suite 100
Roswell, GA 30076
Direct Line: (678) 589-7439
www.medlytix.com
""".format(frm, to, Ymd, md)


message_zip = """From: {}
To: {}
Subject: Update Zip Code(s) in Pre-Billing Table for Billing - {}

Yi,

Please update the following zip code(s) in pre-billing table for billing:
/tmp/update_{}_zip.sql

Sincerely,


James Niu
Data Analyst
Medlytix, LLC

675 Mansell Road
Suite 100
Roswell, GA 30076
Direct Line: (678) 589-7439
www.medlytix.com
""".format(frm, to, Ymd, md)


#----
# send out
#----
def automail_accident():
    try:
        message = message_accident
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as mail:
            mail.starttls()
            mail.login('james.niu@medlytix.com', 'Cw1874567.,')
            mail.sendmail(sender, receivers, message)
        print(message)
        print('sending success...')
    except:
        print('sending ERROR...')

def automail_zip():
    try:
        message = message_zip
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as mail:
            mail.starttls()
            mail.login('james.niu@medlytix.com', 'Cw1874567.,')
            mail.sendmail(sender, receivers, message)
        print(message)
        print('sending success...')
    except:
        print('sending ERROR...')


if os.path.exists(r'L:\Billing_Processing\logs\update_{}_accident.sql'.format(md)):
    automail_accident()
    print('accident file exists')
else:
    print('no accident file')
if os.path.exists(r'L:\Billing_Processing\logs\update_{}_zip.sql'.format(md)):
    automail_zip()
    print('zip file exists')
else:
    print('no zip file')

