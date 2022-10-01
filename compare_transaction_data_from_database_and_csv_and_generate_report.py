import os
import time
import datetime

import psycopg2.extras
import pandas as pd
import numpy as np


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]
datestr = timestr[:8]
t0 = time.time()

print('start generating report... ')
print('\nplease wait...')
print('window will close instantly if login failure...')

########################################################################################################################
# Import data
########################################################################################################################
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': 'james_niu_u',
    'password': 'Cw18745.'
}
con = psycopg2.connect(**params)

print('\nplease wait... querying...')
print('window will close automatically if failure...')

# insert SQL export
sql1 = """
with cte6 as (
select 
	cust_id 
	, pat_acct
	, charges
	, trans_amt
	, trans_date
	, trans_id
	, insurance_name
	, created_at
from 
	tpl_mva_trans
where 
	cust_id = 483 
	and duplicate_payment = false 
)
, cte7 as (
select
	cust_id
	, pat_acct
	, round(max(charges)) charges
	, round(sum(trans_amt)) tot_trans_amt
	, min(trans_date) earliest_cust_trans
	, max(trans_date) latest_cust_trans
	, string_agg(concat(insurance_name, ' -- ', '$'||to_char(round(trans_amt), '999G999')), ';  ') insurance_name
from 
	cte6
group by
	cust_id
	, pat_acct
)
-->
select * from cte7;
"""

df1 = pd.read_sql(sql1, con=con, params=params)

t1 = time.time()
print('\nquery success...' + str(round((t1 - t0), 1)) + 's...')

os.chdir('L:\\Auto_Opportunity_Analysis\\Claim_Responses_Trans_Files_Comparison')

df2 = pd.read_csv('billed_jopari_20201012.csv') # insert jopari export

t2 = time.time()
print('\nread csv success...' + str(round((t2 - t1), 1)) + 's...')

########################################################################################################################
# manipulate data
########################################################################################################################
df1['cust_id'] = df1['cust_id'].apply(lambda x: round(x))
# print(df1.head(3))
# print(df1.tail(2))

df2 = df2[[
    'cust_id'
    , 'Patient_Account_#'
    , 'Payer'
    , 'Charged'
    , 'Paid_Amt'
    , 'Pay_Date'
    , 'ANSI_Codes'
    , 'EFT_Check#'
]]
df2 = df2[df2['cust_id'] == 483].reset_index(drop=True)
df2['cust_id'] = df2['cust_id'].apply(lambda x: round(x))
df2 = df2.rename(columns={
    "Patient_Account_#": "pat_acct"
    , "Payer": "insurance_name"
    , "Charged": "charges"
    , "Paid_Amt": "jopari_paymt"
    , "Pay_Date": "paymt_date"
    , "ANSI_Codes" : "claim_adj_seg"
    , "EFT_Check#": "jopari_check_num"
})
# print(df2.head(3))
# print(df2.tail(2))

gb = df2.groupby('pat_acct')
df2a = gb.size().to_frame(name='paymt_cnt')
df2 = df2a\
    .join(gb.agg({'cust_id': lambda x: round(np.max(x))}).rename(columns={'cust_id': 'cust_id'}))\
    .join(gb.agg({'charges': lambda x: round(np.max(x))}).rename(columns={'charges': 'charges'}))\
    .join(gb.agg({'claim_adj_seg': lambda x: (';  ').join(map(str, set(x)))}).rename(columns={'claim_adj_seg': 'claim_adj_seg'}))\
    .join(gb.agg({'jopari_check_num': lambda x: (';  ').join(map(str, set(x)))}).rename(columns={'jopari_check_num': 'jopari_check_num'}))\
    .join(gb.agg({'jopari_paymt': lambda x: round(np.sum(x))}).rename(columns={'jopari_paymt': 'jopari_paymt'}))\
    .join(gb.agg({'paymt_date': lambda x: round(np.max(x))}).rename(columns={'paymt_date': 'latest_jopari_response'}))\
    .join(gb.agg({'insurance_name': lambda x: (';  ').join(map(str, set(x)))}).rename(columns={'insurance_name': 'jopari_carrier'}))\
    .reset_index()

cols = [
    'cust_id'
    , 'pat_acct'
    , 'charges'
    , 'claim_adj_seg'
    , 'jopari_check_num'
    , 'paymt_cnt'
    , 'jopari_paymt'
    , 'latest_jopari_response'
    , 'jopari_carrier'
]
df2 = df2[cols]

df2 = df2.sort_values(by=['pat_acct'], ascending=[False])
# print(df2.head(3))
# print(df2.tail(2))

df1 = df1.drop(columns=['charges'])
df1 = df1.rename(columns={'insurance_name': 'trans_carrier'})
df3 = pd.merge(df2, df1, how='left', left_on=['cust_id', 'pat_acct'], right_on=['cust_id', 'pat_acct'])

exist_paymt = []
for i, j in zip(df3['jopari_paymt'], df3['tot_trans_amt']):
    if pd.isnull(j):
        exist_paymt.append('N')
    else:
        exist_paymt.append('Y')
df3['exist_paymt'] = exist_paymt
exist_suffic_paymt = []
# print(df3[['jopari_paymt', 'tot_trans_amt', 'exist_paymt']].head(10))

for i, j in zip(df3['jopari_paymt'], df3['tot_trans_amt']):
    if pd.isnull(j):
        exist_suffic_paymt.append('N')
    else:
        if j >= i:
            exist_suffic_paymt.append('Y')
        else:
            exist_suffic_paymt.append('N')
df3['exist_suffic_paymt'] = exist_suffic_paymt
# print(df3[['jopari_paymt', 'tot_trans_amt', 'exist_suffic_paymt']].head(10))

df3['latest_jopari_response'] = df3['latest_jopari_response']\
    .apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d').date())
trans_after_response = []
for i, j in zip(df3['latest_jopari_response'], df3['latest_cust_trans']):
    if pd.isnull(j):
        trans_after_response.append('N')
    else:
        if j >= i:
            trans_after_response.append('Y')
        else:
            trans_after_response.append('N')
df3['trans_after_response'] = trans_after_response
# print(df3[['latest_jopari_response', 'latest_cust_trans', 'trans_after_response']].head(10))

# cols = list(df3.columns.values)
# print(cols)
cols = [
    'cust_id'
    , 'pat_acct'
    , 'charges'
    , 'claim_adj_seg'
    , 'jopari_check_num'
    , 'paymt_cnt'
    , 'jopari_paymt'
    , 'tot_trans_amt'
    , 'exist_paymt'
    , 'exist_suffic_paymt'
    , 'latest_jopari_response'
    , 'latest_cust_trans'
    , 'trans_after_response'
    , 'jopari_carrier'
    , 'trans_carrier'
]
df3 = df3[cols]
df3.insert(1, 'cust_name', 'THR')

########################################################################################################################
# Export data
########################################################################################################################

os.chdir('L:\\Auto_Opportunity_Analysis\\Claim_Responses_Trans_Files_Comparison\\THR')

df3.to_csv('THR_Claim_Responses_Trans_Files_Comparison_' + datestr + '.csv', index=False)

t3 = time.time()
print('\ngenerate report success...' + str(round((t3 - t2), 1)) + 's...')