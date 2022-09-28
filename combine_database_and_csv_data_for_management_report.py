import time
import os

import pandas as pd
import numpy as np
import psycopg2.extras

from config import user_db, passwd_db


timestr = time.strftime("%Y%m%d_%H%M%S")
minutestr = timestr[:-2]
datestr = timestr[:8]

print('start generating report... ')

print('\nplease wait...')
print('window will close automatically if failure...')
time_0 = time.time()


#-----------------------------------------------------------------------------------------------------------------------
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)


# Place SQL query here
sql1 = """
with cte1 as (
	select 
		tpl_claim_responses.pm_sk
		, tpl_claim_responses.cust_id
		, tpl_claim_responses.pat_acct
		, tpl_pre_billing_records.insurance_name
		, tpl_claim_responses.code claim_x12_code
		, tpl_claim_responses.note user_note
		, tpl_claim_responses.created_at
	from 
		tpl_claim_responses 
	left join tpl_pre_billing_records on 
		tpl_claim_responses.pm_sk = tpl_pre_billing_records.pm_sk
	where
		(response_from is null or response_from != 'MX_E_OBCAL') and (
		(code = '277-F4' and tpl_claim_responses.note ~* 'A7:35')
		or (code = '277-F4' and tpl_claim_responses.note ~* 'A7:97')
		or (code = '277-F4' and tpl_claim_responses.note = 'missing Claim Number')
		or (code = '277-F4' and tpl_claim_responses.note = 'patient cannot be identified as our insured')
		or (code = '835-N587' and tpl_claim_responses.note = 'lifetime benefit maximum has been reached for this service')
		or (code = '835-N650' and tpl_claim_responses.note = 'incomplete/invalid documentation')
		or (code = '277-P1' and tpl_claim_responses.note = 'pending')
		or (code = '835-B13' and tpl_claim_responses.note ~* 'CAS code')
		or (code = '277-A7' and tpl_claim_responses.note ~* 'missing or wrong diagnosis code')))
, cte2 as (
	select
		cte1.pm_sk
		, cte1.cust_id
		, cte1.pat_acct
		, cte1.insurance_name
		, cte1.claim_x12_code
		, cte1.user_note
		, timezone('UTC', now())::date - cte1.created_at::date days_frm_response
	from (
		select pat_acct, insurance_name, max(created_at) created_at from cte1 group by pat_acct, insurance_name) foo1
	left join cte1 on 
		foo1.pat_acct = cte1.pat_acct and 
		foo1.insurance_name = cte1.insurance_name and 
		foo1.created_at = cte1.created_at)
, cte3 as (
	select
		cte2.pm_sk
		, master_list.master_id
		, master_list.cust_id
		, master_list.cust_name
		, cte2.pat_acct
		, cte2.insurance_name
		, cte2.claim_x12_code
		, cte2.user_note
		, cte2.days_frm_response
	from (
		select master_id, cust_id, cust_name from tpl_cust_infos) master_list
	right join cte2 on
		master_list.cust_id = cte2.cust_id)
, cte4 as(		
	select
		coalesce(z1.pat_acct, z2.pat_acct) pat_acct
		, coalesce (z1.acct_charge, z2.acct_charge) acct_charge
		, coalesce (z1.days_dos_to_received, z2.days_dos_to_received) days_dos_to_received
		, coalesce (z1.principal_diagnosis, z2.principal_diagnosis) principal_diagnosis
		, coalesce (z1.pat_age, z2.pat_age) pat_age
		, coalesce (z1.pat_gender, z2.pat_gender) pat_gender
	from (
	select
		pat_acct,
		acct_charge,
		days_dos_to_received,
		case
			when principal_diagnosis in ('', ';', '1') then null 
			else principal_diagnosis end as principal_diagnosis,
		case 
			when pat_age <= 0 or pat_age::text = '' then null
			else pat_age::text end as pat_age,
		case
			when pat_gender in ('F; U', 'F; M', 'M; U') then null 
			else pat_gender end as pat_gender
	from
		(
		select 
			pat_acct,
			max(total_charges::integer) as acct_charge, 
			min(created_at::date - greatest(admission_date, discharge_date)::date) as days_dos_to_received,
			left(string_agg(distinct principal_diagnosis, ';  '), 1) as principal_diagnosis,
			max((least(admission_date, discharge_date)::date - cast(content->>'patient_dob' as date)) / 365) as pat_age,
			string_agg(distinct content->>'patient_gender', '; ') as pat_gender
		from 
			tpl_client_raw_bills
		group by 
			pat_acct) as foo_2) as z1
	full join (
		select
			pat_acct,
			acct_charge,
			days_dos_to_received,
			case
				when principal_diagnosis in ('3', '', '9', '2', '1', '4', '7', '8', '5', '6', '0') then null 
				else principal_diagnosis end as principal_diagnosis,
			case 
				when pat_age <= 0 or pat_age::text = '' then null
				else pat_age::text end as pat_age,
			case
				when pat_gender in ('F; M', '') then null 
				else pat_gender end as pat_gender
			from 
				(
				select 
					pat_acct,
					max(gross_charges::integer) as acct_charge, 
					min(created_at::date - greatest(admit_date_or_service_date, discharge_date)::date) as days_dos_to_received,
					left(string_agg(distinct content->>'primary_diagnostic_code', ';  '), 1) as principal_diagnosis,
					max((least(admit_date_or_service_date, discharge_date)::date - cast(case when content->>'patient_dob' = '' then null else content->>'patient_dob' end as date)) 
					/ 365) pat_age,
					string_agg(distinct content->>'patient_gender', '; ') as pat_gender
				from 
					tpl_raw_bills
				group by
					pat_acct) foo_3) z2
			on z1.pat_acct = z2.pat_acct)
, cte5 as (
select 
	pm_sk
	, max(pat_acct) pat_acct
	, max(insurance_name) insurance_name
	, max(claim_type) claim_type
	, case when max(accident_state) = max(patient_state) then 1 else 0 end pat_eq_accident_state
	, max(accident_state) accident_state
	, greatest(max(ldos)
	, max(fdos)) - max(accident_date) days_accident_to_dos
from 
	tpl_billing_records 
group by 
	pm_sk)
, cte6 as (
select
	insurance_name
	, string_agg(distinct content->>'edi_payer_id', ';  ') jopari_payer_id
from 
	tpl_pre_billing_records
group by 
	insurance_name)
-->			
select 
	cte3.pm_sk
	, cte3.master_id
	, cte3.cust_id
	, cte3.cust_name
	, cte3.pat_acct
	, cte3.insurance_name
	, cte3.claim_x12_code
	, cte3.user_note
	, cte3.days_frm_response
	, cte4.acct_charge
	, cte5.claim_type
	, cte5.days_accident_to_dos
	, cte4.days_dos_to_received
	, cte4.principal_diagnosis
	, cte4.pat_age
	, cte4.pat_gender
	, cte5.pat_eq_accident_state
	, cte5.accident_state
	, cte6.jopari_payer_id
--	, null rej_den_clearing_house
--	, null den_payer
from 
	cte3
left join cte4 on 
	cte3.pat_acct = cte4.pat_acct
left join cte5 on
	cte3.pm_sk = cte5.pm_sk
left join cte6 on
	cte3.insurance_name = cte6.insurance_name;
"""


#-----------------------------------------------------------------------------------------------------------------------
# Change target directory
os.chdir('L:\\auto_opportunity_analysis\\MLX_Decision_Management\\Jopari_Payer_List')

df1 = pd.read_csv('jopari_list_John.csv')
df1.columns = df1.columns.str.replace(' ', '_')
df1 = df1.drop(columns=['Clearinghouse',
                        'Medlytix_Payer_ID',
                        'Payer_Name',
                        'Liability',
                        'Last_Updated'])
df1 = df1.rename(columns={'Payer_ID': 'jopari_payer_id',
                          'WC': 'work_comp',
                          'Auto': 'auto',
                          '1500': 'prof_1500',
                          'UB04': 'inst_UB04',
                          'Rx': 'prsc_rx',
                          '835': 'response_all_835',
                          '277': 'response_all_277',
                          'direct': 'direct_connect_jopari',
                          'indirect': 'indirect_connect_jopari'})
# print(df1.head())
# print(len(df1.index))

gb = df1.groupby('jopari_payer_id')
df0 = gb.agg(np.max)
df1 = df0.reset_index()
# print(df1.head())
# print(len(df1.index))


#-----------------------------------------------------------------------------------------------------------------------
# Change target directory
os.chdir('L:\\Auto_Opportunity_Analysis\\MLX_Decision_Management\\Jopari_EDI_Set_1')

df2 = pd.read_sql(sql1, con=con, params=params)
# df2 = pd.read_csv('Decision_Analysis_Jopari_EDI_Set_1_Claims_Response_X12_Account_Traits_James_20200504.csv')
# print(df2.head())
# print(len(df2.index))

df3 = df2.merge(df1, how='left', left_on='jopari_payer_id', right_on='jopari_payer_id')
df3.insert(18, 'accident_state_covered', df3.lookup(df3.index, df3.accident_state))
df3['rej_den_clearing_house'] = np.NaN
df3['den_payer'] = np.NaN
print(df3.head())
print(len(df3.index))


#-----------------------------------------------------------------------------------------------------------------------
df3.to_csv('Decision_Analysis_Jopari_EDI_Set_1_Claims_Response_X12_James_' + minutestr + '.csv', index=False)

time_1 = time.time()
print('\nsuccess...' + str(round((time_1 - time_0), 1)) + 's...')

