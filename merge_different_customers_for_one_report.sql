with cte0 as (  --frm client_raw_bills
	select
		cust_id
		, pat_acct
		, total_charges charges
		, claim_content->>'accident_state' accident_state
		, claim_content->>'facility' facility
		, coalesce(nullif(claim_content->>'service_date', ''), nullif(to_char(admission_date, 'YYYYMMDD'), ''))::date date_of_service
	from 
		tpl_client_raw_bills
	where
		cust_id in ('405', '483', '484', '538')		
)
, cte1 as ( --from client_patient_accounts
	select
		foo0.cust_id
		, foo0.pat_acct
		, foo1.mlx_status_date status_date
		, foo1.mlx_status_code account_status
		, foo2.status_description
	from (
		select 
			cust_id
			, pat_acct
			, max(mlx_status_date) mlx_status_date
		from 
			tpl_client_patient_accounts	
		where 
			cust_id in ('405', '483', '484', '538')
		group by
			cust_id
			, pat_acct
	) foo0
	left join (
		select
			cust_id
			, pat_acct
			, mlx_status_code
			, mlx_status_date
		from 
			tpl_client_patient_accounts	
		where 
			cust_id in ('405', '483', '484', '538')
	) foo1 on
		foo0.cust_id = foo1.cust_id and foo0.pat_acct = foo1.pat_acct and foo0.mlx_status_date = foo1.mlx_status_date
	left join (
	select
		mlx_status_code
		, concat(mlx_status_code_desc, ' -- ', mlx_status_code_default_x12_code, ' -- ', mlx_status_code_usage) status_description
	from 
		tpl_status_codes
	) foo2 on
		foo1.mlx_status_code = foo2.mlx_status_code
)
, cte2 as ( --frm billing_records
	select
		foo0.cust_id
		, foo0.pat_acct
		, foo1.bill_drop_date
		, foo1.claim_submission_date
	from (
		select 
			cust_id
			, pat_acct
			, max(bill_sent_at) bill_drop_date
		from 
			tpl_billing_records
		where 
			cust_id in ('405', '483', '484', '538')
		group by
			cust_id
			, pat_acct
	) foo0
	left join (
		select 
			cust_id
			, pat_acct
			, concat(sending_method, '_OB_', vendor) claim_submission_date
			, bill_sent_at bill_drop_date
		from 
			tpl_billing_records
		where 
			cust_id in ('405', '483', '484', '538')
	) foo1
		on foo0.cust_id = foo1.cust_id and foo0.pat_acct = foo1.pat_acct and foo0.bill_drop_date = foo1.bill_drop_date
)
, cte3 as ( --no join
	select 
		foo0.cust_id
		, foo0.pat_acct
		, foo1.check_num
		, foo1.check_amount
		, foo0.response_code
		, foo0.description
		, foo0.all_notes
		, foo0.created_at response_date
	from (	
		select 
			cust_id
			, pat_acct
			, code response_code
			, case when length(nullif(note, '')) <= 100 then nullif(note, '') else null end description
			, case when length(nullif(note, '')) > 100 then nullif(note, '') else null end all_notes
			, created_at
		from 
			tpl_claim_responses
		where 
			cust_id in ('405', '483', '484', '538')
	) foo0
	left join (
		select 
			cust_id
			, pat_acct
			, substring(note,'(?<=# )(\d{1,30})') check_num
			, case when substring(note,'(?<=# )(\d{1,30})') is null then null else substring(note,'(?<=\$)[^,]*') end check_amount
			, created_at
		from 
			tpl_claim_responses
		where 
			cust_id in ('405', '483', '484', '538')
			and note ~* 'payment of' and note ~* 'check'
	) foo1 on
		foo0.cust_id = foo1.cust_id and foo0.pat_acct = foo1.pat_acct and foo0.created_at = foo1.created_at
)
, cte4 as (--frm claim_responses
select
	cust_id
	, pat_acct
	, string_agg(distinct check_num, '; ') check_num
	, string_agg(distinct check_amount, '; ') check_amount
	, string_agg(distinct response_code, '; ') response_code
	, string_agg(distinct description, '; ') description
	, string_agg(distinct all_notes, '; ') all_notes
	, string_agg(distinct response_date::text, '; ') response_date
from (
	select 
		foo0.cust_id
		, foo0.pat_acct
		, foo1.check_num
		, foo1.check_amount
		, foo1.response_code
		, foo1.description
		, foo1.all_notes
		, foo0.response_date::date response_date
	from (
		select
			cust_id
			, pat_acct
			, max(response_date) response_date
		from
			cte3
		group by
			cust_id
			, pat_acct
	) foo0
	left join (
		select
			cust_id
			, pat_acct
			, check_num
			, check_amount
			, response_code
			, description
			, all_notes
			, response_date
		from 
			cte3
		) foo1 on
			foo0.cust_id = foo1.cust_id and foo0.pat_acct = foo1.pat_acct and foo0.response_date = foo1.response_date
) foo2
group by
	cust_id
	, pat_acct	
)
, cte5 as ( --frm mva_trans
	select
		cust_id
		, pat_acct
		, round(sum(trans_amt)) tot_payment
		, max(trans_date) payment_date
	from (
		select 
			cust_id 
			, pat_acct
			, trans_amt
			, trans_date
		from 
			tpl_mva_trans
		where 
			cust_id in ('405', '483', '484', '538')
			and duplicate_payment = false	
	) foo0
	group by
		cust_id
		, pat_acct
)
, cte6 as ( --frm pre_billing_records
	select
		cust_id
		, pat_acct
		, count(distinct src_sk) acct_num_of_src
		, string_agg(distinct claim_type, '; ') claim_type
		, string_agg(distinct insurance_name, '; ') insurance_name
		, string_agg(distinct claim_num, '; ') claim_num
	from 
		tpl_pre_billing_records
	where 
		cust_id in ('405', '483', '484', '538')
	group by
		cust_id
		, pat_acct	
)	
-->
select 
	cte0.cust_id
	, cte6.acct_num_of_src
	, cte0.pat_acct
	, cte0.accident_state
	, cte0.facility
	, cte0.date_of_service
	, cte1.status_date
	, cte1.account_status
	, cte1.status_description
	, cte2.bill_drop_date
	, cte2.claim_submission_date
	, cte0.charges
	, cte4.check_num
	, cte4.check_amount
	, cte5.tot_payment
	, cte5.payment_date
	, cte6.claim_type
	, cte6.insurance_name
	, cte6.claim_num
	, cte4.response_date
	, cte4.response_code
	, cte4.description
	, cte4.all_notes
from 
	cte0
left join
	cte1 on
	cte0.cust_id = cte1.cust_id and cte0.pat_acct = cte1.pat_acct
left join
	cte2 on
	cte0.cust_id = cte2.cust_id and cte0.pat_acct = cte2.pat_acct
left join
	cte4 on
	cte0.cust_id = cte4.cust_id and cte0.pat_acct = cte4.pat_acct
left join
	cte5 on
	cte0.cust_id = cte5.cust_id and cte0.pat_acct = cte5.pat_acct
left join
	cte6 on
	cte0.cust_id = cte6.cust_id and cte0.pat_acct = cte6.pat_acct
order by
	cust_id	
	, date_of_service desc;

