------
-- grab all relevant customers only within specified time period
------
select distinct
    cust_id
from
    tpl_billing_records
where
    timezone('UTC', now())::date - created_at::date < 180
order by
    cust_id;

/*366	Beaumont Health
405	ApolloMD Athena
538	Team Health
646	AULTMAN PROFESSIONAL
530	Western Medical Associates
497	Logix Health
564	VEP
483	Texas Health Resources
484	AULTMAN HOSPITAL
227	Johns Hopkins
678	Western Medical Associates*/


------
-- expanding relevant customer info by more database tables
------
select distinct tpl_client_raw_bills.cust_id, tpl_cust_infos.cust_name from tpl_client_raw_bills
left join tpl_cust_infos on tpl_client_raw_bills.cust_id = tpl_cust_infos.cust_id
where tpl_client_raw_bills.cust_id not in ('67', '107', '171', '9990');


------
-- start analysis
------
--explain
--analyze
with myid (cid) as ( ---- UNCOMMENT to include customer
	values
	(null)
--	, (227)
--	, (366)
--	, (405)
--	, (538)
--	, (646)
--	, (530)
--	, (497)
--	, (564)
	, (483)
--	, (484)
--	, (678)
)
, cte0 as ( -- get all accounts to compare matching
	select 
		cust_id
		, pat_acct
		, min(created_at)::date info_received
		, string_agg(distinct facility_or_svc_prvd_state, '; ') facility_or_svc_prvd_state
		, string_agg(distinct patient_lastname, '; ') patient_lastname
		, string_agg(distinct patient_firstname, '; ') patient_firstname
		, string_agg(distinct patient_addr1, '; ') patient_addr1
		, string_agg(distinct patient_city, '; ') patient_city
		, string_agg(distinct patient_state, '; ') patient_state
		, string_agg(distinct nullif(patient_phone, ''), '; ') patient_phone
		, string_agg(distinct nullif(patient_ssn, ''), '; ') patient_ssn
	from (
		select 
			cust_id
			, pat_acct
			, created_at
			, coalesce(nullif(content->>'facility_state', ''), nullif(content->>'servicing_provider_location_state', '')) facility_or_svc_prvd_state
			, content->>'patient_lastname' patient_lastname
			, content->>'patient_firstname' patient_firstname
			, content->>'patient_addr1' patient_addr1
			, content->>'patient_city' patient_city
			, content->>'patient_state' patient_state
			, replace(replace(replace(replace(coalesce(nullif(content->>'patient_phone', ''), nullif(claim_content->>'patient_phone', '')), '(' , ''), ')', ''), '-', ''), ' ', '') patient_phone
			, coalesce(nullif(content->>'patient_ssn', ''), nullif(claim_content->>'patient_ssn', '')) patient_ssn
		from 
			tpl_client_raw_bills 
		where
			cust_id in (select cid from myid)
	) foo1
	group by
		cust_id
		, pat_acct
)
, cte1 as ( -- get matches 
	select distinct
		cust_id
		, pat_acct
		, insurance_name
		, min(created_at)::date info_matched
	from 
		tpl_pre_billing_records
	where
		cust_id in (select cid from myid)
	group by
		cust_id
		, pat_acct
		, insurance_name
)
, cte2 as ( -- get accounts and their matches
select 
	cte0.cust_id
	, cte0.pat_acct
	, cte0.info_received
	, cte0.facility_or_svc_prvd_state
	, cte0.patient_lastname
	, cte0.patient_firstname
	, cte0.patient_addr1
	, cte0.patient_city
	, cte0.patient_state
	, cte0.patient_phone
	, cte0.patient_ssn
	, cte1.insurance_name
	, cte1.info_matched
	, cte1.info_matched - cte0.info_received days_to_match
from 
	cte0 
left join 
	cte1 on
	cte0.cust_id = cte1.cust_id and cte0.pat_acct = cte1.pat_acct
)
-->
------
-- Analysis of hit rate and data quality
------
select
	tpl_cust_infos.master_id
	, foo2.cust_id
	, tpl_cust_infos.cust_name
	, tpl_cust_infos.cust_type
	, foo2.cnt_accts_eligibility_found
	, foo2.cnt_accts
	, foo2.hit_rate
	, foo2.perc_valid_firstname
	, foo2.perc_valid_lastname
	, foo2.pct_valid_addr1
	, foo2.pct_valid_city
	, foo2.pct_valid_state
	, foo2.pct_valid_phone
	, foo2.pct_valid_ssn
from (
	select
		cust_id
		, case when cust_id in ('366', '18', '227') then null else sum(eligibility_found) end cnt_accts_eligibility_found
		, count(*) cnt_accts
		,  case when cust_id in ('366', '18', '227') then null else round(sum(eligibility_found) / count(*)::numeric, 2) end hit_rate
		, round(count(patient_firstname)/ count(*)::numeric, 2) perc_valid_firstname
		, round(count(patient_lastname)/ count(*)::numeric, 2) perc_valid_lastname
		, round(sum(flag_valid_addr1)/ count(*)::numeric, 2) pct_valid_addr1
		, round(count(patient_city)/ count(*)::numeric, 2) pct_valid_city
		, round(count(patient_state)/ count(*)::numeric, 2) pct_valid_state
		, round(sum(flag_valid_phone)/ count(*)::numeric, 2) pct_valid_phone
		, round(sum(flag_valid_ssn)/ count(*)::numeric, 2) pct_valid_ssn
	from (
		select
			cust_id
			, pat_acct
			, string_agg(distinct info_received::text, ';  ') info_received
			, string_agg(distinct facility_or_svc_prvd_state, ';  ') facility_or_svc_prvd_state
			, string_agg(distinct patient_lastname, ';  ') patient_lastname
			, string_agg(distinct patient_firstname, ';  ') patient_firstname
			, string_agg(distinct patient_addr1, ';  ') patient_addr1
			, case when string_agg(distinct patient_addr1, ';  ') ~* 'PO BOX' or string_agg(distinct patient_addr1, ';  ') ~* 'UNK'
					or nullif(replace(string_agg(distinct patient_addr1, ';  '), '; ', ''), '') is null
				then 0
			else
				1 end flag_valid_addr1
			, string_agg(distinct patient_city, ';  ') patient_city
			, string_agg(distinct patient_state, ';  ') patient_state
			, string_agg(distinct patient_phone, ';  ') patient_phone
			, case when string_agg(distinct patient_phone, ';  ') ~* '([0-9])\1{9}'
					or substring(string_agg(distinct patient_phone, ';  '), 1, 1) ~* '0|1'
					or substring(string_agg(distinct patient_phone, ';  '), 4, 1) ~* '0|1'
					or nullif(replace(string_agg(distinct patient_phone, ';  '), '; ', ''), '') is null
				then 0
			else
				1 end flag_valid_phone
			, string_agg(distinct patient_ssn, ';  ') patient_ssn
			, case when substring(string_agg(distinct patient_ssn, ';  '), 1, 1) ~* '9'
					or substring(string_agg(distinct patient_ssn, ';  '), 1, 3) ~* '666|000'
					or substring(string_agg(distinct patient_ssn, ';  '), 4, 1) ~* '00'
					or substring(string_agg(distinct patient_ssn, ';  '), 6, 4) ~* '0000'
					or nullif(replace(string_agg(distinct patient_ssn, ';  '), '; ', ''), '') is null
				then 0
			else
				1 end flag_valid_ssn
			, case when nullif(string_agg(distinct insurance_name, ';  '), '') is not null
				then 1
			else
				0 end eligibility_found
			, string_agg(distinct insurance_name, ';  ') payers_match
		from
			cte2
		group by
			cust_id
			, pat_acct
) foo1
group by
	foo1.cust_id
order by
	cust_id
) foo2
left join tpl_cust_infos
	on foo2.cust_id = tpl_cust_infos.cust_id
order by
	hit_rate asc nulls last
	, cnt_accts desc;
------
-- List of accts and matches
------
select
	cust_id
	, pat_acct
	, string_agg(distinct info_received::text, ';  ') info_received
	, string_agg(distinct facility_or_svc_prvd_state, ';  ') facility_or_svc_prvd_state
	, string_agg(distinct patient_lastname, ';  ') patient_lastname
	, string_agg(distinct patient_firstname, ';  ') patient_firstname
	, string_agg(distinct patient_addr1, ';  ') patient_addr1
	, case when string_agg(distinct patient_addr1, ';  ') ~* 'PO BOX' or string_agg(distinct patient_addr1, ';  ') ~* 'UNK' 
			or nullif(replace(string_agg(distinct patient_addr1, ';  '), '; ', ''), '') is null
		then 0
	else 
		1 end flag_valid_addr1
	, string_agg(distinct patient_city, ';  ') patient_city
	, string_agg(distinct patient_state, ';  ') patient_state
	, string_agg(distinct patient_phone, ';  ') patient_phone
	, case when string_agg(distinct patient_phone, ';  ') ~* '([0-9])\1{9}' 
			or substring(string_agg(distinct patient_phone, ';  '), 1, 1) ~* '0|1'
			or substring(string_agg(distinct patient_phone, ';  '), 4, 1) ~* '0|1'
			or nullif(replace(string_agg(distinct patient_phone, ';  '), '; ', ''), '') is null
		then 0
	else 
		1 end flag_valid_phone
	, string_agg(distinct patient_ssn, ';  ') patient_ssn
	, case when substring(string_agg(distinct patient_ssn, ';  '), 1, 1) ~* '9' 
			or substring(string_agg(distinct patient_ssn, ';  '), 1, 3) ~* '666|000' 
			or substring(string_agg(distinct patient_ssn, ';  '), 4, 1) ~* '00' 
			or substring(string_agg(distinct patient_ssn, ';  '), 6, 4) ~* '0000' 
			or nullif(replace(string_agg(distinct patient_ssn, ';  '), '; ', ''), '') is null
		then 0
	else 
		1 end flag_valid_ssn
	, case when nullif(string_agg(distinct insurance_name, ';  '), '') is not null
		then 1
	else 
		0 end eligibility_found
	, string_agg(distinct insurance_name, ';  ') payers_match
from
	cte2
group by
	cust_id
	, pat_acct
order by
	info_received desc;
	, facility_or_svc_prvd_state;

