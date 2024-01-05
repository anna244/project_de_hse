from sqlalchemy import text


# 1.Совершение операции при просроченном или заблокированном паспорте
tmp_1 = """
	SELECT 
	    tr.trans_date as event_dt,
	    clients.passport_num as passport,
	    CONCAT(last_name,' ', first_name,' ', patronymic) as fio,
	    clients.phone as phone,
	    1 as event_type,
	    now() :: date as report_dt
	FROM public.anni_dwh_fact_transactions tr
	
	INNER JOIN public.anni_dwh_dim_cards_hist cards
	ON tr.card_num = cards.card_num
	AND tr.trans_date between cards.effective_from AND cards.effective_to
	AND cards.deleted_flag = 0
	
	INNER JOIN public.anni_dwh_dim_accounts_hist accounts
	ON cards.account_num = accounts.account_num
	AND tr.trans_date BETWEEN accounts.effective_from AND accounts.effective_to
	AND accounts.deleted_flag = 0
	
	INNER JOIN public.anni_dwh_dim_clients_hist clients
	ON accounts.client = clients.client_id
	AND tr.trans_date BETWEEN clients.effective_from AND clients.effective_to
	AND clients.deleted_flag = 0
	
	LEFT JOIN public.anni_dwh_fact_passport_blacklist blacklist
	ON blacklist.passport_num = clients.passport_num
	AND tr.trans_date >= blacklist.entry_dt
	
	WHERE (clients.passport_valid_to IS NOT NULL
		AND clients.passport_valid_to < tr.trans_date )
		
		OR blacklist.passport_num IS NOT null

	order by tr.trans_date asc
"""

# 2.Совершение операции при недействующем договоре
tmp_2 = """
	SELECT 
	    tr.trans_date as event_dt,
	    clients.passport_num as passport,
	    CONCAT(last_name,' ', first_name,' ', patronymic) as fio,
	    clients.phone as phone,
	    2 as event_type,
	    now() :: date as report_dt
	FROM public.anni_dwh_fact_transactions tr
	
	INNER JOIN public.anni_dwh_dim_cards_hist cards
	ON tr.card_num = cards.card_num
	AND tr.trans_date between cards.effective_from AND cards.effective_to
	AND cards.deleted_flag = 0
	
	INNER JOIN public.anni_dwh_dim_accounts_hist accounts
	ON cards.account_num = accounts.account_num
	AND tr.trans_date BETWEEN accounts.effective_from AND accounts.effective_to
	AND accounts.deleted_flag = 0
	
	INNER JOIN public.anni_dwh_dim_clients_hist clients
	ON accounts.client = clients.client_id
	AND tr.trans_date BETWEEN clients.effective_from AND clients.effective_to
	AND clients.deleted_flag = 0
	
	WHERE accounts.valid_to < tr.trans_date

	order by tr.trans_date asc
"""

# 3.Совершение операций в разных городах в течение одного часа.
tmp_3 = """
	WITH tmp_3 AS (
		SELECT 
			tr.trans_date as event_dt,
			tr.card_num as card,
			clients.passport_num as passport,
			CONCAT(last_name,' ', first_name,' ', patronymic) as fio,
			clients.phone as phone,
			3 as event_type,
			now() :: date as report_dt,
			tr.trans_date - LAG(tr.trans_date,1) OVER (PARTITION BY tr.card_num order by tr.trans_date asc ) as dif_time,
			terminals.terminal_city as city,
			LAG(terminals.terminal_city,1) OVER (PARTITION BY tr.card_num order by tr.trans_date asc) prev_city
		FROM public.anni_dwh_fact_transactions tr
		
		INNER JOIN public.anni_dwh_dim_cards_hist cards
		ON tr.card_num = cards.card_num
		AND tr.trans_date between cards.effective_from AND cards.effective_to
		AND tr.oper_type = 'WITHDRAW'
		AND cards.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_accounts_hist accounts
		ON cards.account_num = accounts.account_num
		AND tr.trans_date BETWEEN accounts.effective_from AND accounts.effective_to
		AND accounts.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_clients_hist clients
		ON accounts.client = clients.client_id
		AND tr.trans_date BETWEEN clients.effective_from AND clients.effective_to
		AND clients.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_terminals_hist terminals
		ON tr.terminal = terminals.terminal_id
		AND tr.trans_date BETWEEN terminals.effective_from AND terminals.effective_to
		and terminals.deleted_flag = 0
	)

	SELECT 
		event_dt,
		passport,
		fio,
		phone,
		event_type,
		report_dt
	from tmp_3 
	where tmp_3.dif_time is not null and tmp_3.dif_time <= interval '1 hour'
		and tmp_3.prev_city <> city
	order by tmp_3.card, tmp_3.event_dt asc
"""

#4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей,
# при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.
tmp_4 = """
	with tmp_4 AS(
		SELECT 
			tr.trans_date as event_dt,
			tr.card_num as card,
			clients.passport_num as passport,
			CONCAT(last_name,' ', first_name,' ', patronymic) as fio,
			clients.phone as phone,
			4 as event_type,
			now() :: date as report_dt,
			tr.trans_date - LAG(tr.trans_date,3) OVER w as dif_time_lag3,

			tr.amt as amt,
			LAG(tr.amt, 1) OVER w as dif_amt_lag1,
			LAG(tr.amt, 2) OVER w as dif_amt_lag2,
			LAG(tr.amt, 3) OVER w as dif_amt_lag3,
			
			tr.oper_result as oper_result,
			LAG(tr.oper_result, 1) OVER w as dif_oper_result_lag1,
			LAG(tr.oper_result, 2) OVER w as dif_oper_result_lag2,
			LAG(tr.oper_result, 3) OVER w as dif_oper_result_lag3,

			tr.oper_type as oper_type,
			LAG(tr.oper_type, 1) OVER w as dif_oper_type_lag1,
			LAG(tr.oper_type, 2) OVER w as dif_oper_type_lag2,
			LAG(tr.oper_type, 3) OVER w as dif_oper_type_lag3
		FROM public.anni_dwh_fact_transactions tr
		
		INNER JOIN public.anni_dwh_dim_cards_hist cards
		ON tr.card_num = cards.card_num
		AND tr.trans_date between cards.effective_from AND cards.effective_to
		AND cards.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_accounts_hist accounts
		ON cards.account_num = accounts.account_num
		AND tr.trans_date BETWEEN accounts.effective_from AND accounts.effective_to
		AND accounts.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_clients_hist clients
		ON accounts.client = clients.client_id
		AND tr.trans_date BETWEEN clients.effective_from AND clients.effective_to
		AND clients.deleted_flag = 0
		
		INNER JOIN public.anni_dwh_dim_terminals_hist terminals
		ON tr.terminal = terminals.terminal_id
		AND tr.trans_date BETWEEN terminals.effective_from AND terminals.effective_to
		and terminals.deleted_flag = 0

		WINDOW w AS (PARTITION BY tr.card_num order by tr.trans_date asc)
	)

	select 	
		event_dt,
		passport,
		fio,
		phone,
		event_type,
		report_dt
	from tmp_4 
	where
	  	tmp_4.dif_time_lag3 is not null and tmp_4.dif_time_lag3 <= interval '20 MINUTE'

		and tmp_4.amt < tmp_4.dif_amt_lag1 
		and tmp_4.dif_amt_lag1 < tmp_4.dif_amt_lag2 
		and tmp_4.dif_amt_lag2 < tmp_4.dif_amt_lag3

		and tmp_4.oper_result = 'SUCCESS' 
		and tmp_4.dif_oper_result_lag1 = 'REJECT' 
		and tmp_4.dif_oper_result_lag2 = 'REJECT' 
		and tmp_4.dif_oper_result_lag3 = 'REJECT'

		and tmp_4.oper_type = 'WITHDRAW' 
		and tmp_4.dif_oper_type_lag1 = 'WITHDRAW' 
		and tmp_4.dif_oper_type_lag2 = 'WITHDRAW' 
		and tmp_4.dif_oper_type_lag3 = 'WITHDRAW'

	order by tmp_4.card, tmp_4.event_dt asc
"""

def process_fraud_table(sqlalchemy_conn):
	for query in [tmp_1, tmp_2, tmp_3, tmp_4]:
		sqlalchemy_conn.execute(text(f"""
			INSERT INTO public.anni_rep_fraud(
				event_dt,
				passport,
				fio,
				phone,
				event_type,
				report_dt
			)
			SELECT 
				subquery.event_dt,
				subquery.passport,
				subquery.fio,
				subquery.phone,
				subquery.event_type,
				subquery.report_dt
			FROM ({query}) subquery
			ON CONFLICT DO NOTHING;  
		"""))

	sqlalchemy_conn.commit()
