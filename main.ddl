SET search_path TO public;

--Создание таблиц стейдж таблиц
CREATE TABLE IF NOT EXISTS anni_stg_transactions (
	trans_id varchar(50), 
	trans_date timestamp(0),
	card_num varchar(50) NULL,
	oper_type varchar(50) NULL, 
	amt decimal NOT NULL default 0, 
	oper_result varchar(50) NULL,
    terminal varchar(50) NULL,
    update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS anni_stg_terminals (
	terminal_id varchar(50) NULL,
	terminal_type varchar(50) NULL,
	terminal_city varchar(50) NULL, 
	terminal_address varchar(200) NULL,
    update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS anni_stg_blacklist (
    passport_num varchar(50) NULL,
    entry_dt date NULL,
    update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS anni_stg_cards (
    card_num varchar(50) NULL,
	account_num varchar(50) NULL,
    update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS anni_stg_accounts (
    account_num varchar(50) NULL,
	valid_to date NULL,
	client varchar(50) NULL,
    update_dt timestamp(0)
);

CREATE TABLE IF NOT EXISTS anni_stg_clients (
	client_id varchar(50) NULL,
	last_name varchar(50) NULL,
	first_name varchar(50) NULL,
	patronymic varchar(50) NULL,
	date_of_birth date NULL,
	passport_num varchar(50) NULL,
	passport_valid_to date NULL,
	phone varchar(50) NULL,
    update_dt timestamp(0)
);

--Создание фактовых таблиц

CREATE TABLE IF NOT EXISTS anni_dwh_fact_transactions (
    trans_id varchar(50), -- primary key
	trans_date timestamp(0),
	card_num varchar(50) ,
	oper_type varchar(50), 
	amt decimal , 
	oper_result varchar(50),
    terminal varchar(50) 
);

CREATE TABLE IF NOT EXISTS anni_dwh_fact_passport_blacklist (
    passport_num varchar(50), -- primary key
    entry_dt date 
); 

--Создание  таблиц SC2 формата

CREATE TABLE IF NOT EXISTS anni_dwh_dim_terminals_hist (
    terminal_id varchar(50),
	terminal_type varchar(50),
	terminal_city varchar(50), 
	terminal_address varchar(200),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flag integer default 0
);

CREATE TABLE IF NOT EXISTS anni_dwh_dim_cards_hist (
    card_num varchar(50),
	account_num varchar(50),
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flag integer default 0
);

CREATE TABLE IF NOT EXISTS anni_dwh_dim_accounts_hist (
    account_num varchar(50),
	valid_to date,
	client varchar(50) ,
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flag integer default 0
);

CREATE TABLE IF NOT EXISTS anni_dwh_dim_clients_hist (
	client_id varchar(50),
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date ,
	passport_num varchar(50),
	passport_valid_to date ,
	phone bpchar(16) ,
    effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flag integer default 0
);

-- Таблица отчета

CREATE TABLE IF NOT EXISTS anni_rep_fraud (
    -- id serial primary key,
    event_dt timestamp(0),
	passport varchar(50),
	fio varchar(50),
    phone varchar(50),
    event_type varchar(50),
    report_dt date
);

--мета_таблица
CREATE TABLE IF NOT EXISTS anni_meta_load (
    schema_name varchar(50),
    table_name varchar(50),
    max_update_dt timestamp(0)
    -- PRIMARY KEY (schema_name, table_name)
);

-- таблица stg_del
CREATE TABLE IF NOT EXISTS anni_stg_del_project( 
	id varchar(50),
    schema_name varchar(50),
    table_name varchar(50) 
);

-- https://stackoverflow.com/questions/6801919/postgres-add-constraint-if-it-doesnt-already-exist
DO $$
BEGIN

  BEGIN
    ALTER TABLE anni_dwh_fact_transactions ADD CONSTRAINT anni_dwh_fact_transactions_unique UNIQUE (trans_id);
  EXCEPTION
    WHEN duplicate_table THEN 
    WHEN duplicate_object THEN
      RAISE NOTICE 'skip';
  END;

  BEGIN
    ALTER TABLE anni_dwh_fact_passport_blacklist ADD CONSTRAINT anni_dwh_fact_passport_blacklist_unique UNIQUE (passport_num);
  EXCEPTION
    WHEN duplicate_table THEN 
    WHEN duplicate_object THEN
      RAISE NOTICE 'skip';
  END;

  BEGIN
    ALTER TABLE anni_rep_fraud ADD CONSTRAINT anni_rep_fraud_unique UNIQUE (event_dt, passport);
  EXCEPTION
    WHEN duplicate_table THEN 
    WHEN duplicate_object THEN
      RAISE NOTICE 'skip';
  END;

  BEGIN
    ALTER TABLE anni_meta_load ADD CONSTRAINT anni_meta_load_unique UNIQUE (schema_name, table_name);
  EXCEPTION
    WHEN duplicate_table THEN 
    WHEN duplicate_object THEN
      RAISE NOTICE 'skip';
  END;

END $$;