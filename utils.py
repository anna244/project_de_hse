from sqlalchemy import text
import pandas as pd
import re
from datetime import datetime


def process_transaction_file(sqlalchemy_conn, file_object):
    df = pd.read_csv(file_object, sep=';')
    df = df.rename(columns={
        'transaction_id': 'trans_id',
        'transaction_date': 'trans_date',
        'amount': 'amt',
    })
    df['update_dt'] = datetime.strptime(re.search('\d+', file_object.name)[0],'%d%m%Y').date()
    df['amt'] = df['amt'].replace(',', '.', regex=True)

    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_transactions;")
    )

    df.to_sql(
        name='anni_stg_transactions',
        con=sqlalchemy_conn,
        schema='public',
        if_exists='append',
        index=False
    )

    sqlalchemy_conn.execute(text("""
        INSERT INTO public.anni_dwh_fact_transactions (
            trans_id,
            trans_date,
            oper_type,
            card_num ,
            amt, 
            oper_result,
            terminal 
        ) 
        SELECT
            trans_id,
            trans_date,
            oper_type,
            card_num ,
            amt, 
            oper_result,
            terminal                
        FROM anni_stg_transactions;
    """))

    sqlalchemy_conn.commit()

def process_passport_file(sqlalchemy_conn, file_object):
    df = pd.read_excel(file_object, sheet_name = 'blacklist')

    df = df.rename(columns={
        'passport': 'passport_num',
        'date': 'entry_dt',
    })
    date_passport = datetime.strptime(re.search('\d+', file_object.name)[0],'%d%m%Y')
    df = df[df.entry_dt == date_passport]
    df['update_dt'] = date_passport.date()

    # Очищение стейдж таблиц 
    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_blacklist;")
    )
    # Вставка данных в стейдж
    df.to_sql(
        name='anni_stg_blacklist',
        con=sqlalchemy_conn,
        schema='public',
        if_exists='append',
        index=False
    )

    #Вставка из стейдж в фактовую таблицу
    sqlalchemy_conn.execute(text("""
        INSERT INTO public.anni_dwh_fact_passport_blacklist (
            passport_num,
            entry_dt  
        ) 
        SELECT
            passport_num,
            entry_dt              
        FROM anni_stg_blacklist;
    """))
    sqlalchemy_conn.commit()

def process_terminal_file(sqlalchemy_conn, file_object):
    df = pd.read_excel(file_object, sheet_name = 'terminals')

    date_terminals = datetime.strptime(re.search('\d+', file_object.name)[0],'%d%m%Y')
    df['update_dt'] = date_terminals.date()

    # Очищение стейдж таблиц 
    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_terminals;")
    )
    # Вставка данных в стейдж
    df.to_sql(
        name='anni_stg_terminals',
        con=sqlalchemy_conn,
        schema='public',
        if_exists='append',
        index=False
    )

    #Захват в стейджинг ключей из источника полным срезом для вычисления удалений
    sqlalchemy_conn.execute(text("""
        DELETE FROM public.anni_stg_del_project WHERE schema_name = 'public' AND table_name = 'terminal';
                                    
        INSERT INTO public.anni_stg_del_project(id, schema_name, table_name)
        SELECT 
            terminal_id,
            'public',
            'terminal'
        FROM public.anni_stg_terminals;
    """))                                      

    #Загрузка в приемник "вставок"  (формат SCD2).
    sqlalchemy_conn.execute(text("""
        INSERT INTO public.anni_dwh_dim_terminals_hist( 
            terminal_id,
            terminal_type,
            terminal_city, 
            terminal_address,
            effective_from,
            effective_to )
        SELECT 
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city, 
            stg.terminal_address,
            stg.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM public.anni_stg_terminals stg
        LEFT JOIN  public.anni_dwh_dim_terminals_hist tgt
        ON stg.terminal_id = tgt.terminal_id
        WHERE tgt.terminal_id IS NULL;
    """))

    base_subquery = """
        SELECT
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city, 
            stg.terminal_address,
            stg.update_dt
        FROM public.anni_stg_terminals stg
        INNER JOIN public.anni_dwh_dim_terminals_hist tgt
        ON stg.terminal_id = tgt.terminal_id
        WHERE 
            (
                stg.terminal_type <> tgt.terminal_type 
                OR (stg.terminal_type is null and tgt.terminal_type is not null ) 
                OR ( stg.terminal_type is not null and tgt.terminal_type is null )

                OR stg.terminal_city <> tgt.terminal_city 
                OR (stg.terminal_city is null and tgt.terminal_city is not null ) 
                OR ( stg.terminal_city is not null and tgt.terminal_city is null )

                OR stg.terminal_address <> tgt.terminal_address 
                OR (stg.terminal_address is null and tgt.terminal_address is not null ) 
                OR (stg.terminal_address is not null and tgt.terminal_address is null )
            )
            AND {extra_where}
    """

    subquery_1 = base_subquery.format(
        extra_where="tgt.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')"
    )

    subquery_2 = base_subquery.format(
        extra_where="tgt.effective_to = (stg.update_dt - interval '1 second')"
    )

    # Обновление (формат SCD2).
    # Закрытие по дате старой записи
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_dwh_dim_terminals_hist
        SET 
            effective_to = (tmp.update_dt - interval '1 second')
        FROM ({subquery_1}) tmp
        WHERE 
            public.anni_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
            AND public.anni_dwh_dim_terminals_hist.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')
    """))

    # Вставка-обновление (формат SCD2)
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_dwh_dim_terminals_hist( 
            terminal_id,
            terminal_type,
            terminal_city, 
            terminal_address,
            effective_from,
            effective_to)
        SELECT 
            tmp.terminal_id,
            tmp.terminal_type,
            tmp.terminal_city, 
            tmp.terminal_address,
            tmp.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM ({subquery_2}) tmp;
    """)) 

    #Удаление записей в таргете (формат SCD2).
    sqlalchemy_conn.execute(text(f"""
        WITH tmp AS (
            SELECT 
                tgt.terminal_id, 
                max(tgt.effective_to) as max_end_dt
            FROM public.anni_dwh_dim_terminals_hist tgt
            LEFT JOIN public.anni_stg_del_project stg
            ON stg.id = tgt.terminal_id AND stg.table_name = 'terminal'
            WHERE stg.id is null 
            GROUP BY tgt.terminal_id
        )

        UPDATE public.anni_dwh_dim_terminals_hist
        SET 
            effective_to = '{date_terminals.strftime('%Y-%m-%d %H:%M:%S')}',
            deleted_flag = 1
        FROM tmp
        WHERE public.anni_dwh_dim_terminals_hist.effective_to = tmp.max_end_dt and public.anni_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id;
    """)) 

    #Вставка начальных значений в таблицу метаданных.
    sqlalchemy_conn.execute(text("""
        INSERT INTO public.anni_meta_load(
            schema_name, 
            table_name,
            max_update_dt )
        VALUES( 
            'public',
            'terminal',
            to_timestamp('1900-01-01','YYYY-MM-DD'))
        ON CONFLICT DO NOTHING;
    """)) 

    #Обновление метаданных.
    sqlalchemy_conn.execute(text("""
        UPDATE public.anni_meta_load
        SET 
            max_update_dt = coalesce( 
                (select max(update_dt) from public.anni_stg_terminals), 
                (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='terminal')
            )
        where schema_name='public' and table_name = 'terminal';                                
    """))  

    sqlalchemy_conn.commit()     

def process_accounts_table(sqlalchemy_conn, table):

    #Вставка начальных значений в таблицу метаданных.
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_meta_load(
            schema_name, 
            table_name,
            max_update_dt )
        VALUES( 
            'public',
            '{table}',
            to_timestamp('1800-01-01','YYYY-MM-DD'))
        ON CONFLICT DO NOTHING;
    """))     

    # Очищение стейдж таблиц 
    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_accounts;")
    )

    # Вставка данных в стейдж таблицу
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_stg_accounts( 
            account_num,
	        valid_to,
	        client,
            update_dt)
        SELECT 
            account,
            valid_to,
            client, 
            greatest(coalesce(create_dt, '1900-01-01'), coalesce(update_dt, '1900-01-01')) 
        FROM {table}
        WHERE 
        greatest(coalesce(create_dt, '1900-01-01'), coalesce(update_dt, '1900-01-01')) >
        coalesce( (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}'),
                (to_timestamp ('1800-12-31', 'YYYY-MM-DD')) );
    """))

    #Захват в стейджинг ключей из источника полным срезом для вычисления удалений
    sqlalchemy_conn.execute(text(f"""
        DELETE FROM public.anni_stg_del_project WHERE schema_name = 'public' AND table_name = '{table}';
                                    
        INSERT INTO public.anni_stg_del_project(id, schema_name, table_name)
        SELECT 
            account,
            'public',
            '{table}'
        FROM {table};
    """))                                      

    #Загрузка в приемник "вставок"  (формат SCD2).
    sqlalchemy_conn.execute(text("""
        INSERT INTO anni_dwh_dim_accounts_hist( 
            account_num,
	        valid_to,
	        client,
            effective_from,
            effective_to )
        SELECT 
            stg.account_num,
            stg.valid_to,
            stg.client, 
            stg.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM public.anni_stg_accounts stg
        LEFT JOIN  public.anni_dwh_dim_accounts_hist tgt
        ON stg.account_num = tgt.account_num
        WHERE tgt.account_num IS NULL;
    """))

    base_subquery = """
        SELECT
            stg.account_num,
            stg.valid_to,
            stg.client, 
            stg.update_dt
        FROM public.anni_stg_accounts stg
        INNER JOIN public.anni_dwh_dim_accounts_hist tgt
        ON stg.account_num = tgt.account_num
        WHERE 
            (
                stg.valid_to <> tgt.valid_to 
                OR (stg.valid_to is null and tgt.valid_to is not null ) 
                OR ( stg.valid_to is not null and tgt.valid_to is null )

                OR stg.client <> tgt.client 
                OR (stg.client is null and tgt.client is not null ) 
                OR ( stg.client is not null and tgt.client is null )
            )
            AND {extra_where}
    """

    subquery_1 = base_subquery.format(
        extra_where="tgt.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')" #если данные поменялись напрмер в 3ей итерпции на первончашьные
    )

    subquery_2 = base_subquery.format(
        extra_where="tgt.effective_to = (stg.update_dt - interval '1 second')"
    )

    # Обновление (формат SCD2).
    # Закрытие по дате старой записи
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_dwh_dim_accounts_hist
        SET 
            effective_to = (tmp.update_dt - interval '1 second')
        FROM ({subquery_1}) tmp
        WHERE 
            public.anni_dwh_dim_accounts_hist.account_num = tmp.account_num
            AND public.anni_dwh_dim_accounts_hist.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')
    """))

    # Вставка-обновление (формат SCD2)
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_dwh_dim_accounts_hist( 
            account_num,
	        valid_to,
	        client,
            effective_from,
            effective_to)
        SELECT 
            tmp.account_num,
            tmp.valid_to,
            tmp.client, 
            tmp.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM ({subquery_2}) tmp;
    """)) 

    #Удаление записей в таргете (формат SCD2).
    sqlalchemy_conn.execute(text(f"""
        WITH tmp AS (
            SELECT 
                tgt.account_num, 
                MAX(tgt.effective_to) as max_end_dt                           
            FROM public.anni_dwh_dim_accounts_hist tgt
            LEFT JOIN public.anni_stg_del_project stg
            ON stg.id = tgt.account_num AND stg.table_name = '{table}'
            WHERE stg.id is null 
            GROUP BY tgt.account_num
        )

        UPDATE public.anni_dwh_dim_accounts_hist
        SET 
            effective_to = now(),
            deleted_flag = 1
        FROM tmp
        WHERE public.anni_dwh_dim_accounts_hist.effective_to = tmp.max_end_dt 
            AND public.anni_dwh_dim_accounts_hist.account_num = tmp.account_num;
    """)) 

    #Обновление метаданных.
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_meta_load
        SET 
            max_update_dt = coalesce( 
                (select max(update_dt) from public.anni_stg_accounts), 
                (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}')
            )
        where schema_name='public' and table_name = '{table}';                                
    """))  

    sqlalchemy_conn.commit() 

def process_cards_table(sqlalchemy_conn, table):

    #Вставка начальных значений в таблицу метаданных.
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_meta_load(
            schema_name, 
            table_name,
            max_update_dt )
        VALUES( 
            'public',
            '{table}',
            to_timestamp('1800-01-01','YYYY-MM-DD'))
        ON CONFLICT DO NOTHING;
    """))     

    # Очищение стейдж таблиц 
    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_cards;")
    )

    # Вставка данных в стейдж таблицу
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_stg_cards( 
            card_num ,
	        account_num ,
            update_dt)
        SELECT 
            card_num ,
            account,
            greatest(coalesce(create_dt, '2001-01-01'), coalesce(update_dt, '2001-01-01')) 
        FROM {table}
        WHERE 
        greatest(coalesce(create_dt, '2001-01-01'), coalesce(update_dt, '2001-01-01')) >
        coalesce( (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}'),
                (to_timestamp ('1800-12-31', 'YYYY-MM-DD')) );
    """))

    #Захват в стейджинг ключей из источника полным срезом для вычисления удалений
    sqlalchemy_conn.execute(text(f"""
        DELETE FROM public.anni_stg_del_project WHERE schema_name = 'public' AND table_name = '{table}';
                                    
        INSERT INTO public.anni_stg_del_project(id, schema_name, table_name)
        SELECT 
            card_num,
            'public',
            '{table}'
        FROM {table};
    """))                                      

    #Загрузка в приемник "вставок"  (формат SCD2).
    sqlalchemy_conn.execute(text("""
        INSERT INTO anni_dwh_dim_cards_hist( 
            card_num ,
	        account_num ,
            effective_from,
            effective_to )
        SELECT 
            stg.card_num,
            stg.account_num, 
            stg.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM public.anni_stg_cards stg
        LEFT JOIN  public.anni_dwh_dim_cards_hist tgt
        ON stg.card_num = tgt.card_num
        WHERE tgt.card_num IS NULL;
    """))

    base_subquery = """
        SELECT
            stg.card_num,
            stg.account_num,
            stg.update_dt
        FROM public.anni_stg_cards stg
        INNER JOIN public.anni_dwh_dim_cards_hist tgt
        ON stg.card_num = tgt.card_num
        WHERE 
            (
                stg.account_num <> tgt.account_num 
                OR (stg.account_num is null and tgt.account_num is not null ) 
                OR ( stg.account_num is not null and tgt.account_num is null )
            )
            AND {extra_where}
    """

    subquery_1 = base_subquery.format(
        extra_where="tgt.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')" #если данные поменялись напрмер в 3ей итерпции на первончашьные
    )

    subquery_2 = base_subquery.format(
        extra_where="tgt.effective_to = (stg.update_dt - interval '1 second')"
    )

    # Обновление (формат SCD2).
    # Закрытие по дате старой записи
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_dwh_dim_cards_hist
        SET 
            effective_to = (tmp.update_dt - interval '1 second')
        FROM ({subquery_1}) tmp
        WHERE 
            public.anni_dwh_dim_cards_hist.card_num = tmp.card_num
            AND public.anni_dwh_dim_cards_hist.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')
    """))

    # Вставка-обновление (формат SCD2)
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_dwh_dim_cards_hist( 
            card_num,
            account_num,
            effective_from,
            effective_to)
        SELECT 
            tmp.card_num,
            tmp.account_num,
            tmp.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM ({subquery_2}) tmp;
    """)) 

    #Удаление записей в таргете (формат SCD2).
    sqlalchemy_conn.execute(text(f"""
        WITH tmp AS (
            SELECT 
                tgt.card_num, 
                MAX(tgt.effective_to) as max_end_dt                           
            FROM public.anni_dwh_dim_cards_hist tgt
            LEFT JOIN public.anni_stg_del_project stg
            ON stg.id = tgt.card_num AND stg.table_name = '{table}'
            WHERE stg.id is null 
            GROUP BY tgt.card_num
        )

        UPDATE public.anni_dwh_dim_cards_hist
        SET 
            effective_to = now(),
            deleted_flag = 1
        FROM tmp
        WHERE public.anni_dwh_dim_cards_hist.effective_to = tmp.max_end_dt 
            AND public.anni_dwh_dim_cards_hist.card_num = tmp.card_num;
    """)) 

    #Обновление метаданных.
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_meta_load
        SET 
            max_update_dt = coalesce( 
                (select max(update_dt) from public.anni_stg_cards), 
                (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}')
            )
        where schema_name='public' and table_name = '{table}';                                
    """))  

    sqlalchemy_conn.commit() 

def process_clients_table(sqlalchemy_conn, table):

    #Вставка начальных значений в таблицу метаданных.
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_meta_load(
            schema_name, 
            table_name,
            max_update_dt )
        VALUES( 
            'public',
            '{table}',
            to_timestamp('1800-01-01','YYYY-MM-DD'))
        ON CONFLICT DO NOTHING;
    """))     

    # Очищение стейдж таблиц 
    sqlalchemy_conn.execute(
        text("truncate table public.anni_stg_clients;")
    )

    # Вставка данных в стейдж таблицу
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_stg_clients( 
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            update_dt)
        SELECT 
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            greatest(coalesce(create_dt, '1900-01-01'), coalesce(update_dt, '1900-01-01')) 
        FROM {table}
        WHERE 
        greatest(coalesce(create_dt, '1900-01-01'), coalesce(update_dt, '1900-01-01')) >
        coalesce( (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}'),
                (to_timestamp ('1800-12-31', 'YYYY-MM-DD')) );
    """))

    #Захват в стейджинг ключей из источника полным срезом для вычисления удалений
    sqlalchemy_conn.execute(text(f"""
        DELETE FROM public.anni_stg_del_project WHERE schema_name = 'public' AND table_name = '{table}';
                                    
        INSERT INTO public.anni_stg_del_project(id, schema_name, table_name)
        SELECT 
            client_id,
            'public',
            '{table}'
        FROM {table};
    """))                                      

    #Загрузка в приемник "вставок"  (формат SCD2).
    sqlalchemy_conn.execute(text("""
        INSERT INTO anni_dwh_dim_clients_hist( 
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            effective_from,
            effective_to )
        SELECT 
            stg.client_id,
            stg.last_name, 
            stg.first_name,
            stg.patronymic,
            stg.date_of_birth,
            stg.passport_num,
            stg.passport_valid_to,
            stg.phone,                                                      
            stg.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM public.anni_stg_clients stg
        LEFT JOIN  public.anni_dwh_dim_clients_hist tgt
        ON stg.client_id = tgt.client_id
        WHERE tgt.client_id IS NULL;
    """))

    base_subquery = """
        SELECT
            stg.client_id,
            stg.last_name, 
            stg.first_name,
            stg.patronymic,
            stg.date_of_birth,
            stg.passport_num,
            stg.passport_valid_to,
            stg.phone, 
            stg.update_dt
        FROM public.anni_stg_clients stg
        INNER JOIN public.anni_dwh_dim_clients_hist tgt
        ON stg.client_id = tgt.client_id
        WHERE 
            (
                stg.last_name <> tgt.last_name
                OR (stg.last_name is null and tgt.last_name is not null ) 
                OR ( stg.last_name is not null and tgt.last_name is null )

                OR stg.first_name <> tgt.first_name
                OR (stg.first_name is null and tgt.first_name is not null ) 
                OR ( stg.first_name is not null and tgt.first_name is null )

                OR stg.patronymic <> tgt.patronymic
                OR (stg.patronymic is null and tgt.patronymic is not null ) 
                OR ( stg.patronymic is not null and tgt.patronymic is null )

                OR stg.date_of_birth <> tgt.date_of_birth
                OR (stg.date_of_birth is null and tgt.date_of_birth is not null ) 
                OR ( stg.date_of_birth is not null and tgt.date_of_birth is null )

                OR stg.passport_num <> tgt.passport_num
                OR (stg.passport_num is null and tgt.passport_num is not null ) 
                OR ( stg.passport_num is not null and tgt.passport_num is null ) 

                OR stg.passport_valid_to <> tgt.passport_valid_to
                OR (stg.passport_valid_to is null and tgt.passport_valid_to is not null ) 
                OR ( stg.passport_valid_to is not null and tgt.passport_valid_to is null ) 

                OR stg.phone <> tgt.phone
                OR (stg.phone is null and tgt.phone is not null ) 
                OR ( stg.phone is not null and tgt.phone is null )                                                             

            )
            AND {extra_where}
    """

    subquery_1 = base_subquery.format(
        extra_where="tgt.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')" #если данные поменялись напрмер в 3ей итерпции на первончашьные
    )

    subquery_2 = base_subquery.format(
        extra_where="tgt.effective_to = (stg.update_dt - interval '1 second')"
    )

    # Обновление (формат SCD2).
    # Закрытие по дате старой записи
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_dwh_dim_clients_hist
        SET 
            effective_to = (tmp.update_dt - interval '1 second')
        FROM ({subquery_1}) tmp
        WHERE 
            public.anni_dwh_dim_clients_hist.client_id = tmp.client_id
            AND public.anni_dwh_dim_clients_hist.effective_to >= to_timestamp ('2990-12-31', 'YYYY-MM-DD')
    """))

    # Вставка-обновление (формат SCD2)
    sqlalchemy_conn.execute(text(f"""
        INSERT INTO public.anni_dwh_dim_clients_hist( 
            client_id,
            last_name, 
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,                      
            effective_from,
            effective_to)
        SELECT 
            tmp.client_id,
            tmp.last_name, 
            tmp.first_name,
            tmp.patronymic,
            tmp.date_of_birth,
            tmp.passport_num,
            tmp.passport_valid_to,
            tmp.phone,                      
            tmp.update_dt, 
            to_timestamp ('2990-12-31', 'YYYY-MM-DD')
        FROM ({subquery_2}) tmp;
    """)) 

    #Удаление записей в таргете (формат SCD2).
    sqlalchemy_conn.execute(text(f"""
        WITH tmp AS (
            SELECT 
                tgt.client_id, 
                MAX(tgt.effective_to) as max_end_dt                           
            FROM public.anni_dwh_dim_clients_hist tgt
            LEFT JOIN public.anni_stg_del_project stg
            ON stg.id = tgt.client_id AND stg.table_name = '{table}'
            WHERE stg.id is null 
            GROUP BY tgt.client_id
        )

        UPDATE public.anni_dwh_dim_clients_hist
        SET 
            effective_to = now(),
            deleted_flag = 1
        FROM tmp
        WHERE public.anni_dwh_dim_clients_hist.effective_to = tmp.max_end_dt 
            AND public.anni_dwh_dim_clients_hist.client_id = tmp.client_id;
    """)) 

    #Обновление метаданных.
    sqlalchemy_conn.execute(text(f"""
        UPDATE public.anni_meta_load
        SET 
            max_update_dt = coalesce( 
                (select max(update_dt) from public.anni_stg_clients), 
                (select max_update_dt from public.anni_meta_load where schema_name='public' and table_name='{table}')
            )
        where schema_name='public' and table_name = '{table}';                                
    """))  

    sqlalchemy_conn.commit() 