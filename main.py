from sqlalchemy import create_engine, text
from pathlib import Path
import utils
import fraud

dir_source = Path("./data")
dir_destination = Path("./archive")

files_transactions = sorted(list(dir_source.glob("transactions_*.txt")))
files_passport = sorted(list(dir_source.glob("passport_blacklist_*.xlsx"))) 
files_terminals = sorted(list(dir_source.glob("terminals_*.xlsx")))

if not files_transactions and not files_passport and not files_terminals:
    exit()

#dialect+driver://username:password@host:port/database
engine = create_engine('postgresql+psycopg2://hse:hsepassword@rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net:6432/db')
sqlalchemy_conn  = engine.connect()

# Отключение автокоммита
sqlalchemy_conn.autocommit = False

ddl_file = Path('./main.ddl')
sqlalchemy_conn.execute(
    text(ddl_file.read_text())
)
sqlalchemy_conn.commit()

#####################################################################################################
### Загрузка в stg и fact

# table_transactions
if files_transactions:
    for f in files_transactions:
        utils.process_transaction_file(sqlalchemy_conn, f)

        #Перемещение файла в архив
        f.rename(dir_destination/f.with_suffix('.backup').name)

# table_passport 
if files_passport:
    for f in files_passport:
        utils.process_passport_file(sqlalchemy_conn, f)

        # Перемещение файла в архив
        f.rename(dir_destination/f.with_suffix('.backup').name)

# table_terminals
if files_terminals:
    for f in files_terminals:
        utils.process_terminal_file(sqlalchemy_conn, f)                                        
         
        # Перемещение файла в архив
        f.rename(dir_destination/f.with_suffix('.backup').name)

# table_account
utils.process_accounts_table(sqlalchemy_conn, 'info.accounts')                                        

#table_cards
utils.process_cards_table(sqlalchemy_conn, 'info.cards') 

# table_clients
utils.process_clients_table(sqlalchemy_conn, 'info.clients') 

# table_fraud
fraud.process_fraud_table(sqlalchemy_conn)

sqlalchemy_conn.close()