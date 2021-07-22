# Imports
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from sqlalchemy.sql import text 
from sqlalchemy import insert
import csv 
import numpy as np

# Create connection to database
engine_db = create_engine('postgresql+psycopg2://postgres:genomicsdb@localhost/dbs_cohorts')
engine_index = create_engine('postgresql+psycopg2://cdt_user:Daws0n_H&ll@cln-cdt-ddf-index-01.gel.zone/cohorts')
engine_dams = create_engine('postgresql+psycopg2://cdt_user:Daws0n_H&ll@cln-cdt-dams-postgresql-01.gel.zone/labkey')

# From CSV Direct
# csv_path = r'./request_data/mild_request_file.csv'
# df = pd.read_csv(csv_path)

# From DB
with engine_db.connect() as c:
    resultproxy = c.execute("SELECT * FROM request.request_landing")
    

df_dict = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
df = pd.DataFrame(df_dict)

# Get seq no

with engine_index.connect() as c:
    resultproxy = c.execute("SELECT max(file_sequence_number) from dbs.batch_request_tracker")

temp = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
seq_dict = temp[0]

# Create Header and Footer
cur_datetime = '140000' + str(datetime.today().strftime('%Y%m%d'))
org_code = '8J834'
df_len = str(df.shape[0])
num_records = df_len.zfill(6)
prev_seq = seq_dict['max']
seq_no = str(prev_seq + 1)
seq_no_raw = seq_no
seq_no = seq_no.zfill(8)

df_cols = df.shape[1]
header_row = []
footer_row = []
for i in range(df_cols):
    header_row.append('')
    footer_row.append('')



header_row_first = '0001011' + org_code + '         DBS           ' + cur_datetime + seq_no + num_records
footer_row_first = '990101' + org_code + '         DBS           ' + cur_datetime + seq_no + num_records
header_row[0] = header_row_first
footer_row[0] = footer_row_first

# Adding header and footer into df
header_dict = {}
footer_dict = {}
df_cols = list(df.columns)
for i in range(len(header_row)):
    header_dict[df_cols[i]] = header_row[i]
    footer_dict[df_cols[i]] = footer_row[i]

header_df = pd.DataFrame({k:[v] for (k,v) in header_dict.items()})
footer_df = pd.DataFrame({k:[v] for (k,v) in footer_dict.items()})

df_temp = header_df.append(df)
df_out = df_temp.append(footer_df)


# Writing out
base_path = '/tmp/batch_request_process/'
test_path = './'
curr_date = datetime.today().strftime('%Y%m%d')
ftype = '.csv'


fname = base_path + str(curr_date) + str(seq_no) + '_dbs_batch_request' + ftype
test_fname = test_path + str(curr_date) + str(seq_no) + '_dbs_batch_request' + ftype
df_out = df_out.replace(np.nan, '', regex=True)
df_out.to_csv(test_fname, header=False, index=False)

# Updating request tracker

# header_write = str(header_row[0])
# footer_write = str(footer_row[0])
# records_write = str(df_out.shape[0] - 2)


# with engine_db.connect() as conn:
#     query = text(f'INSERT INTO request.batch_request_tracker (file_sequence_number, num_records, header_string, footer_string) values ({seq_no_raw}, {records_write}, {header_write}, {footer_write})')
#     conn.execute(query)



