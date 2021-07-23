# Imports
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from sqlalchemy.sql import text 
from sqlalchemy import insert
import csv 
import time

engine_dams = create_engine('postgresql+psycopg2://cdt_user:Daws0n_H&ll@cln-cdt-dams-postgresql-01.gel.zone/labkey')
engine_db = create_engine('postgresql+psycopg2://postgres:genomicsdb@localhost/dbs_cohorts')

# # DAMS Test
sql_file = './raw_data/dams.sql'

with engine_dams.connect() as conn:
    file = open(sql_file)
    query = text(file.read())
    result = conn.execute(query)

# Create DataFrame

df_dict = [{column: value for column, value in row.items()} for row in result]

df_template = pd.DataFrame(df_dict)

cols = df_template.columns

csv_path = r'./raw_data/Covid_To_Trace.csv'
df = pd.read_csv(csv_path)

prep_dict = {}

nulls = []
for i in range(df.shape[0]):
    nulls.append('')

for c in cols:
    prep_dict[c] = nulls

prep_dict['record_type'] = [10 for x in range(df.shape[0])]
prep_dict['local_pid'] = list(df['dynamics_contact_id'])
prep_dict['date_of_birth'] = list(df['date_of_birth'])
prep_dict['surname'] = list(df['last_name'])
prep_dict['prev_alt_surname'] = list(df['last_name'])
prep_dict['forename'] = list(df['first_name'])
prep_dict['alternative_forename'] = list(df['first_name'])
prep_dict['sex'] = list(df['gender'])
prep_dict['pcd_postcode'] = list(df['postcode'])
prep_dict['prev_postcode'] = list(df['postcode'])

df_out = pd.DataFrame(prep_dict)

df_out.to_csv('./request_data/covid_request_file.csv', index=False)

# # Write to DB
# with engine_db.connect() as c:
#     for i in range(df.shape[0]):
#         query = text(f'INSERT INTO request.request_landing (record_type, local_pid, date_of_birth, surname, prev_alt_surname,forename, alternative_forename, sex, pcd_postcode, prev_postcode) values ({str(prep_dict["record_type"][i])}, {str(prep_dict["local_pid"][i])}, {str(prep_dict["date_of_birth"][i])}, {str(prep_dict["surname"][i])},{str(prep_dict["prev_alt_surname"][i])},{str(prep_dict["forename"][i])},{str(prep_dict["alternative_forename"][i])},{str(prep_dict["sex"][i])},{str(prep_dict["pcd_postcode"][i])}, {str(prep_dict["prev_postcode"][i])})')
#         c.execute(query)
#         time.sleep(0.1)



