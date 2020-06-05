from sqlalchemy import create_engine
import pandas as pd
import numpy as np

old_data = pd.read_pickle("all_before_10354015_ver4.pkl")
old_data_df = pd.DataFrame.from_dict(old_data, orient='index')

with open('_product_names', 'r') as product_names_file:
    product_names = [line.rstrip('\n') for line in product_names_file]


def transform_to_one_hot(x):
    result = np.zeros(len(product_ids), dtype=bool)
    for elem in list(x):
        result[product_ids.index(elem)] = True
    return result


one_hot_column = old_data_df['product'].map(transform_to_one_hot)

new_data_df = pd.DataFrame(np.array([np.array(i) for i in one_hot_column.values]),
                           columns=product_ids, index=old_data_df.index)

# _db_string pattern
# postgres://user:password@server:port/table_name
with open('_db_string', 'r') as db_string_file:
    db_string = db_string_file.readline()
engine = create_engine(db_string, pool_pre_ping=True)
connection = engine.connect()
new_data_df.to_sql('response', connection, schema='public', if_exists='append',
                   index=True, index_label='response_id')
