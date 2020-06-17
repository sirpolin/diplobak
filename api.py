from pandas import DataFrame
import pandas as pd
#!/usr/bin/python3

# ! pip3 install sqlalchemy, psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from response import Response

# _db_string pattern:
# postgres://user:password@server:port/table_name
with open('_db_string', 'r') as db_string_file:
    db_string = db_string_file.readline()


def get_similar_responses(element_id):
    engine = create_engine(db_string, pool_recycle=60)
    Main_Session = sessionmaker(bind=engine)
    main_session = Main_Session()

    element_id_db = main_session.query(Response).filter_by(**{'response_id': element_id}).first()
    filter_expression = {'debitcards': element_id_db.debitcards,
                        'creditcards': element_id_db.creditcards,
                        'hypothec': element_id_db.hypothec,
                        'autocredits': element_id_db.autocredits,
                        'credits': element_id_db.credits,
                        'restructing': element_id_db.restructing,
                        'deposits': element_id_db.deposits,
                        'investments': element_id_db.investments,
                        'transfers': element_id_db.transfers,
                        'remote': element_id_db.remote,
                        'corporate': element_id_db.corporate,
                        'rko': element_id_db.rko,
                        'acquiring': element_id_db.acquiring,
                        'salary_project': element_id_db.salary_project,
                        'businessdeposits': element_id_db.businessdeposits,
                        'businesscredits': element_id_db.businesscredits,
                        'bank_guarantee': element_id_db.bank_guarantee,
                        'leasing': element_id_db.leasing,
                        'business_other': element_id_db.business_other,
                        'business_remote': element_id_db.business_remote}
    main_session.close()

    connection = engine.connect()
    return pd.read_sql(main_session.query(Response).filter_by(**filter_expression).statement, connection)




