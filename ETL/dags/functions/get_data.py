def getData():
    import pandas as pd
    import psycopg2 as db

    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    df=pd.read_sql('select * from table_final_project', conn)
    df.to_csv('/opt/airflow/data/product_data_raw.csv', index=False)