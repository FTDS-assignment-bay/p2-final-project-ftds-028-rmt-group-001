def loadData():
    import pandas as pd
    import psycopg2 as db

    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    cur = conn.cursor()

    sql = '''
        CREATE TABLE IF NOT EXISTS table_final_project_clean (
            id VARCHAR(225),
            brand VARCHAR(225),
            category VARCHAR(255),
            name VARCHAR(255),
            size VARCHAR(255),
            rating FLOAT,
            number_of_reviews INTEGER,
            love INTEGER,
            price FLOAT,
            value_price FLOAT,
            url VARCHAR,
            marketingflags VARCHAR(225),
            marketingflags_content VARCHAR(225),
            options VARCHAR,
            details VARCHAR,
            how_to_use VARCHAR,
            ingredients VARCHAR,
            online_only INTEGER,
            exclusive INTEGER,
            limited_edition INTEGER,
            limited_time_offer INTEGER,
            new_category VARCHAR(225),
            details_category VARCHAR,
            preprocessing_details_category VARCHAR
        )
    '''
    cur.execute(sql)
    conn.commit()

    df= pd.read_csv('/opt/airflow/data/product_data_clean.csv')
    for index, row in df.iterrows():
        insert_query = "INSERT INTO table_final_project_clean (id, brand, category, name, size, rating, number_of_reviews, love, price, value_price, url, marketingflags, marketingflags_content, options, details, how_to_use, ingredients, online_only, exclusive, limited_edition, limited_time_offer, new_category, details_category, preprocessing_details_category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        values = list(row)
        cur.execute(insert_query, values)

    conn.commit()
    conn.close()   