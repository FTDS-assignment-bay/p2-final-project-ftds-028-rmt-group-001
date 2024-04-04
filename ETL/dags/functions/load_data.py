def loadData():
    '''
    This function creates a table in a PostgreSQL database if it doesn't exist already, then loads data from 
    a CSV file into that table. It uses psycopg2 for database connection and pandas for data manipulation.
    '''
    # Import the pandas library for data manipulation
    import pandas as pd
    # Import the psycopg2 library for connecting to the PostgreSQL database
    import psycopg2 as db

    # Define the connection string with the necessary parameters (dbname, host, user, password, port)
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    # Establish a connection to the PostgreSQL database using psycopg2 and the specified connection string
    conn=db.connect(conn_string)
    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    # SQL query to create a table if it doesn't exist already, with specified column names and data types
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
    # Execute the SQL query to create the table
    cur.execute(sql)
    # Commit the transaction to save the changes
    conn.commit()

    # Read data from the CSV file containing cleaned data
    df= pd.read_csv('/opt/airflow/data/sephora_website_clean.csv')
    # Iterate over each row in the DataFrame and insert it into the database table
    for index, row in df.iterrows():
        # SQL query to insert a row into the table with placeholders for values
        insert_query = "INSERT INTO table_final_project_clean (id, brand, category, name, size, rating, number_of_reviews, love, price, value_price, url, marketingflags, marketingflags_content, options, details, how_to_use, ingredients, online_only, exclusive, limited_edition, limited_time_offer, new_category, details_category, preprocessing_details_category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        # Get the values from the current row and convert them into a list
        values = list(row)
        # Execute the SQL query with the values to insert the row into the table
        cur.execute(insert_query, values)

    # Commit the transaction to save the changes
    conn.commit()
    # Close the database connection
    conn.close()   