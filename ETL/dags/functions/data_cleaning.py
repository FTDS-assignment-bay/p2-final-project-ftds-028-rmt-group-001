def textPreprocessing(text):
    import re
    import nltk
    
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')

    from nltk.stem import WordNetLemmatizer
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords

    # Define additional stopwords to be added to the default English stopwords list
    additional_stopwords = ['Know', 'know', 'need', 'redeemed', 'Contains', 's', 'U S', 'JCPenny Store', 'inside JCPenny']

    # Retrieve the default English stopwords list and add additional stopwords
    stpwrd = list(set(stopwords.words('english')))
    for i in additional_stopwords:
        stpwrd.append(i)

    # Initialize WordNetLemmatizer
    lemmatizer = WordNetLemmatizer()

    # Remove new line
    text = re.sub("\n" , " ", text)

    # Convert text to lowercase
    text = text.lower()

    # Remove symbols and numbers
    text = re.sub(r"[^a-zA-Z\s]", " ", text)

    # Remove extra whitespaces
    text = re.sub("\s\s+" , " ", text)

    # Strip leading and trailing whitespaces
    text = text.strip()

    # Tokenize the text
    tokens = word_tokenize(text)

    # Remove stopwords
    text = ' '.join([word for word in tokens if word not in stpwrd])

    # Lemmatize the text
    text = lemmatizer.lemmatize(text)

    return text

def dataCleaning():
    # Read csv file
    import pandas as pd

    df = pd.read_csv('/opt/airflow/data/product_data_clean.csv')

    # Drop missing values
    df = df.dropna()

    # Convert column names to lowercase
    df.columns = df.columns.str.lower()

    # Replace column names space with underscore (_) 
    df.columns = df.columns.str.replace(' ', '_')

    # Convert all object columns to lowercase
    df.columns = [x.lower() for x in df.columns]

    # Drop duplicates
    df = df.drop_duplicates()

    #  Calling text processing
    df['preprocessing_details_category'] = df['details_category'].apply(lambda x: textPreprocessing(x))

    # Save data to csv
    df.to_csv('/opt/airflow/data/product_data_clean.csv', index=False)
