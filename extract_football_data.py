import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
from datetime import datetime
from util import get_database_conn

header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

# Page Extraction layer
def extract_data():
    #Make a GET request to the page
    url = 'https://www.football-data.co.uk/englandm.php'
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        exit()
    # Parse the HTML content using BeautifulSoup
    soup = bs(response.content, 'lxml')
    # Find all the links to CSV files on the page
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.csv'):
            links.append(href)
            #print(links)
    # Loop through each link, extract the CSV content, and add it to a list
    data_frames = []
    for link in links:
        response = requests.get(f'https://www.football-data.co.uk/{link}')
        # Determine the encoding of the content
        encoding = chardet.detect(response.content)['encoding']
        # Decode the content using the detected encoding
        content = response.content.decode(encoding)
        #Split the content into lines
        lines = content.split('\n')
        # Convert the lines to a pandas DataFrame
        df = pd.DataFrame([line.split(',') for line in lines])
        data_frames.append(df)
        # # print(data_frames)

    # Merge all the data frames into a single data frame
    merged_df = pd.concat(data_frames)
    # Print the first few rows of the merged data frame
    #print(merged_df.head())
   # Read the CSV file, specifying that the first row contains the column names
    merged_df.to_csv('extracted_raw_data/extracted_football_data.csv', index=False, header=0)
    # Rename columns to remove numerical prefix
    merged_df = merged_df.rename(columns=lambda x: str(x).strip('0123456789'))
    print('Data successfully extracted to csv file')

# data Transform layer
def transform_data():
    # Read the extracted csv into a DataFrame
    df = pd.read_csv('extracted_raw_data/extracted_football_data.csv')
    #print(df.head())
    #selected required data columns
    df = df[['Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']]
    # Write the transformed data to csv file
    df.to_csv('transformed_data/transformed_football_data.csv', index=False)
    print('Data successfully trandformed and written to csv file')
    
# Data load transfor layer
def load_data():
    # Read the transformed csv into a DataFrame
    df = pd.read_csv('transformed_data/transformed_football_data.csv')
    engine = get_database_conn()
    df.to_sql('football_data', con=engine, if_exists='append', index=False)
    print('Data successfully written to postgreSQL database')