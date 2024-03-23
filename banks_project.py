import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
csv_path = './exchange_rate.csv'
table_name = 'Largest_banks'
db_name = 'Banks.db'
log_file = 'code_log.txt'
sql_connection = sqlite3.connect(db_name)

def log_progress(message):
    ''' Hàm tạo log biểu thị thời gian lưu và xử lý dữ liê'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (log_file , 'a') as f:
        f.write(timestamp + ':'+ message + '\n')

def extract(url, table_attribs):
    ''' hàm trích thông tin từ website đích và luu trữ chúng duới dataframe '''
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find('tbody')
    rows = table.find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None and '-' not in col[2]:
                second_name = col[1].find_all('a')
                name = second_name[1].contents[0]
                mc_usd_billion = float(col[2].contents[0].replace('\n', ""))
                data_dict = {'Name': name,
                             'MC_USD_Billion': mc_usd_billion}
                df1 = pd.DataFrame(data_dict, index= [0])
                df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, csv_path):
    '''Hàm đọc và trích xuất giá trị tiền tệ sang USD'''
    data = pd.read_csv(csv_path)
    dict = data.set_index('Currency')['Rate'].to_dict()
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * dict['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * dict['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * dict['INR'], 2)
    return df


def load_to_csv(df, output_path):
    ''' Lưu datafame duới dạng file csv'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    '''Lưu dataframe vào database'''
    df.to_sql(table_name, sql_connection , if_exists= 'replace', index= False)


def run_query(query_statement, sql_connection):
    '''hàm chạy các lệnh truy vấn và thực hiện các truy vâ bằng terminal'''
    query_ouput = pd.read_sql(query_statement, sql_connection)
    print(query_ouput)
#thực hiện các bước
log_progress('Initiating ETL process')
#thiết ập dataframe
dataframe = extract(url, table_attribs)
#Tạo track log
log_progress('TranformedETL process')
#biến đổi data
df = transform(dataframe, csv_path)
log_progress('Loaded in to csv file')
load_to_csv(df, output_path)
#Thực hiện truy vấn đơn giản
log_progress('Run_query')
load_to_db(df = df, sql_connection = sql_connection, table_name = table_name)
query_1 = f'SELECT * FROM {table_name}'
query_2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query_3 = f'SELECT Name from {table_name} LIMIT 5'
run_query(query_statement= query_3, sql_connection = sql_connection)
sql_connection.close()
