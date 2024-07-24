import psycopg2 as pg
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import time

# Authenticate with Google Sheets
creds = Credentials.from_service_account_file('files/credentials.json')
service = build('sheets', 'v4', credentials=creds)

spreadsheet_id = "13rgsyFiA7URNNE2XNCFvIqkMdQzoBJXURiQBtVsoWqY"

global conn, cursor

conn = pg.connect(
    host="dpg-cqb3njo8fa8c73b0bn8g-a.frankfurt-postgres.render.com",
    database="botdb_hc6i",
    user="botdb_hc6i_user",
    password="eP6HxjZuBxuevilm7qNVPhsK75cAWsbD",
    port="5432"
)

cursor = conn.cursor()


def get_row_count(table_name):
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        return cursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None

def all_users(sheet_name, table_name):

    row_count = get_row_count(table_name)
    start_row = row_count + 2   
    range_name = f'{sheet_name}!B{start_row}:D'
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    if not values:
        print(f'No new data found in {sheet_name}.')
        return None
    else:
        try:
            for row in values:
                if len(row) >= 2:
                    cursor.execute(f"SELECT * FROM {table_name} WHERE phone_number = '{row[0]}' AND book_id = {row[2]}")
                    result = cursor.fetchone()
                    if result == None:
                        print(f"Adding {row[1]} ({row[0]}) with book id {row[2]} to the database.")
                        cursor.execute(f"INSERT INTO {table_name} (name, phone_number, book_id) VALUES (%s, %s, %s)", (row[1], row[0], row[2]))
                        conn.commit()
                    else:
                        print(f'{row[1]} already exists in the database with this book id {row[2]}.')
                    time.sleep(1)
        except Exception as e:
            print(e)
            return None
        
            

def checked_users(sheet_name, table_name):
    
        row_count = get_row_count(table_name)
        start_row = row_count + 2   
        range_name = f'{sheet_name}!B{start_row}:c'
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])
        try:
            if not values:
                print(f'No new data found in {sheet_name}.')
                return None
            else:
                for row in values:
                    print(f"{row[1]}'s number is {row[0]}")
                    if len(row) >= 2:
                        cursor.execute(f"INSERT INTO {table_name} (name, phone_num) VALUES (%s, %s)", (row[1], row[0]))
                        conn.commit()
                        time.sleep(1)
        except Exception as e:
            print(e)
            return None

def google_sheets_imports():
    all_users('All', 'users')
    checked_users('Chack', 'checked_users')

