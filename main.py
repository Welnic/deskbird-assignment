import pandas as pd
import psycopg2
import docker


client = docker.from_env()

def create_table(name, schema, conn):
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE IF NOT EXISTS {name} ({schema});')
    conn.commit()

def filter_out_erroneous_bookings(file_name):
    df = pd.read_csv(f'./data/{file_name}.csv')

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df.loc[df['created_at'].isnull(), 'created_at'] = pd.NaT

    df['booking_start_time'] = pd.to_datetime(df['booking_start_time'], errors='coerce')
    df.loc[df['booking_start_time'].isnull(), 'booking_start_time'] = pd.NaT

    df['booking_end_time'] = pd.to_datetime(df['booking_end_time'], errors='coerce')
    df.loc[df['booking_end_time'].isnull(), 'booking_end_time'] = pd.NaT

    df.to_csv(f'./output/{file_name}.csv', index=False)

def insert_data(path, table_name, columns, conn):
    cur = conn.cursor()

    cur.execute(f'COPY {table_name} ({columns}) FROM \'{path}.csv\' DELIMITER \',\' CSV HEADER;')
    conn.commit()

if __name__ == '__main__':
    conn = psycopg2.connect(host='localhost', database='assignment_db', user='root', password='assignment')

    companies_schema = 'company_id int PRIMARY KEY, status varchar, created_at timestamp, company_name varchar'
    users_schema = 'id serial PRIMARY KEY, rn int, created_at timestamp, company_id int REFERENCES companies(company_id), status varchar, demo_user varchar'
    bookings_schema = 'user_id int REFERENCES users(id), booking_id int, created_at timestamp NULL, status varchar, checkin_status varchar, booking_start_time timestamp NULL, booking_end_time timestamp NULL, is_demo varchar, PRIMARY KEY(user_id, booking_id)'
    
    create_table('companies', companies_schema, conn)
    create_table('users', users_schema, conn)
    create_table('bookings', bookings_schema, conn)

    filter_out_erroneous_bookings('booking')

    insert_data('/data/company', 'companies', 'company_id, status, created_at, company_name', conn)
    insert_data('/data/users', 'users', 'rn, created_at, company_id, status, demo_user', conn)
    insert_data('/output/booking', 'bookings', 'user_id, booking_id, created_at, status, checkin_status, booking_start_time, booking_end_time, is_demo', conn)

    print('Done')
    exit(0)
