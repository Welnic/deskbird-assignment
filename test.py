import unittest
import psycopg2
import pandas as pd

class DataIngestionTestCase(unittest.TestCase):
    def setUp(self):
        # Connect to the test database
        self.conn = psycopg2.connect(host='localhost', database='assignment_db', user='root', password='assignment')
        self.cur = self.conn.cursor()

    def tearDown(self):
        # Clean up the test database
        self.cur.close()
        self.conn.close()

    def test_users_ingestion(self):
        csv_file = 'data/users.csv'
        df = pd.read_csv(csv_file)
        num_rows = len(df)
        self.cur.execute('SELECT COUNT(*) FROM users;')
        result = self.cur.fetchone()
        self.assertEqual(num_rows, result[0])

    def test_companies_ingestion(self):
        csv_file = 'data/company.csv'
        df = pd.read_csv(csv_file)
        num_rows = len(df)
        self.cur.execute('SELECT COUNT(*) FROM companies;')
        result = self.cur.fetchone()
        self.assertEqual(num_rows, result[0])

    def test_bookings_ingestion(self):
        csv_file = 'data/booking.csv'
        df = pd.read_csv(csv_file)
        num_rows = len(df)
        self.cur.execute('SELECT COUNT(*) FROM bookings;')
        result = self.cur.fetchone()
        self.assertEqual(num_rows, result[0])

    def test_bookings_row_by_row(self):
        csv_file = 'output/booking.csv'
        df = pd.read_csv(csv_file)
        df = df.sort_values(by=['user_id', 'booking_id'])
        for index, row in df.iterrows():
            self.cur.execute(f'SELECT * FROM bookings WHERE user_id = {row["user_id"]} AND booking_id = {row["booking_id"]} order by user_id, booking_id;')
            result = self.cur.fetchone()
            self.assertEqual(row['user_id'], result[0])
            self.assertEqual(row['booking_id'], result[1])
            if pd.isna(row['status']):
                self.assertEqual(None, result[3])
            else:
                self.assertEqual(row['status'], result[3])
            if pd.isna(row['checkin_status']):
                self.assertEqual(None, result[4])
            else:
                self.assertEqual(row['checkin_status'], result[4])
            if pd.isna(row['is_demo']):
                self.assertEqual(None, result[7])
            else:
                self.assertEqual(row['is_demo'], result[7])
           

if __name__ == '__main__':
    unittest.main()
