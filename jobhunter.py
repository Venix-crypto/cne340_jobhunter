import mysql.connector
import requests
import json
import time
from html2text import html2text

def connect_to_sql() -> object:
    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1 ', database='cne340')
    return conn

def create_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INT PRIMARY KEY AUTO_INCREMENT,
        job_id TEXT, 
        company_name TEXT,
        created_at DATE,
        url TEXT,
        title LONGBLOB,
        description LONGBLOB
    )''')

def query_sql(cursor, query, values=None):
    cursor.execute(query, values)
    return cursor

def add_new_job(cursor, job):
    description = html2text(job['description'])
    date = job['date']
    query = 'INSERT INTO jobs(job_id, company_name, title, description, created_at, url) ' \
            'VALUES(%s,%s,%s,%s,%s,%s)'
    values = (job['id'], job['company'], job['title'], description, date, job['url'])
    query_sql(cursor, query, values)

def check_if_job_exists(cursor, job):
    job_id = job['id']
    query = "SELECT * FROM jobs WHERE job_id = %s"
    cursor.execute(query, (job_id,))
    return cursor.fetchone() is not None

def delete_job(cursor, job):
    job_id = job['id']
    query = "DELETE FROM jobs WHERE job_id = %s"
    cursor.execute(query, (job_id,))
    if cursor.rowcount > 0:
        print(f"Job with id {job_id} deleted successfully.")
    else:
        print(f"Job with id {job_id} does not exist in the database.")

def fetch_new_jobs():
    api_key = 'your_api_key'
    url = f'https://www.indeed.com/jobs?q=python&l=New+York&sort=date&limit=50&format=json&fromage=1&publisher={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

def add_or_delete_job(job, cursor, conn):
    job_id = job['id']
    if check_if_job_exists(cursor, job):
        print(f"Job {job_id} already exists in the database.")
    else:
        add_new_job(cursor, job)
        conn.commit()
        print(f"Job {job_id} added to database.")

def jobs(cursor, conn):
    jobpage = fetch_new_jobs()
    if jobpage:
        for job in jobpage['results']:
            job = {
                'id': job['id'],
                'title': job['jobtitle'],
                'company': job['company'],
                'description': job['snippet'],
                'date': job['date'],
                'url': job['url']
            }
            add_or_delete_job(job, cursor, conn)

def main():
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_table(cursor)
    jobs(cursor, conn)
    conn.close()
    while True:
        conn = connect_to_sql()
        cursor = conn.cursor()
        jobs(cursor, conn)
        conn.close()
        time.sleep(10)

if __name__ == '__main__':
    main()