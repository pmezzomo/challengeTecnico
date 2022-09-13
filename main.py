import json
import sqlite3
import pandas as pd
import ssl
import smtplib

from email.message import EmailMessage

#Creating database and connection - SQLite
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('data_classification.db')
        print(sqlite3.version)
    except Exception as e:
        print(e)

    return conn

#Creating table user_manager
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute('DROP TABLE IF EXISTS User_Manager')
        c.execute(create_table_sql)
    except Exception as e:
        print(e)

def insert_data(conn, data):
    sql = ''' INSERT INTO User_Manager('dn_name','owner_email','manager_email','confidentiality','integrity','availability')
              VALUES(?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()

    cur.execute("SELECT * FROM User_Manager")
    print(cur.fetchall())

#Selects the Json and Excel information that needs to be added to the database
def get_infos(dicio, manager_df):
    db_names = {}
    for dn in dicio['db_list']:

        if dn['owner']['uid'] in manager_df['user_id'].to_list():
            manager_email = manager_df.loc[manager_df['user_id'] == dn['owner']['uid']]['user_manager'].to_list()[0]
        else:
            manager_email = None

        classification = []
        for k in dn['classification'].values():
            classification.append(k)

        db_names[dn['dn_name']] = [dn['dn_name'], dn['owner']['email'], manager_email, *classification]

    return db_names

def send_email(dn, manager_email):
    password = 'password'
    de = 'email'
    para = manager_email
    subject = 'Solicitud de Aprobación'
    message = f'Hola!\n Considerando que para "{dn}" hay una clasificacion high, solicito su aprobación.'

    email = EmailMessage()
    email['To'] = para
    email['From'] = de
    email['Subject'] = subject
    email.set_content(message)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com: 465') as smtp:
        smtp.login(de, password)
        smtp.sendmail(de, para, email.as_string())

def main():
    #Read json
    with open('dblist.json') as f:
        dados = json.load(f)

    #Read excel
    user_manager = pd.read_excel('user_manager.xlsx', header=None)
    user_manager.columns = ['row_id', 'user_id', 'user_state', 'user_manager']

    final_data = get_infos(dados, user_manager)

    conn = create_connection()
    cur = conn.cursor()
    sql_create_user_manager_table = """ CREATE TABLE IF NOT EXISTS User_Manager (
                                            dn_name text NOT NULL,
                                            owner_email text NULL,
                                            manager_email text NULL,
                                            confidentiality text NULL,
                                            integrity text NULL,
                                            availability text NULL,
                                            PRIMARY KEY(dn_name)
                                        ); """
    create_table(conn, sql_create_user_manager_table)

    # Insert dictionary on Table
    for row in final_data.values():
        insert_data(conn, row)

    #Check information on DB
    cur.execute('SELECT * from User_Manager')
    rows = cur.fetchall()

    #Read the table to send the email
    for row in rows:
        print(row)  # Exibe as informações do DB
        if 'high' in row and row[2] is not None:
            send_email(row[0], row[2])

if __name__ == '__main__':
    main()
