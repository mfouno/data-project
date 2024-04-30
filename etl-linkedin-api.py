# # Extract

#Load env. secret
import os 
from dotenv import load_dotenv
load_dotenv(os.path.join("secrets.local", ".secrets"))

ACCESS_TOKEN: str = os.getenv(key="ACCESS_TOKEN_LINKEDIN_API")
PWD_MYSQL: str = os.getenv(key="PWD_MYSQL")

import json
import requests
import pandas as pd

# List of useful domains for analysis
domains_useful = ['ALL_LIKES', 'INBOX', 'INVITATIONS']

# Headers for request
headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202312'
}

# URL for request
url = "https://api.linkedin.com/rest/memberSnapshotData?q=criteria"

# Set df dictionnary
data_frames = {}

# API request
for domain in domains_useful:

    # Parameters
    params = {
        'domain': domain,
        'start': "0"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data_dict = response.json()
        df_name = 'df_' + domain.lower()
        data_frames[df_name] = pd.DataFrame([item for element in data_dict['elements'] for item in element['snapshotData']])
    else:
        print(f"Query error : {response.status_code} {response.text}")


df_invitations = data_frames['df_invitations']
df_inbox = data_frames['df_inbox']
df_all_likes = data_frames['df_all_likes']

# # Transform

# ##### a. Inbox data

# Rename columns
rename_dict_inbox = {
    "DATE": "date_utc",
    "TO": "recipient_name",
    "FROM": "sender_name",
    "CONTENT": "message_content",
    "SUBJECT": "email_subject",
    "CONVERSATION ID": "conversation_id",
    "RECIPIENT PROFILE URLS": "recipient_profile_url",
    "SENDER PROFILE URL": "sender_profile_url",
    "CONVERSATION TITLE": "conversation_title",
    "FOLDER": "folder"
}

df_inbox.rename(columns=rename_dict_inbox, inplace=True)

# Convert 'date_utc' column to datetime
df_inbox['date_utc'] = pd.to_datetime(df_inbox['date_utc'], utc=True)

# Convert from UTC to Paris time zone
df_inbox['datetime_paris'] = df_inbox['date_utc'].dt.tz_convert('Europe/Paris')

# Extract date and hour
df_inbox['date_paris'] = df_inbox['datetime_paris'].dt.date
df_inbox['hour_paris'] = df_inbox['datetime_paris'].dt.hour

# Drop 'date_utc' column
df_inbox.drop(columns=['date_utc'], inplace=True)

# ##### b. Invitations data

# Rename columns
rename_dict_invitations = {
    "Message": "message",
    "inviterProfileUrl": "inviter_profile_url",
    "Sent At": "sent_at",
    "To": "invitee",
    "From": "inviter",
    "Direction": "direction",
    "inviteeProfileUrl": "invitee_profile_url"
}

df_invitations.rename(columns=rename_dict_invitations, inplace=True)

# Convert 'date' column to datetime, assuming it is in UTC
df_invitations['sent_at'] = pd.to_datetime(df_invitations['sent_at'], format='%m/%d/%y, %I:%M %p', errors='coerce').dt.tz_localize('UTC')

# Convert from UTC to Paris time zone
df_invitations['sent_at'] = df_invitations['sent_at'].dt.tz_convert('Europe/Paris')

# Extract hour
df_invitations['sent_at_hour'] = df_invitations['sent_at'].dt.hour

# ##### c. Likes data

# Rename columns
rename_dict_likes = {
    "Link": "link",
    "Date": "date",
    "Type": "type"
}

df_all_likes.rename(columns=rename_dict_likes, inplace=True)

# Convert 'date' column to datetime, assuming it is in UTC
df_all_likes['date'] = pd.to_datetime(df_all_likes['date']).dt.tz_localize('UTC')

# Convert from UTC to Paris time zone
df_all_likes['date'] = df_all_likes['date'].dt.tz_convert('Europe/Paris')

# Extract hour
df_all_likes['date_hour'] = df_all_likes['date'].dt.hour

# # Load

#Load env. secret
import os 
from dotenv import load_dotenv
load_dotenv(os.path.join("secrets.local", ".secrets"))

PWD_MYSQL: str = os.getenv(key="PWD_MYSQL")

# 1) Connection test to MySQL server

from mysql import connector

try:
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL
    ) as database: 
        print(f"Database object: {database}")
except connector.Error as e: 
    print(e)

# 2) Creating a new database

try: 
    # Connect to server
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL
    ) as database: 
        
        # Create a database
        delete_db = "DROP DATABASE linkedin_data"
        create_db = "CREATE DATABASE linkedin_data"
        with database.cursor() as cursor: 
            cursor.execute(delete_db) 
            cursor.execute(create_db)

            # Display existing databases
            show_existing_db = "SHOW DATABASES"
            cursor.execute(show_existing_db)
            for db in cursor:
                print(db)
# Catch errors
except connector.Error as e:
    print(e)

# 3) Creating tables

# ALL LIKES
create_all_likes_table = """
CREATE TABLE all_likes(
    link VARCHAR(255) PRIMARY KEY,
    date DATE,
    type VARCHAR(255),
    date_hour INT
)
"""

try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor: 
            cursor.execute(create_all_likes_table)
            existing_database.commit()
        
            # Display the table schema 
            describe_likes = "DESCRIBE all_likes"
            cursor.execute(describe_likes)
            likes_schema = cursor.fetchall() 
            for column in likes_schema: 
                print(column)

except connector.Error as e: 
    print(e)

# INBOX
create_inbox_table = """
CREATE TABLE inbox(
    conversation_id VARCHAR(255),
    recipient_profile_url VARCHAR(255),
    sender_profile_url VARCHAR(255),
    email_subject VARCHAR(255),
    recipient_name VARCHAR(255),
    message_content TEXT,
    sender_name VARCHAR(255),
    conversation_title VARCHAR(255),
    folder VARCHAR(255),
    datetime_paris DATETIME,
    date_paris DATE,
    hour_paris INT
)
"""

try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor: 
            cursor.execute(create_inbox_table)
            existing_database.commit()
        
            # Display the table schema 
            describe_inbox = "DESCRIBE inbox"
            cursor.execute(describe_inbox)
            inbox_schema = cursor.fetchall() 
            for column in inbox_schema: 
                print(column)

except connector.Error as e: 
    print(e)

# INVITATIONS
create_invitations_table = """
CREATE TABLE invitations(
    message TEXT,
    inviter_profile_url VARCHAR(255),
    sent_at DATETIME,
    invitee VARCHAR(255),
    inviter VARCHAR(255),
    direction VARCHAR(255),
    invitee_profile_url VARCHAR(255),
    sent_at_hour INT
)
"""

try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor: 
            cursor.execute(create_invitations_table)
            existing_database.commit()
        
            # Display the table schema 
            describe_inv = "DESCRIBE invitations"
            cursor.execute(describe_inv)
            inv_schema = cursor.fetchall() 
            for column in inv_schema: 
                print(column)

except connector.Error as e: 
    print(e)

# 4) Insert data

# Construct insert statement for the ALL_LIKES data
insert_statement = f"INSERT INTO all_likes (link, date, type, date_hour) VALUES (%s, %s, %s, %s)"

# Insert data row by row
try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor:
            for _, row in df_all_likes.iterrows():
                cursor.execute(insert_statement, tuple(row))
            existing_database.commit()
        
except connector.Error as e: 
    print(e)

# Construct insert statement for the INBOX data
insert_statement = f"INSERT INTO inbox (conversation_id, recipient_profile_url, sender_profile_url, email_subject, recipient_name, message_content, sender_name, conversation_title, folder, datetime_paris, date_paris, hour_paris) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

# Insert data row by row
try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor:
            for _, row in df_inbox.iterrows():
                cursor.execute(insert_statement, tuple(row))
            existing_database.commit()
        
except connector.Error as e: 
    print(e)

# Construct insert statement for the INVITATIONS data
insert_statement = f"INSERT INTO invitations (message, inviter_profile_url, sent_at, invitee, inviter, direction, invitee_profile_url, sent_at_hour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

# Insert data row by row
try: 
    # Connect to existing database
    with connector.connect(
        host = "localhost",
        user = "root",
        password = PWD_MYSQL,
        database = "linkedin_data"
    ) as existing_database:
        
        # Create cursor object
        with existing_database.cursor() as cursor:
            for _, row in df_invitations.iterrows():
                cursor.execute(insert_statement, tuple(row))
            existing_database.commit()
        
except connector.Error as e: 
    print(e)