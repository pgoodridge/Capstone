# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 13:49:46 2020

@author: peter_goodridge
"""

import pandas as pd
import os
import json
import re
from datetime import datetime, timedelta, date
import pandas as pd
import os
import json
import cx_Oracle
from data_prep_helpers import parse_location, read_query, location_type, new_jobs
# from title_preds import final_cms
from google.cloud import bigquery
from google.oauth2 import service_account

######title_preds produces predictions from Nehal's title matching model, taking raw listings as input.
#######date_prep_helpers are like the name sounds, some generic functions

def clean_nulls(df):
    
    char_columns = df.select_dtypes(include=['object']).columns
    
    for col in char_columns:
        df[col] = df[col].fillna('')
        
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    
    numeric_cols = df.select_dtypes(include=numerics).columns
    
    for col in numeric_cols:
        df[col] = df[col].fillna(0)
        
    return df

def gcp_client(key_path="bi-dev-262318-a995d87cbe13.json"):

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    
    
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    return client

def oracle_client():

    pw = 'i3mu5be7'
    
    db_con = cx_Oracle.connect("dbase", pw, "prd-bia-db-003.oxfordcorp.com:1521/BIPRD", encoding="UTF-8")
    
    cur = db_con.cursor()
    
    return cur, db_con


def existing_raw():
    
    one_week =  datetime.now() - timedelta(28) #this goes back a little further than the GCP side to ensure we don't insert dupes
    string_date = one_week.strftime('%Y-%m-%d')    
    
    query = """select job_id from indeed_raw_listings_skill where 
        trunc(scrape_time) >= TO_DATE('{}','yyyy-mm-dd')
        
    """.format(string_date)
    
    existing_jobs = read_query(db_con, query)
    existing_jobs['job_id'] = existing_jobs['job_id'].map(lambda x: x.strip())
    
    return existing_jobs


def existing_skill():
    
    one_week =  datetime.now() - timedelta(28) #this goes back a little further than the GCP side to ensure we don't insert dupes
    string_date = one_week.strftime('%Y-%m-%d')    
    
    query = """select job_id from indeed_job_skills_skill where 
        trunc(scrape_time) >= TO_DATE('{}','yyyy-mm-dd')
        
    """.format(string_date)
    
    existing_jobs = read_query(db_con, query)
    existing_jobs['job_id'] = existing_jobs['job_id'].map(lambda x: x.strip())
    
    return existing_jobs


def max_date():
    
    #max_dt = read_query(db_con, 'select max(scrape_time) as max_time from {}'.format(table))
    
    #raw_dt = max_dt.iloc[0,0]
    #instead we get the date as of 7 days ago
    #the scraping processes run asyncronously so we want to limit our
    #possible new job inserts but still do a comparison on ID
    #to ensure we pick up all new jobs
    one_week =  datetime.now() - timedelta(21)
    string_date = one_week.strftime('%Y-%m-%d %H:%M:%S UTC')
    
    return string_date


def find_new(max_date, existing_jobs):
    
    q1 = """
    select *
    from DW__test.indeed_raw_listings_skill
    where scrape_time >= timestamp('{}')
    ;
    """.format(max_date)
    listings_raw = client.query(q1).result().to_dataframe()
    listings_raw['job_id'] = listings_raw['job_id'].map(lambda x: x.strip())
    listings_raw = listings_raw.set_index('job_id')
    


    df_raw = new_jobs(existing_jobs, listings_raw).reset_index()
    len_df = len(df_raw)

    df_raw = df_raw.dropna()
    len_lsts = len(df_raw)
    
    print("{} Listings dropped off due to nulls".format(len_lsts-len_df))
    
    return df_raw

def find_new_skill(max_date, existing_jobs):
    
    q1 = """
    select *
    from DW__test.indeed_job_skills_skill
    where scrape_time > timestamp('{}')
    ;
    """.format(max_date)
    
    listings_raw = client.query(q1).result().to_dataframe()
    listings_raw['job_id'] = listings_raw['job_id'].map(lambda x: x.strip())
    listings_raw = listings_raw.set_index('job_id')



    df_raw = new_jobs(existing_jobs, listings_raw).reset_index()
    len_df = len(df_raw)

    df_raw = df_raw.dropna()
    len_lsts = len(df_raw)
    
    
    print("{} Listings dropped off due to nulls".format(len_lsts-len_df))
    
    return df_raw
    


def clean_jobs(df_raw):
    
    df_raw['location_type'] = df_raw['location'].map(location_type)
    
    df_raw = df_raw.apply(parse_location, axis=1)
    df_raw['city'] = df_raw['city'].map(lambda x: x[:50])
    
    df_raw = df_raw[[ 'skill_query', 'job_id', 'company', 'location', 'job_title',
                                                     'url', 'date', 'scrape_time', 'disc', 'disc_score', 'location_type',
                                                     'city', 'state', 'zip']]
    
    
    
    
    df_raw = clean_nulls(df_raw)
    
    return df_raw


def skills_df_clean(skills_raw):

    skills_raw['location_type'] = skills_raw['location'].map(location_type)
    
    skills_df_raw = skills_raw.apply(parse_location, axis=1)
    
    skills_df_raw['url'] = skills_df_raw['url'].map(lambda x: x[:100])
    skills_df_raw['city'] = skills_df_raw['city'].map(lambda x: x[:50])
    skills_df_raw['skill'] = skills_df_raw['skill'].fillna('')
    skills_df_raw['skill'] = skills_df_raw['skill'].map(lambda x: x[:300])
    
    
    
    skills_df_raw = skills_df_raw [['skill_query', 'job_id', 'company', 'location', 'job_title',
                                   'url', 'date', 'scrape_time', 'skill', 'disc', 'disc_score', 'location_type',
                                    'city', 'state', 'zip']]
    
    
    skills_df_raw = clean_nulls(skills_df_raw)
    
    return skills_df_raw
    

#raw_listings

    
client = gcp_client()
cur, db_con = oracle_client()


max_date_ = max_date()
existing_jobs = existing_raw()
new_jobs_raw = find_new(max_date_, existing_jobs)



#skills
existing_jobs_skill = existing_skill()
skills_raw = find_new_skill(max_date_, existing_jobs_skill)

#final 

df_raw = clean_jobs(new_jobs_raw.reset_index())
skills_df_raw = skills_df_clean(skills_raw.reset_index())

##########CM Match model###########
#maybe add to skills playbook down the road
#full_cms_final = final_cms(df_raw)




##############################
###Insert indeeed_raw_listings
##############################
#cur.execute('drop table indeed_raw_listings_dev')

create_statement = """ create table indeed_raw_listings_skill(
    skill_query char(32),
    job_id char(32),
    company varchar(200),
    location varchar(200),
    job_title varchar(200),
    url varchar(100),
    job_date char(32),
    scrape_time date,
    disc char(10),
    disc_score number,
    location_type char(15),
    city varchar(50),
    state char(50),
    zip char(15)
    
)
"""
#cur.execute(create_statement)


all_rows = []

for row in df_raw.itertuples():
    all_rows.append(row[1:])

insert_statement = """insert into indeed_raw_listings_skill(skill_query, job_id, company, location, job_title,
        url, job_date, scrape_time, disc, disc_score, location_type, city, state, zip)
        values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
"""

cur.executemany(insert_statement, all_rows)
db_con.commit()


##############################
###Insert indeed_job_skills_dev
##############################
    
    
#cur.execute('drop table indeed_job_skills_dev')
create_statement = """ create table indeed_job_skills_dev(
    skill_query char(32),
    job_id char(32),
    company varchar(200),
    location varchar(200),
    job_title varchar(200),
    url varchar(100),
    job_date char(32),
    scrape_time date,
    skill varchar(300),
    disc char(10),
    disc_score number,
    location_type char(15),
    city varchar(50),
    state char(50),
    zip char(15)

)
"""
#cur.execute(create_statement)


all_rows = []

for row in skills_df_raw.itertuples():
    all_rows.append(row[1:])
    
    
batches = []
for i in range(100000, len(all_rows) + 100000, 100000):
    batch = all_rows[i-100000:i]
    if len(batch) > 0:
        batches.append(batch)

insert_statement = """insert into indeed_job_skills_skill(skill_query, job_id, company, location, job_title, url, job_date, 
        scrape_time, skill, disc, disc_score, location_type, city, state, zip)
        values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)
"""
for batch in batches:
    cur.executemany(insert_statement, batch)
    db_con.commit()

print("job finished")