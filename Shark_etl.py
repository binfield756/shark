#!/usr/bin/env python
# coding: utf-8

# In[2]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[3]:


conn = pyodbc.connect('DSN=kubricksql;UID=de14;PWD=password')
cur = conn.cursor()


# In[4]:


sharkfile = r'c:\data\november\GSAF5 (1).csv'


# In[5]:


attack_dates = []
case_number = []
country = []
activity = []
age = []
gender = []
isfatal = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[6]:


data = zip(attack_dates, case_number, country, activity, age, gender, isfatal)


# In[7]:


cur.execute('truncate table jason.shark')


# In[23]:


q = 'INSERT INTO jason.shark (attack_date, case_number, country, activity, age, gender, isfatal) VALUES (?, ?, ?, ?, ?, ?, ?)'


# In[24]:


for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()

