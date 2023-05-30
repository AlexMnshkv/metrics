#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from scipy.stats import norm
import matplotlib.pyplot as plt

from tqdm.auto import tqdm

plt.style.use('ggplot')
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import plotly.express as px
import re
from io import BytesIO
import requests
import json
from urllib.parse import urlencode
import gspread
import pingouin as pg
from pingouin import multivariate_normality
import math as math
import scipy as scipy
import scipy.stats as stats
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials 
df=pd.read_csv('/mnt/HC_Volume_18315164/home-jupyter/jupyter-a-/less_15/KC_case_data .csv')


# In[2]:


df


# In[3]:


# посчитаем MAU февраля
qq=df.query('date>="2020-02-01" & date<="2020-02-29"')


# In[4]:


qq


# In[5]:


# MAU февраля
qq.device_id.nunique()


# In[6]:


df.query('event=="app_install"')


# In[7]:


# Найдем количество установок в январе
qq1=df.query('date>="2020-01-01" & date<="2020-01-31"')
qq1


# In[8]:


qq1.query('event=="app_install"').nunique()


# In[9]:


# количество установок в январе
qq1.query('event=="app_install"').count()


# In[10]:


df.query('event=="app_install"').groupby(['utm_source']).agg({'device_id':'count'}).sort_values("device_id")


# In[11]:


count_ins=df.query('event=="app_install"').groupby('date').agg({'device_id':'nunique'})
count_ins


# In[12]:


df.query('event=="purchase"').groupby('date').agg({'device_id':'count'})


# In[13]:


qq3=df.query('event=="purchase"')
qq3


# In[14]:


df["date"] = pd.to_datetime(df['date'])
df


# In[15]:


qq3.device_id.nunique()


# In[16]:


# таблица по установке приложения
df_install = df.query('event=="app_install"')
df_install


# In[17]:


df_install.groupby(['date','device_id']).agg({'device_id':'count'})


# In[18]:


# таблица по первой покупке
df_purch = df.query('event=="purchase"')
df_purch


# In[19]:


qqq=df_purch.merge(df_install, on ='device_id', how = 'left')
qqq


# In[20]:


qqq['date_in']=qqq['date_x'] - qqq['date_y']
qqq


# In[21]:


# Найдем конверсию из установки в покупку в течение 7 дней

qq1=qqq.query('date_in <= "7 days"')
qq1


# In[22]:


bb=qq1.groupby(['date_x']).agg({'device_id':'nunique'})
bb


# In[23]:


pppp=df_install.groupby('date').agg({'device_id':'count'})
pppp


# In[24]:


qqqq2=qq1.groupby('date_y').agg({'device_id':'nunique'})
qqqq2


# In[25]:


# посчитаем конверсию из установки в покупку в течение 7 дней
(qqqq2/pppp*100).sort_values('device_id')


# In[26]:


# С какого платного маркетингового канала пришло больше всего новых пользователей? 
df_install.groupby('utm_source').agg({'device_id':'nunique'}).sort_values('device_id')


# In[27]:


df.groupby('event').agg({'device_id':'nunique'})


# In[28]:


ff=df.merge(df_install, on='device_id', how='left')
ff


# In[29]:


ff1=ff.query('date_x >=date_y')


# In[30]:


# Зарегистрированные покупатели
ff1


# In[31]:


# на каком этапе воронки отваливается бОльшая часть клиентов
ff1.groupby('event_x').agg({'device_id':'nunique'}).sort_values('device_id')


# In[32]:


# Пользователи, пришедшие с каких каналов, показали самую низкую конверсию в первую покупку?


# In[33]:


ff


# In[34]:


cc1 = ff.groupby('utm_source_x').agg({'device_id': 'nunique'})
cc1


# In[35]:


cc2 = ff.groupby('utm_source_y').agg({'device_id': 'nunique'})
cc2


# In[36]:


cc1/cc2*100


# In[37]:


# Пользователи, пришедшие с какого канала, имеют медианный первый чек выше?
df


# In[38]:


gf=df.query('event == "purchase"')
gf


# In[39]:


ss=gf.drop_duplicates (subset=['device_id'])
ss


# In[40]:


# Пользователи, пришедшие с какого канала, имеют медианный первый чек выше
ss.groupby('utm_source').agg({'purchase_sum': 'median'}).sort_values('purchase_sum')


# In[42]:


gf.groupby('utm_source').agg({'purchase_sum':'sum'})

