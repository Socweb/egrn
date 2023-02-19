#!/usr/bin/env python
# coding: utf-8

# In[56]:


from datetime import date, datetime, timedelta
import requests
import json
import pandas as pd
import logging 
from sqlalchemy import create_engine

logging.basicConfig(
    filename="/home/flavius/main.log",
    format='%(asctime)s - %(levelname)s -%(name)s - %(message)s'
                   ) 


# In[57]:


START_DATE = (date.today() - timedelta(1)).strftime('%d.%m.%Y')
END_DATE = date.today().strftime('%d.%m.%Y')


# In[58]:


engine = create_engine('postgresql://egrn:egrn@localhost:5432/egrn')


# In[59]:


def get_info(info_type: 'str', start_date: 'str' = START_DATE, end_date: 'str' = END_DATE) -> pd.DataFrame:
    base_url = 'http://egr.gov.by/api/v2/egr'
    if info_type == 'short_info':
        url = f'{base_url}/getShortInfoByPeriod/{start_date}/{end_date}'
        return requests.get(url).json()
    elif info_type == 'contact_info':
        url = f'{base_url}/getAddressByPeriod/{start_date}/{end_date}'
        return requests.get(url).json()
    elif info_type == 'okved_info':
        url = f'{base_url}/getVEDByPeriod/{start_date}/{end_date}'
        return requests.get(url).json()
    else:
        logging.warning('Функция get_info ничего не вернула!')
        
def download_to_sql(df: pd.DataFrame, schema: str, table: str) -> 0:
    df.to_sql(
        name= table, con= engine,schema= schema, chunksize=10000, if_exists= 'append', index= False
    )
    return 0


# In[98]:


organizations_short_info = pd.DataFrame(get_info('short_info', START_DATE, END_DATE))
address_info = pd.DataFrame(get_info('contact_info', START_DATE, END_DATE))
okved_info = pd.DataFrame(get_info('okved_info', START_DATE, END_DATE))


# In[99]:


organizations_short_info = organizations_short_info[
    [
        'ngrn', 'dfrom', 'vfio', 'vnaim', 'nsi00219'
    ]
]
address_info = address_info[
    [
        'ngrn', 'vregion', 'vdistrict', 'vnp', 'vulitsa',
        'vdom', 'vpom', 'nsi00202', 'vemail', 'vtels'
    ]
]
okved_info = okved_info[
    [
        'ngrn', 'nsi00114'
    ]
]


# In[100]:


address_info['location'] = address_info['nsi00202'].apply(lambda x: x['vnsfull'])
address_info = address_info.drop(columns=['nsi00202']).drop_duplicates(subset=['ngrn'])
address_info = address_info.fillna('Нет')


# In[101]:


organizations_short_info['org_name'] = organizations_short_info['vfio'].combine_first(organizations_short_info['vnaim'])
organizations_short_info['status'] = organizations_short_info['nsi00219'].apply(lambda x: x['vnsostk'])
organizations_short_info['org_type'] = organizations_short_info.loc[organizations_short_info['vfio'].isnull() != True, 'vfio'] = 'ИП'
organizations_short_info['org_type'].loc[(organizations_short_info['vfio'].isnull() == True)] = 'ЮЛ'
organizations_short_info = organizations_short_info.drop(columns=['vfio', 'vnaim', 'nsi00219']).drop_duplicates(subset=['ngrn'])
organizations_short_info = organizations_short_info.fillna('Нет')


# In[102]:


okved_info['okved_code'] = okved_info['nsi00114'].apply(lambda x: x['vkvdn'])
okved_info['okved_text'] = okved_info['nsi00114'].apply(lambda x: x['vnvdnp'])
okved_info = okved_info.drop(columns=['nsi00114']).drop_duplicates(subset=['ngrn'])
okved_info= okved_info.fillna('Нет')


# In[103]:


# Загрузка в базу данных
download_to_sql(organizations_short_info, 'stg', 'main_short_info')
download_to_sql(address_info, 'stg', 'main_address_info')
download_to_sql(okved_info, 'stg', 'main_okved_info')


# In[83]:


organizations_short_info.query('ngrn == 791256938')


# In[66]:


max([x for x in address_info['vemail']])


# In[ ]:




