#!/usr/bin/env python
# coding: utf-8

# In[56]:


from datetime import date, datetime, timedelta
import requests
import json
import pandas as pd
import logging 
from sqlalchemy import create_engine
from time import sleep

logging.basicConfig(
    filename="/home/flavius/main.log",
    format='%(asctime)s - %(levelname)s -%(name)s - %(message)s'
                   ) 


# In[57]:

i = 1
while i > 0:
    # НУЛЕВОЙ ШАГ. Загрузка сырых данных
    engine = create_engine('postgresql://egrn:egrn@localhost:5432/egrn')
    
    # Рабочий вариант
    START_DATE = engine.execute('SELECT max(max_dfrom) FROM stg.short_info_tech_table').fetchall()[0][0].strftime('%d.%m.%Y')
    END_DATE = date.today().strftime('%d.%m.%Y')
    # Тестовый вариант
    #START_DATE = datetime(2023,1,1).strftime('%d.%m.%Y')
    #END_DATE = (date.today() - timedelta(2)).strftime('%d.%m.%Y')
        
    logging.info(f'Стартовая дата{START_DATE}, конечная дата {END_DATE}')

    def get_info(info_type: 'str', start_date: 'str' = START_DATE, end_date: 'str' = END_DATE) -> pd.DataFrame:
        base_url = 'http://egr.gov.by/api/v2/egr'
        url = ''
        response = ''
        try:
            if info_type == 'short_info':
                url = f'{base_url}/getShortInfoByPeriod/{start_date}/{end_date}'
                response = requests.get(url)
                return response.json()
            elif info_type == 'contact_info':
                url = f'{base_url}/getAddressByPeriod/{start_date}/{end_date}'
                response = requests.get(url)
                return response.json()
            elif info_type == 'okved_info':
                url = f'{base_url}/getVEDByPeriod/{start_date}/{end_date}'
                response = requests.get(url)
                return response.json()
            else:
                logging.warning('Функция get_info ничего не вернула!')
        except:
            logging.error(f'Ошибка сетевого запроса к серверу. Код ошибки {response.status_code} \
            . Запрошенный адрес: {url}')
            # Вызывать функцию для парсинга?
            pass

    def download_to_sql(df: pd.DataFrame, schema: str, table: str) -> 0:
        df.to_sql(
            name= table, con= engine,schema= schema, chunksize=10000, if_exists= 'append', index= False
        )
        return 0



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
            'ngrn', 'dfrom', 'vregion', 'vdistrict', 'vnp', 'vulitsa',
            'vdom', 'vpom', 'nsi00202', 'vemail', 'vtels'
       ]
    ]
    okved_info = okved_info[
        [
            'ngrn', 'dfrom', 'nsi00114'
        ]
    ]


    # In[100]:


    location = [
        ','.join(x[1:]).split(',')[0][1:-1] for x in (str(x).split("'vnsfull': ") for x in address_info['nsi00202'])
    ]
    address_info['location'] = location
    address_info = address_info.drop(columns=['nsi00202']).drop_duplicates(subset=['ngrn'])
    address_info = address_info.fillna('Нет')

    organizations_short_info['org_name'] = organizations_short_info['vfio'].combine_first(organizations_short_info['vnaim'])
    status = [
        ','.join(x[1:]).split(',')[0][1:-1] for x in (str(x).split("'vnsostk': ") for x in organizations_short_info['nsi00219'])
    ]
    organizations_short_info['status'] = status
    organizations_short_info['org_type'] = organizations_short_info.loc[organizations_short_info['vfio'].isnull() != True, 'vfio'] = 'ИП'
    organizations_short_info['org_type'].loc[(organizations_short_info['vfio'].isnull() == True)] = 'ЮЛ'
    organizations_short_info = organizations_short_info.drop(columns=['vfio', 'vnaim', 'nsi00219']).drop_duplicates(subset=['ngrn'])
    organizations_short_info = organizations_short_info.fillna('Нет')

        # Создаёт список ОКВЭД в строковом формате
    okved_code = [
        ','.join(x[1:]).split(',')[0][1:-1] for x in (str(x).split("'vkvdn': ") for x in okved_info['nsi00114'])
    ]
    okved_info['okved_code'] = okved_code
    okved_text = [
        ','.join(x[1:]).split("',")[0][1:] for x in (str(x).split("'vnvdnp': ") for x in okved_info['nsi00114'])
    ]
    okved_info['okved_text'] = okved_text
    okved_info = okved_info.drop(columns=['nsi00114']).drop_duplicates(subset=['ngrn'])
    okved_info= okved_info.fillna('Нет')



    # Загрузка в базу данных
    download_to_sql(organizations_short_info, 'stg', 'main_short_info')
    download_to_sql(address_info, 'stg', 'main_address_info')
    download_to_sql(okved_info, 'stg', 'main_okved_info')

    download_to_sql(organizations_short_info, 'stg', 'main_short_info')
    download_to_sql(address_info, 'stg', 'main_address_info')
    download_to_sql(okved_info, 'stg', 'main_okved_info')
    
    # ПЕРВЫЙ ШАГ. Удаление дубликатов из слоя сырых данных и заполнение тех. таблиц
    engine.execute(
        '''
        create table stg.cur_main_short_info as
            select distinct msi.*
            from stg.main_short_info as msi;

        truncate table stg.main_short_info RESTART IDENTITY;
        insert into stg.main_short_info (ngrn, dfrom, org_name, status, org_type)
            select ngrn, dfrom, org_name, status, org_type
            from stg.cur_main_short_info;

        DROP TABLE IF EXISTS stg.cur_main_short_info;

        create table stg.cur_main_address_info as
            select distinct mai.*
            from stg.main_address_info as mai;

        truncate table stg.main_address_info RESTART IDENTITY;
        insert into stg.main_address_info (ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location)
            select ngrn, dfrom, vregion, vdistrict, vnp, vulitsa, vdom, vpom, vemail, vtels, location
            from stg.cur_main_address_info;

        DROP TABLE IF EXISTS stg.cur_main_address_info;

        create table stg.cur_main_okved_info as
            select distinct moi.*
            from stg.main_okved_info as moi;

        truncate table stg.main_okved_info RESTART IDENTITY;
        insert into stg.main_okved_info (ngrn, dfrom, okved_code, okved_text)
            select ngrn, dfrom, okved_code, okved_text
            from stg.cur_main_okved_info;

        DROP TABLE IF EXISTS stg.cur_main_okved_info;

        
        INSERT INTO stg.short_info_tech_table (max_dfrom)
        SELECT MAX(dfrom)
        FROM stg.main_short_info
        ON CONFLICT ON CONSTRAINT short_info_tech_table_max_dfrom_key
        DO NOTHING;

        INSERT INTO stg.address_info_tech_table (max_dfrom)
        SELECT MAX(dfrom)
        FROM stg.main_address_info
        ON CONFLICT ON CONSTRAINT address_info_tech_table_max_dfrom_key
        DO NOTHING;

        INSERT INTO stg.okved_info_tech_table (max_dfrom)
        SELECT MAX(dfrom)
        FROM stg.main_okved_info
        ON CONFLICT ON CONSTRAINT okved_info_tech_table_max_dfrom_key
        DO NOTHING;

        '''
    )
    
    # ВТОРОЙ ШАГ. Загрузка в слой детальных данных
    engine.execute(
        '''
        INSERT INTO dds.ngrns (ngrn)
        SELECT distinct ON (ngrn) ngrn
        FROM stg.main_short_info as msi
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT ngrns_ngrn_key
        DO NOTHING;

        INSERT INTO dds.org_names (org_name)
        SELECT distinct ON (org_name) org_name
        FROM stg.main_short_info as msi
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT org_names_org_name_key
        DO NOTHING;

        INSERT INTO dds.okveds (okved_code, okved_text)
        SELECT distinct ON (okved_code, okved_text) okved_code, okved_text
        FROM stg.main_okved_info as moi
        WHERE moi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.okved_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT okveds_okved_code_okved_text_key
        DO NOTHING;

        INSERT INTO dds.statuses (status)
        SELECT distinct ON (status) status
        FROM stg.main_short_info as msi
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT statuses_status_key
        DO NOTHING;

        INSERT INTO dds.org_types (org_type)
        SELECT distinct ON (org_type) org_type
        FROM stg.main_short_info as msi
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT org_types_org_type_key
        DO NOTHING;

        INSERT INTO dds.regions (region)
        SELECT distinct ON (vregion) vregion
        FROM stg.main_address_info as mai
        WHERE mai.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT regions_region_key
        DO NOTHING;

        INSERT INTO dds.districts (district)
        SELECT distinct ON (vdistrict) vdistrict
        FROM stg.main_address_info as mai
        WHERE mai.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT districts_district_key
        DO NOTHING;

        INSERT INTO dds.settlements (settlement)
        SELECT distinct ON (vnp) vnp
        FROM stg.main_address_info as mai
        WHERE mai.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT settlements_settlement_key
        DO NOTHING;


        INSERT INTO dds.contacts (location, email, phone)
        SELECT distinct ON (location, vemail, vtels) location, vemail, vtels
        FROM stg.main_address_info as mai
        WHERE mai.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT contacts_location_email_phone_key
        DO NOTHING;

        INSERT INTO dds.dates (date)
        SELECT distinct ON (dfrom) dfrom
        FROM stg.main_short_info as msi 
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT dates_date_key
        DO NOTHING;
        '''
    )
    
    # ТРЕТИЙ ШАГ. Загрузка данных в витрину
    engine.execute(
        '''
        INSERT INTO dm.organization(
            org_name, ngrn, org_type, date, status, okved_code, okved_text,
            region, district, settlement, location, email, phone
            )
        SELECT DISTINCT ON (org_name, ngrn, status, location)
        msi.org_name,
        msi.ngrn,
        msi.org_type,
        msi.dfrom,
        msi.status,
        COALESCE(moi.okved_code, 'Нет') as okved_code,
        COALESCE(moi.okved_text, 'Нет') as okved_text,
        mai.vregion,
        mai.vdistrict,
        mai.vnp,
        mai.location,
        mai.vemail,
        mai.vtels
        FROM stg.main_short_info as msi
        LEFT JOIN
        stg.main_okved_info as moi
        ON msi.ngrn = moi.ngrn
        LEFT JOIN
        stg.main_address_info as mai
        ON msi.ngrn = mai.ngrn
        WHERE msi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.short_info_tech_table
            ) as t WHERE dfrom_rank = 2) or 
            mai.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.address_info_tech_table
            ) as t WHERE dfrom_rank = 2) or
            moi.dfrom >= (SELECT max_dfrom FROM (
            SELECT max_dfrom, ROW_NUMBER () OVER (ORDER BY max_dfrom DESC) as dfrom_rank FROM stg.okved_info_tech_table
            ) as t WHERE dfrom_rank = 2)
        ON CONFLICT ON CONSTRAINT organization_org_name_ngrn_key
        DO UPDATE SET date = EXCLUDED.date, status = EXCLUDED.status, okved_code = EXCLUDED.okved_code, okved_text = EXCLUDED.okved_text;
    
    '''
        )
    engine.dispose()
    sleep(600)
    
