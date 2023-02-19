import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import logging
from io import BytesIO


st.title("Портал ЕГР")

engine = create_engine('postgresql://egrn:egrn@localhost:5432/egrn')

ngrn_command_sql = 'SELECT DISTINCT(ngrn) as ngrn FROM dds.ngrns'
ngrns = [ngrn[0] for ngrn in engine.execute(ngrn_command_sql).fetchall()]
ngrns_cur = ngrns.insert(0, 'Все')

org_name_command_sql = 'SELECT DISTINCT(org_name) as org_name FROM dds.org_names'
org_names = [org_name[0] for org_name in engine.execute(org_name_command_sql).fetchall()]
org_names_cur = org_names.insert(0, 'Все')

statuses_command_sql = 'SELECT DISTINCT(status) as status FROM dds.statuses'
statuses = [status[0] for status in engine.execute(statuses_command_sql).fetchall()]
statuses_cur = statuses.insert(0, 'Все')

org_type_command_sql = 'SELECT DISTINCT(org_type) as org_type FROM dds.org_types'
org_types = [org_type[0] for org_type in engine.execute(org_type_command_sql).fetchall()]
org_types_cur = org_types.insert(0, 'Все')

region_command_sql = 'SELECT DISTINCT(region) as org_type FROM dds.regions'
regions = [region[0] for region in engine.execute(region_command_sql).fetchall()]
regions_cur = regions.insert(0, 'Все')

with st.form(key='form'):
    ngrn = st.selectbox(label='УНП', options= ngrns)
    org_name = st.selectbox(label='Название организации', options= org_names)
    start_date = st.date_input(label='Стартовая дата', value=datetime.now())
    end_date = st.date_input(label='Конечная дата', value=datetime.now())
    status = st.selectbox(
        label= 'Статус',
        options= statuses)
    org_type = st.selectbox(
        label= 'Организационный тип',
        options= org_types)
    region = st.selectbox(
        label= 'Регион',
        options= regions)
    st.form_submit_button('Поиск')


date = (start_date, end_date)

# Получается список типа {'ngrh': 111111, 'org_name': 'Топ компания', ...}
active_params = {
    key:value for key, value in {
        'ngrn': ngrn, 'org_name': org_name, 'date': date,
        'status': status, 'org_type':org_type, 'region': region}.items() if value != 'Все'
}

# Делаем результирующий запрос в базу
# [:-4] для того, чтобы последний and был удалён
res_sql_command = 'SELECT * FROM dm.organization WHERE' \
    + ' '.join(
        [f" {name} = '{value}' and" if name != 'date' else f" date >= '{value[0]}'::timestamptz and date < '{value[1]}'::timestamptz and" for name, value in active_params.items()]
        )[:-4]

df = pd.read_sql_query(res_sql_command, con= engine).astype({'ngrn': str, 'okved_code': str})
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    writer.save()
    processed_data = output.getvalue()
    return processed_data
df = df.astype({'date': 'datetime64[ns]'})
df_xlsx = to_excel(df)
st.download_button(label='📥 Загрузить выбранный набор данных',
                                data=df_xlsx ,
                                file_name= 'Набор.xlsx')
st.write(df)
engine.dispose()