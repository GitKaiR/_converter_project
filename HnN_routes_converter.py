import pandas as pd
import os.path 
import csv
from interface import input_path, input_load_date
from datetime import datetime, timedelta    
from routes_export import export_new_routes_files
pd.options.mode.chained_assignment = None


def HnN_import_rtm_file(PATH):
    df_rtm = pd.read_excel(PATH, sheet_name='MergeDATA', usecols='A:AM')#, engine = 'pyxlsb')
    df_rtm = df_rtm.replace(r'\n','', regex=True)
    return df_rtm


def HnN_rename_columns(df_rtm):
    renames = {'Тип маршрута'                  :  'ROUTE_TYPE', 
               'ТТ SAP Код'                    :  'SHIP_TO', 
               'Наименование маршрута'         :  'ROUTE_NAME', 
               'Ответственный'                 :  'ROUTE_ID', 
               	'Длительность визита (минут)'  :  'VISIT_DURATION',
                'Агенство'                     :  'AGENCY_NAME',
               'Weekly Frequency'              :  'FREQUENCY_RTM',
               'РОЛЬ'                          :  'ROLE',
               'Форма заказа'                  :  'ORDER_TYPE',
               'СЦЕНАРИЙ'                      :  'SCENARIO'}
    df_rtm.rename(columns=renames, inplace=True)

    columns_list = ['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO',
                    'ПН1', 'ВТ1', 'СР1', 'ЧТ1', 'ПТ1', 'СБ1', 'ВС1', 
                    'ПН2', 'ВТ2', 'СР2', 'ЧТ2', 'ПТ2', 'СБ2', 'ВС2',
                    'ПН3', 'ВТ3', 'СР3', 'ЧТ3', 'ПТ3', 'СБ3', 'ВС3', 
                    'ПН4', 'ВТ4', 'СР4', 'ЧТ4', 'ПТ4', 'СБ4', 'ВС4' ]
    df_rtm = df_rtm[columns_list]    
    df_rtm['SHIP_TO'] = df_rtm['SHIP_TO'].astype('float').astype('int').astype('str')
    df_rtm['VISIT_DURATION'] = df_rtm['VISIT_DURATION'].astype('float').astype('int') 
    return df_rtm

def HnN_filtering_rtm(df_rtm):
    if len(df_rtm.query("ROUTE_TYPE not in ('TSS', 'ASM', 'SV')")) > 0:
        print("\nВНИМАНИЕ! Файл содержит неправльные Типы маршрута (не только TSS или ASM). Данные строки будут удалены.")
    if len(df_rtm.query("ORDER_TYPE not in ('FIX', 'FLEX')")) > 0:
        print("\nВНИМАНИЕ! Файл содержит неправльные Типы заказа (не только FLEX или FIX). Данные строки будут удалены.")
    if len(df_rtm.query("AGENCY_NAME not in ('HnN')")) > 0:
        print("\nВНИМАНИЕ! Файл содержит неправльное Наименование агентства (не только HnN). Данные строки будут удалены.")
    df_rtm = df_rtm.query("ROUTE_TYPE in ('TSS', 'ASM', 'SV') & ORDER_TYPE in ('FLEX', 'FIX') & AGENCY_NAME == 'HnN'")
    return df_rtm

def check_wrong_order_type(df_rtm):
    '''Функция проверяет на корректность поле Тип заказа в файле Excel'''
    df_check_wrong_order_type = df_rtm.groupby(by=['ROUTE_NAME', 'ORDER_TYPE']).agg('size').reset_index().drop(columns=0)
    df_check_wrong_order_type = df_check_wrong_order_type.groupby(by=['ROUTE_NAME']).agg('size').reset_index()
    df_check_wrong_order_type = df_check_wrong_order_type.loc[df_check_wrong_order_type[0] > 1]

    if len(df_check_wrong_order_type) > 0:
        print("\nВНИМАНИЕ! Эти маршруты не будут сконвертированы, так как сожержат не верный формат заказа: FIX и FLEX одновеменно")
    for i in df_check_wrong_order_type['ROUTE_NAME'].to_list():
        print(i)

    df_rtm = pd.merge(df_rtm, df_check_wrong_order_type['ROUTE_NAME'], how='outer', on='ROUTE_NAME', indicator=True)
    df_rtm = df_rtm.query("_merge != 'both'").drop(columns='_merge')
    return df_rtm


def check_wrong_route_type(df_rtm):
    '''Функция проверяет на корректность поле Тип маршрута в файле Excel'''
    df_check_wrong_route_type = df_rtm.groupby(by=['ROUTE_NAME', 'ROUTE_TYPE']).agg('size').reset_index().drop(columns=0)
    df_check_wrong_route_type = df_check_wrong_route_type.groupby(by=['ROUTE_NAME']).agg('size').reset_index()
    df_check_wrong_route_type = df_check_wrong_route_type.loc[df_check_wrong_route_type[0] > 1]

    if len(df_check_wrong_route_type) > 0:
        print("\nВНИМАНИЕ! Эти маршруты не будут сконвертированы, так как содержат задвоение типа маршрута: например, ASM и TSS одновеменно")
    for i in df_check_wrong_route_type['ROUTE_NAME'].to_list():
        print(i)

    df_rtm = pd.merge(df_rtm, df_check_wrong_route_type['ROUTE_NAME'], how='outer', on='ROUTE_NAME', indicator=True)
    df_rtm = df_rtm.query("_merge != 'both'").drop(columns='_merge')
    return df_rtm


def HnN_unpivot_rtm(df_rtm):
    df_rtm_tss = pd.melt(df_rtm, id_vars=['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO'])
    renames = {'variable'  :  'DAY_NAME', 
               'value'     :  'VISIT_NUMBER'} 
    df_rtm_tss.rename(columns=renames, inplace=True)
    filt = df_rtm_tss['VISIT_NUMBER'].notnull()
    df_rtm_tss = df_rtm_tss[filt]
    df_rtm_tss['WEEK_1234_ORDER'] = df_rtm_tss['DAY_NAME'].str[-1]
    df_rtm_tss['WEEK_1234_ORDER'] = df_rtm_tss['WEEK_1234_ORDER'].astype('int')
    df_rtm_tss['DAY_NAME']        = df_rtm_tss['DAY_NAME'].str[0:2]
    df_rtm_tss['DAY_NAME']        = df_rtm_tss['DAY_NAME'].astype('str')
    df_rtm_tss['EXT_ROUTE_ID']    = df_rtm_tss['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    df_rtm_tss['ROW_NUMBER']      = df_rtm_tss.groupby(by=['ROUTE_NAME','DAY_NAME','WEEK_1234_ORDER' ]).cumcount()+1
    df_rtm_tss.loc[df_rtm_tss['VISIT_NUMBER'] == 1, ['VISIT_NUMBER']] = df_rtm_tss['ROW_NUMBER']
    df_rtm_tss['SHIP_TO'] = df_rtm_tss['SHIP_TO'].astype('float').astype('int').astype('str')
    return df_rtm_tss


#Формируем календарь на год вперед
def HnN_create_calender(DATE_OF_LOAD):
    df_calender = pd.DataFrame(pd.date_range('2023-11-27', '2024-12-31'), columns=['DATE'] )
    df_calender['WEEKDAY_NUMBER'] = df_calender['DATE'].dt.weekday+1
    df_calender['DAY_NAME'] = df_calender['WEEKDAY_NUMBER'] 
    df_calender['WEEK_NUMBER'] = df_calender['DATE'].dt.strftime("%W").astype('int')+ 1  #еще можно так .dt.isocalendar().week, но тут не стандартные номера недель
    df_calender['WEEK_1234_ORDER'] = ((df_calender['WEEK_NUMBER']  / 4)  - (df_calender['WEEK_NUMBER']  // 4)) * 4
    df_calender.loc[df_calender['WEEK_1234_ORDER'] == 0 ,   'WEEK_1234_ORDER'] = 4
    df_calender['WEEK_1234_ORDER'] = df_calender['WEEK_1234_ORDER'].astype('int')
    df_calender.replace({'DAY_NAME' : { 1 : 'ПН', 2 : 'ВТ', 3 : 'СР', 4 : 'ЧТ', 5 : 'ПТ', 6 : 'СБ', 7 : 'ВС'}}, inplace=True)
    DATE_OF_END = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y') +  + timedelta(days=60)  #Пробрасываем маршруты на этот перид
    filt = (df_calender['DATE'] >= pd.to_datetime(DATE_OF_LOAD, dayfirst=True)) & (df_calender['DATE'] <= DATE_OF_END)
    df_calender = df_calender[filt]
    return df_calender


def HnN_converting_FIX_rtm_to_routes(df_rtm, df_calender): 
   #Оставляем только часть заказа с фиксированными маршрутами
    df_rtm_keys = pd.DataFrame(df_rtm.query("ORDER_TYPE == 'FIX'"))
   #Создаем матрицу: всем маршруто-точки и все дни
    df_rtm_keys = df_rtm_keys.groupby(by=['SHIP_TO', 'ROUTE_NAME']).agg('size').reset_index().drop(columns=0)
    df_rtm_keys = df_rtm_keys[['SHIP_TO', 'ROUTE_NAME']]
    df_routes_HnN_fix = pd.merge(df_rtm_keys, df_calender, how='cross' )
   #Добавляем к матрице заказ и удаляем лишние (пустые) строки
    df_routes_HnN_fix = pd.merge(df_routes_HnN_fix, df_rtm[['SHIP_TO', 'ROUTE_NAME', 'EXT_ROUTE_ID', 'DAY_NAME', 'WEEK_1234_ORDER', 'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE']], how='left',  on=['SHIP_TO', 'ROUTE_NAME', 'DAY_NAME', 'WEEK_1234_ORDER'])
    df_routes_HnN_fix.dropna(subset=['VISIT_NUMBER'], inplace=True)
   #Дозаполняем необходимые поля
    df_routes_HnN_fix.rename(columns={'DATE': 'FIRST_VISIT_DATE'}, inplace=True)                                                                                                                                                    
    df_routes_HnN_fix.loc[ df_routes_HnN_fix['VISIT_DURATION'] < 5, ['VISIT_DURATION']]= '5'
    df_routes_HnN_fix['VISIT_DURATION'] = df_routes_HnN_fix['VISIT_DURATION'].astype('float').astype('int').astype('str')   
    df_routes_HnN_fix['VISIT_NUMBER'] = df_routes_HnN_fix['VISIT_NUMBER'].astype('int').astype('str') 
    df_routes_HnN_fix['EXT_ROUTE_ID'] = '0000' + df_routes_HnN_fix['EXT_ROUTE_ID'].astype('str')
    df_routes_HnN_fix['EXT_ROUTE_ID'] = df_routes_HnN_fix['EXT_ROUTE_ID'].str[-4:]  
    df_routes_HnN_fix['PHOTOS_TARGET'] = '10'     # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_HnN_fix['DOCUMENTS_TARGET'] = '8'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_HnN_fix['PHOTOAUDIT_TARGET']= '1'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_HnN_fix['END_DATE']  = '2024-12-31'
    df_routes_HnN_fix['REPEAT_DAYS']  = '0'  # Все  визиты  грузим как разовые, без повторений
    df_routes_HnN_fix['SHIP_TO'] = df_routes_HnN_fix['SHIP_TO'].astype('float').astype('int').astype('str')
    df_routes_HnN_fix[' FIRST_VISIT_DATE'] = df_routes_HnN_fix['FIRST_VISIT_DATE'].astype('str')
   #Упорядочиваем данные
    df_routes_HnN_fix.drop(columns=['WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER'], inplace=True)
    df_routes_HnN_fix = df_routes_HnN_fix[['SHIP_TO', 'ROUTE_NAME', 'FIRST_VISIT_DATE', 'EXT_ROUTE_ID','VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE','PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE','REPEAT_DAYS']]
    return df_routes_HnN_fix


def HnN_converting_FLEX_rtm_to_routes(df_rtm):
    df_rtm = pd.DataFrame(df_rtm.query("ORDER_TYPE == 'FLEX'"))     #ROUTE_TYPE in ('TSS') &
    df_rtm = df_rtm.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'ROUTE_TYPE', 'SHIP_TO']).agg('size').reset_index().drop(columns=0)
    df_rtm['EXT_ROUTE_ID'] = df_rtm['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    df_rtm['EXT_ROUTE_ID'] = '0000' + df_rtm['EXT_ROUTE_ID'].astype('str')
    df_rtm['EXT_ROUTE_ID'] = df_rtm['EXT_ROUTE_ID'].str[-4:]
    df_rtm['FIRST_VISIT_DATE'] = '2024-12-30'
    df_rtm['REPEAT_DAYS']    = '0'
    df_rtm['VISIT_NUMBER']   = '1'
    df_rtm['VISIT_DURATION'] = '100' 
    df_rtm['PHOTOS_TARGET'] = '5' 
    df_rtm['DOCUMENTS_TARGET'] = '5' 
    df_rtm['PHOTOAUDIT_TARGET'] = '1' 
    df_rtm['END_DATE'] = '2024-12-31'
    df_routes_HnN_flex = df_rtm[['SHIP_TO', 'ROUTE_NAME', 'FIRST_VISIT_DATE', 'EXT_ROUTE_ID','VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE','PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE','REPEAT_DAYS']]
    return df_routes_HnN_flex


def HnN_union_FLEX_n_FIX_routes(df_routes_tss_fix, df_routes_tss_flex):
    df_routes = pd.concat([df_routes_tss_fix, df_routes_tss_flex]) 
    df_routes.reset_index(drop=True, inplace=True)
    return df_routes


def add_empty_shipto_HnN(df_routes, DATE_OF_LOAD):
    #Добавляем пустую точку чтобы обозначить системе, что данны маршрут надо целиком перезаписать
    #Danone|TestTerritory_3_Dub33|Dub33|Мерчандайзер||2023-05-08|7|1|130||||
    df_routes_in_case = df_routes.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'EXT_ROUTE_ID','ROUTE_TYPE']).agg('size').reset_index().drop(columns=0)
    df_routes_in_case['FIRST_VISIT_DATE'] = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y')
    df_routes_in_case['FIRST_VISIT_DATE'] = df_routes_in_case['FIRST_VISIT_DATE'].astype(str)
    df_routes_in_case['REPEAT_DAYS']    = '7'
    df_routes_in_case['VISIT_NUMBER']   = '1'
    df_routes_in_case['VISIT_DURATION'] = '130' 
    df_routes_in_case['SHIP_TO'] = ''
    df_routes_in_case[['WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE']] = ''
    df_routes_in_case = df_routes_in_case[['SHIP_TO', 'ROUTE_NAME', 'FIRST_VISIT_DATE', 'WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'EXT_ROUTE_ID',  'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE',  'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE',  'REPEAT_DAYS']]
    
    df_routes['FIRST_VISIT_DATE'] = df_routes['FIRST_VISIT_DATE'].astype(str)
    df_routes[['SHIP_TO','EXT_ROUTE_ID', 'VISIT_NUMBER', 'VISIT_DURATION', 'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'REPEAT_DAYS']] = df_routes[['SHIP_TO','EXT_ROUTE_ID', 'VISIT_NUMBER', 'VISIT_DURATION', 'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'REPEAT_DAYS']].astype('int').astype('str')
    df_routes_HnN = pd.concat([df_routes_in_case, df_routes])
    df_routes_HnN.reset_index(inplace=True, drop=True)
    return df_routes_HnN


def route_type_substitution(df_routes_HnN):
    df_routes_HnN.loc[df_routes_HnN['ROUTE_TYPE'] == 'TSS', 'ROUTE_TYPE'] = 'Мерчандайзер'
    df_routes_HnN.loc[df_routes_HnN['ROUTE_TYPE'] == 'ASM', 'ROUTE_TYPE'] = 'Тим Лид'
    df_routes_HnN.loc[df_routes_HnN['ROUTE_TYPE'] == 'SV',  'ROUTE_TYPE'] = 'Тим Лид'
    return df_routes_HnN

def errors_correction(df_routes_HnN):
    df_routes_HnN['FIRST_VISIT_DATE'].replace(' 00:00:00', '', regex=True, inplace=True) 
    df_routes_HnN['END_DATE'].replace(' 00:00:00', '', regex=True, inplace=True) 
    return df_routes_HnN

