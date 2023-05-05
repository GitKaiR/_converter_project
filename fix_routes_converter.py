#=========================================================================================================
#=================================   КОНВЕРТОР ФИКСИРОВАННЫХ МАРШРУТОВ   =================================
#=========================================================================================================

import pandas as pd
import os.path 
import csv
from interface import input_path, input_load_date
from datetime import datetime, timedelta    
from routes_export import export_new_routes_files
pd.options.mode.chained_assignment = None


def fix_import_rtm_file(PATH):
    df_rtm = pd.read_excel(PATH, sheet_name='MergeDATA', usecols='A:AM')#, engine = 'pyxlsb')
    return df_rtm



def fix_rename_columns(df_rtm):
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
    filt = df_rtm['ROUTE_TYPE'] == 'Мерчандайзер'
    df_rtm = df_rtm[filt]
    filt = df_rtm['ORDER_TYPE'] == 'FIX'  #FLEX     
    df_rtm = df_rtm[filt]
    return df_rtm


#Форматируем список маршрутов

def fix_unpivot_rtm(df_rtm):
    df_rtm = pd.melt(df_rtm, id_vars=['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO'])
    renames = {'variable'  :  'DAY_NAME', 
               'value'     :  'VISIT_NUMBER'}
    df_rtm.rename(columns=renames, inplace=True)
    df_rtm['WEEK_1234_ORDER'] = df_rtm['DAY_NAME'].str[-1]
    df_rtm['WEEK_1234_ORDER'] = df_rtm['WEEK_1234_ORDER'].astype('int')
    df_rtm['FREQUENCY_RTM'] = df_rtm['FREQUENCY_RTM'].astype('float')
    df_rtm['DAY_NAME']    = df_rtm['DAY_NAME'].str[0:2]
    df_rtm['DAY_NAME']    = df_rtm['DAY_NAME'].astype('str')
    df_rtm['EXT_ROUTE_ID'] = df_rtm['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    filt = df_rtm['VISIT_NUMBER'].notnull()
    df_rtm = df_rtm[filt]
    df_rtm['ROW_NUMBER'] = df_rtm.groupby(by=['ROUTE_NAME','DAY_NAME','WEEK_1234_ORDER' ]).cumcount()+1
    df_rtm.loc[df_rtm['VISIT_NUMBER'] == 1, ['VISIT_NUMBER']] = df_rtm['ROW_NUMBER']
    return df_rtm


#Формируем календарь на год вперед
def fix_create_calender(DATE_OF_LOAD):
    df_calender = pd.DataFrame(pd.date_range('2023-01-01', '2023-12-31'), columns=['DATE'] )
    df_calender['WEEKDAY_NUMBER'] = df_calender['DATE'].dt.weekday+1
    df_calender['DAY_NAME'] = df_calender['WEEKDAY_NUMBER'] 
    df_calender['WEEK_NUMBER'] = df_calender['DATE'].dt.strftime("%W").astype('int')+ 1  #еще можно так .dt.isocalendar().week, но тут не стандартные номера недель
    df_calender['WEEK_1234_ORDER'] = ((df_calender['WEEK_NUMBER']  / 4)  - (df_calender['WEEK_NUMBER']  // 4)) * 4
    df_calender.loc[df_calender['WEEK_1234_ORDER'] == 0 ,   'WEEK_1234_ORDER'] = 4
    df_calender['WEEK_1234_ORDER'] = df_calender['WEEK_1234_ORDER'].astype('int')
    df_calender.replace({'DAY_NAME' : { 1 : 'ПН', 2 : 'ВТ', 3 : 'СР', 4 : 'ЧТ', 5 : 'ПТ', 6 : 'СБ', 7 : 'ВС'}}, inplace=True)
    DATE_OF_END = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y') +  + timedelta(days=27)
    filt = (df_calender['DATE'] >= pd.to_datetime(DATE_OF_LOAD)) & (df_calender['DATE'] <= DATE_OF_END)
    df_calender = df_calender[filt]
    return df_calender


def fix_converting_rtm_to_routes(df_rtm, df_calender):
    #df_rtm_keys = df_rtm.pivot_table(index= ['SHIP_TO', 'ROUTE_NAME']).reset_index()
    df_rtm_keys = df_rtm.groupby(by=['SHIP_TO', 'ROUTE_NAME']).agg('size').reset_index().drop(columns=0)
    df_rtm_keys = df_rtm_keys[['SHIP_TO', 'ROUTE_NAME']]
    df_routes = pd.merge(df_rtm_keys, df_calender, how='cross' )
    df_routes = pd.merge(df_routes, df_rtm[['SHIP_TO', 'ROUTE_NAME', 'EXT_ROUTE_ID', 'DAY_NAME', 'WEEK_1234_ORDER', 'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE']], how='left',  on=['SHIP_TO', 'ROUTE_NAME', 'DAY_NAME', 'WEEK_1234_ORDER'])
    df_routes.dropna(subset=['VISIT_NUMBER'], inplace=True)
    df_routes.rename(columns={'DATE': 'FIRST_VISIT_DATE'}, inplace=True)                                                                                                                                                    
    df_routes.loc[ df_routes['VISIT_DURATION'] < 5, ['VISIT_DURATION']]= '5'
    df_routes['VISIT_DURATION'] = df_routes['VISIT_DURATION'].astype('int').astype('str')   
    df_routes['VISIT_NUMBER'] = df_routes['VISIT_NUMBER'].astype('int').astype('str') 
    df_routes['EXT_ROUTE_ID'] = '0000' + df_routes['EXT_ROUTE_ID'].astype('str')
    df_routes['EXT_ROUTE_ID'] = df_routes['EXT_ROUTE_ID'].str[-4:]  
    df_routes['PHOTOS_TARGET'] = '10'     # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes['DOCUMENTS_TARGET'] = '8'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes['PHOTOAUDIT_TARGET']= '1'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes['END_DATE']  = ''
    df_routes['REPEAT_DAYS']  = '28' 
    return df_routes


def add_empty_shipto(df_routes, DATE_OF_LOAD):
    #Danone|TestTerritory_3_Dub33|Dub33|Мерчандайзер||2023-05-08|7|1|130||||
    df_routes_in_case = df_routes.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'EXT_ROUTE_ID']).agg('size').reset_index().drop(columns=0)
    df_routes_in_case['FIRST_VISIT_DATE'] = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y')
    df_routes_in_case['REPEAT_DAYS']    = '7'
    df_routes_in_case['VISIT_NUMBER']   = '1'
    df_routes_in_case['VISIT_DURATION'] = '130' 
    df_routes_in_case['ROUTE_TYPE'] = 'Мерчандайзер' 
    df_routes_in_case['SHIP_TO'] = ''
    df_routes_in_case[['WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE']] = ''
    df_routes_in_case = df_routes_in_case[['SHIP_TO', 'ROUTE_NAME', 'FIRST_VISIT_DATE', 'WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'EXT_ROUTE_ID',  'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE',  'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE',  'REPEAT_DAYS']]
    df_routes_flex = pd.concat([df_routes_in_case, df_routes])
    return df_routes




