#=========================================================================================================
#=================================   КОНВЕРТОР ГИБКИХ МАРШРУТОВ   =================================
#=========================================================================================================

import pandas as pd
import numpy as np
import csv
import os.path 
from datetime import datetime, timedelta        
from routes_export import export_new_routes_files
from interface import input_path, input_load_date, print_processed_agency, show_routs_wo_employee, show_new_routes, show_flex_routes_memo
from emploee_converter import  import_emploee_data, convert_emploee_data
pd.options.mode.chained_assignment = None
#import openpyxl


def flex_import_rtm_file(PATH):
    df_rtm = pd.read_excel(PATH, sheet_name='MergeDATA', usecols='A:R')#, engine = 'pyxlsb')
    return df_rtm

def flex_rename_columns(df_rtm):
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
    return df_rtm
    
def flex_filters_data(df_rtm, agency_name):    
    columns_list = ['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO',
                    'ПН1', 'ВТ1', 'СР1', 'ЧТ1', 'ПТ1', 'СБ1', 'ВС1' ]
    df_rtm = df_rtm[columns_list]
    filt = df_rtm['ROUTE_TYPE'] == 'Мерчандайзер'
    df_rtm = df_rtm[filt]
    filt = (df_rtm['ORDER_TYPE'] == 'FLEX')  &  (df_rtm['AGENCY_NAME'] == str(agency_name))   
    df_rtm = df_rtm[filt]
    return df_rtm


def set_agency_list(df_rtm):
    agency_list = df_rtm['AGENCY_NAME'].unique().tolist()
    return agency_list


def flex_unpivot_rtm(df_rtm):
    df_rtm_flex = pd.melt(df_rtm, id_vars=['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO'])
    renames = {'variable'  :  'DAY_NAME', 
               'value'     :  'VISIT_NUMBER'} 
    df_rtm_flex.rename(columns=renames, inplace=True)
    filt = df_rtm_flex['VISIT_NUMBER'].notnull()
    df_rtm_flex = df_rtm_flex[filt]
    df_rtm_flex['WEEK_1234_ORDER'] = df_rtm_flex['DAY_NAME'].str[-1]
    df_rtm_flex['WEEK_1234_ORDER'] = df_rtm_flex['WEEK_1234_ORDER'].astype('int')
    df_rtm_flex['DAY_NAME']    = df_rtm_flex['DAY_NAME'].str[0:2]
    df_rtm_flex['DAY_NAME']    = df_rtm_flex['DAY_NAME'].astype('str')
    df_rtm_flex['EXT_ROUTE_ID'] = df_rtm_flex['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    df_rtm_flex['ROW_NUMBER'] = df_rtm_flex.groupby(by=['ROUTE_NAME','DAY_NAME','WEEK_1234_ORDER' ]).cumcount()+1
    df_rtm_flex.loc[df_rtm_flex['VISIT_NUMBER'] == 1, ['VISIT_NUMBER']] = df_rtm_flex['ROW_NUMBER']
    return df_rtm_flex

def flex_create_calender(DATE_OF_LOAD):
    df_calender = pd.DataFrame(pd.date_range('2023-01-01', '2023-12-31'), columns=['DATE'] )
    df_calender['WEEKDAY_NUMBER'] = df_calender['DATE'].dt.weekday+1
    df_calender['DAY_NAME'] = df_calender['WEEKDAY_NUMBER'] 
    df_calender['WEEK_NUMBER'] = df_calender['DATE'].dt.strftime("%W").astype('int')+ 1  #еще можно так .dt.isocalendar().week, но тут не стандартные номера недель
    df_calender['WEEK_1234_ORDER'] = 1
    df_calender['WEEK_1234_ORDER'] = df_calender['WEEK_1234_ORDER'].astype('int')
    df_calender.replace({'DAY_NAME' : { 1 : 'ПН', 2 : 'ВТ', 3 : 'СР', 4 : 'ЧТ', 5 : 'ПТ', 6 : 'СБ', 7 : 'ВС'}}, inplace=True)
    DATE_OF_END = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y') +  + timedelta(days=60)
    filt = (df_calender['DATE'] >= pd.to_datetime(DATE_OF_LOAD)) & (df_calender['DATE'] <= DATE_OF_END)
    df_calender = df_calender[filt]
    return df_calender 

def flex_converting_rtm_to_routes(df_rtm_flex, df_calender):
    #df_rtm_keys = df_rtm_flex.pivot_table(index= ['SHIP_TO', 'ROUTE_NAME']).reset_index()
    df_rtm_keys = df_rtm_flex.groupby(by=['SHIP_TO', 'ROUTE_NAME']).agg('size').reset_index().drop(columns=0)
    df_rtm_keys = df_rtm_keys[['SHIP_TO', 'ROUTE_NAME']]
    df_routes_flex = pd.merge(df_rtm_keys, df_calender, how='cross' )
    df_routes_flex = pd.merge(df_routes_flex, df_rtm_flex[['SHIP_TO', 'ROUTE_NAME', 'EXT_ROUTE_ID', 'DAY_NAME', 'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE']], how='left',  on=['SHIP_TO', 'ROUTE_NAME', 'DAY_NAME'])
    df_routes_flex.dropna(subset=['VISIT_NUMBER'], inplace=True)
    df_routes_flex.rename(columns={'DATE': 'FIRST_VISIT_DATE'}, inplace=True)
    df_routes_flex.loc[ df_routes_flex['VISIT_DURATION'] < 5, ['VISIT_DURATION']]=5
    df_routes_flex['VISIT_DURATION'] = df_routes_flex['VISIT_DURATION'].astype('int').astype('str')   
    df_routes_flex['VISIT_NUMBER'] = df_routes_flex['VISIT_NUMBER'].astype('int').astype('str')
    df_routes_flex['EXT_ROUTE_ID'] = '0000' + df_routes_flex['EXT_ROUTE_ID'].astype('str')
    df_routes_flex['EXT_ROUTE_ID'] = df_routes_flex['EXT_ROUTE_ID'].str[-4:]
    df_routes_flex['PHOTOS_TARGET'] = '10'     # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_flex['DOCUMENTS_TARGET'] = '8'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_flex['PHOTOAUDIT_TARGET']= '1'   # - Задай им 1 или 0, Система будет их требовать, нужна какая-нибудь циферка, если хотим совсем невалидное можно -1 указать, допустимо любое число.
    df_routes_flex['END_DATE']  = ''
    df_routes_flex['REPEAT_DAYS']  = '0' #Гибкие маршруты теперь заливаем так же как и фиксированные, просто вместо цикличности указывать 0 (без цикла). 
    return df_routes_flex


def add_empty_shipto(df_routes_flex, DATE_OF_LOAD):
    #Danone|TestTerritory_3_Dub33|Dub33|Мерчандайзер||2023-05-08|7|1|130||||
    df_routes_in_case = df_routes_flex.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'EXT_ROUTE_ID']).agg('size').reset_index().drop(columns=0)
    df_routes_in_case['FIRST_VISIT_DATE'] = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y')
    df_routes_in_case['REPEAT_DAYS']    = '7'
    df_routes_in_case['VISIT_NUMBER']   = '1'
    df_routes_in_case['VISIT_DURATION'] = '130' 
    df_routes_in_case['ROUTE_TYPE'] = 'Мерчандайзер' 
    df_routes_in_case['SHIP_TO'] = ''
    df_routes_in_case[['WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE']] = ''
    df_routes_in_case = df_routes_in_case[['SHIP_TO', 'ROUTE_NAME', 'FIRST_VISIT_DATE', 'WEEKDAY_NUMBER', 'DAY_NAME', 'WEEK_NUMBER', 'WEEK_1234_ORDER', 'EXT_ROUTE_ID',  'VISIT_NUMBER', 'AGENCY_NAME', 'VISIT_DURATION', 'ROUTE_TYPE',  'PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE',  'REPEAT_DAYS']]
    df_routes_flex = pd.concat([df_routes_in_case, df_routes_flex])
    return df_routes_flex

#df_rtm_keys = df_rtm_flex.pivot_table(index= ['SHIP_TO', 'ROUTE_NAME']).reset_index()


#df = flex_rtm_convertor()
#df.to_csv(r"C:\Users\ROKOTYEV\OneDrive - Danone\Desktop\test\Flex_routes\New Text Document.csv")