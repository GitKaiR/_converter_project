import pandas as pd
import numpy as np
import csv
import os.path 
from datetime import datetime, timedelta      
pd.options.mode.chained_assignment = None
#import openpyxl


def tl_import_rtm_file(PATH):
    df_rtm = pd.read_excel(PATH, sheet_name='MergeDATA', usecols='A:R',  dtype='str')#, engine = 'pyxlsb')
    df_rtm = df_rtm.replace(r'\n','', regex=True)
    return df_rtm


def tl_rename_columns(df_rtm):
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
    df_rtm['SHIP_TO'] = df_rtm['SHIP_TO'].astype('float').astype('int').astype('str')
    df_rtm['VISIT_DURATION'] = df_rtm['VISIT_DURATION'].astype('float').astype('int') 
    return df_rtm
    

def tl_filters_data(df_rtm, agency_name):    
    columns_list = ['ROUTE_TYPE','SHIP_TO','ROUTE_NAME','ROUTE_ID','FREQUENCY_RTM','AGENCY_NAME','VISIT_DURATION','ROLE','ORDER_TYPE','SCENARIO',
                    'ПН1', 'ВТ1', 'СР1', 'ЧТ1', 'ПТ1', 'СБ1', 'ВС1' ]
    df_rtm = df_rtm[columns_list]
    df_rtm['ROUTE_TYPE'] = df_rtm['ROUTE_TYPE'].str.replace(' ', '').str.lower().str.strip()
    df_rtm = df_rtm.query("ROUTE_TYPE in ('тимлид', 'супервайзер')", engine='python')
    filt = df_rtm['AGENCY_NAME'] == str(agency_name)  
    df_rtm = df_rtm[filt]
    return df_rtm


def tl_converting_rtm_to_routes(df_rtm):
    df_rtm = df_rtm.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'SHIP_TO']).agg('size').reset_index().drop(columns=0)
    df_rtm['EXT_ROUTE_ID'] = df_rtm['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    df_rtm['EXT_ROUTE_ID'] = '0000' + df_rtm['EXT_ROUTE_ID'].astype('str')
    df_rtm['EXT_ROUTE_ID'] = df_rtm['EXT_ROUTE_ID'].str[-4:]
    df_rtm['ROUTE_TYPE'] = 'Тим Лид' 
    df_rtm['FIRST_VISIT_DATE'] = '2024-12-30'
    df_rtm['REPEAT_DAYS']    = '0'
    df_rtm['VISIT_NUMBER']   = '1'
    df_rtm['VISIT_DURATION'] = '100' 
    df_rtm['PHOTOS_TARGET'] = '5' 
    df_rtm['DOCUMENTS_TARGET'] = '5' 
    df_rtm['PHOTOAUDIT_TARGET'] = '1' 
    df_rtm['END_DATE'] = '2024-12-31'
    df_routes = df_rtm[['AGENCY_NAME','ROUTE_NAME','EXT_ROUTE_ID','ROUTE_TYPE','SHIP_TO','FIRST_VISIT_DATE','REPEAT_DAYS','VISIT_NUMBER','VISIT_DURATION', 'PHOTOS_TARGET','DOCUMENTS_TARGET','PHOTOAUDIT_TARGET', 'END_DATE']]
    return df_routes



def tl_add_empty_shipto(df_routes, DATE_OF_LOAD):
    #Danone|TestTerritory_3_Dub33|Dub33|Мерчандайзер||2023-05-08|7|1|130||||
    df_routes_in_case = df_routes.groupby(by=['AGENCY_NAME', 'ROUTE_NAME', 'EXT_ROUTE_ID']).agg('size').reset_index().drop(columns=0)
    df_routes_in_case['FIRST_VISIT_DATE'] = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y')
    df_routes_in_case['FIRST_VISIT_DATE'] = df_routes_in_case['FIRST_VISIT_DATE'].astype('str')
    df_routes_in_case['REPEAT_DAYS']    = '7'
    df_routes_in_case['VISIT_NUMBER']   = '1'
    df_routes_in_case['VISIT_DURATION'] = '130' 
    df_routes_in_case['ROUTE_TYPE'] = 'Тим Лид' 
    df_routes_in_case['SHIP_TO'] = ''
    df_routes_in_case[['PHOTOS_TARGET', 'DOCUMENTS_TARGET', 'PHOTOAUDIT_TARGET', 'END_DATE']] = ''
    df_routes_in_case = df_routes_in_case[['AGENCY_NAME','ROUTE_NAME','EXT_ROUTE_ID','ROUTE_TYPE','SHIP_TO','FIRST_VISIT_DATE','REPEAT_DAYS','VISIT_NUMBER','VISIT_DURATION', 'PHOTOS_TARGET','DOCUMENTS_TARGET','PHOTOAUDIT_TARGET', 'END_DATE']]
    df_routes = pd.concat([df_routes_in_case, df_routes])
    df_routes_in_case['FIRST_VISIT_DATE'] = df_routes_in_case['FIRST_VISIT_DATE'].astype('str')
    return df_routes