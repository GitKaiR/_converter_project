#=========================================================================================================
#========================================   КОНВЕРТОР СОТРУДНИКОВ   ======================================
#=========================================================================================================
import pandas as pd
import os.path 
import csv
from interface import input_path
pd.options.mode.chained_assignment = None
from interface import show_new_routes


#def emp_convert_emploee_data(PATH):
#    df_merch_list = pd.read_excel(PATH, sheet_name='DMT_set_Agent', dtype='str')
#    df_merch_routes = pd.read_excel(PATH, sheet_name='DMT_Set_ClassifyEx', dtype='str')
#
#    export_path_emp = os.path.dirname(PATH) + "\DMT_set_Agent.txt"
#    df_merch_list.to_csv(export_path_emp, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
#
#    print('')
#    print('Данные сохранены в файл #1: ' + str(export_path_emp))
#
#    export_path_routes = os.path.dirname(PATH) + "\DMT_Set_ClassifyEx.txt"
#    df_merch_routes.to_csv(export_path_routes, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
#
#    print('')
#    print('Данные сохранены в файл #2: ' + str(export_path_routes))


def import_emploee_data(PATH):
    df_employees = pd.read_excel(PATH, sheet_name='Сотрудники', usecols='A:D', skiprows=5, dtype='str')#, engine = 'pyxlsb')
    df_employees.rename(columns={'Маршрут': 'ROUTE_NAME', 'Внешний код': 'EMPLOYEE_ID', 'ФИО': 'EMPLOYEE_NAME', 'Код': 'EMPLOYEE_INNER_ID'}, inplace=True)
    return df_employees


def convert_emploee_data(df_employees, df_rtm):    
    df_employees = pd.DataFrame(df_employees[['ROUTE_NAME','EMPLOYEE_ID']])

    df_new_routes = df_rtm.groupby(by=['ROUTE_NAME']).agg('size').reset_index().drop(columns=0)
    df_show_new_routes = pd.merge(df_employees, df_new_routes, on='ROUTE_NAME', how='outer', indicator=True).query("_merge == 'right_only'")
    df_employees = pd.merge(df_employees, df_new_routes, on='ROUTE_NAME', how='outer', indicator=True).query("_merge == 'right_only' or  _merge == 'both'").drop(columns='_merge')
    df_employees.dropna(subset=['ROUTE_NAME'], inplace=True)
    df_employees['EMPLOYEE_ID'] = df_employees['EMPLOYEE_ID'].fillna('0')

    df_employees['EXT_ROUTE_ID'] = df_employees['ROUTE_NAME'].str.split('_').str[-1].str.strip()
    df_employees['EXT_ROUTE_ID'] = '0000' + df_employees['EXT_ROUTE_ID'].astype('str')
    df_employees['EXT_ROUTE_ID'] = df_employees['EXT_ROUTE_ID'].str[-4:]  
    df_employees['ROUTE_TYPE'] = df_employees['ROUTE_NAME'].str.split('_').str[0].str.strip().str[0:3]
    df_employees.query("ROUTE_TYPE == 'MRC' or ROUTE_TYPE == 'Tes'", inplace=True)
    df_employees['EMPLOYEE_AND_VACANTS'] = df_employees['EMPLOYEE_ID']
    df_employees.loc[df_employees['EMPLOYEE_ID'] == '0', 'EMPLOYEE_AND_VACANTS'] = "Вакант_для_маршрута_" + df_employees['EXT_ROUTE_ID'].str[-5:]
    return df_employees, df_show_new_routes



#def employee_convertor(df_employees, PATH, folder, agency=''):
#    PATH = input_path()
#    if PATH != 'error':
#        df_employees = emp_convert_emploee_data(PATH)
#        export_employee_files(df_employees, PATH, folder, agency='')
        
