#========================================   ЭКСПОРТ   ======================================

import pandas as pd
import numpy as np
import csv
import os.path 
from datetime import datetime, timedelta
import shutil as shutil
pd.options.mode.chained_assignment = None

def create_folder(path, folder, agency=''):
    if agency != '':     agency = "\\" + str(agency)
    dir_path = os.path.dirname(path) + "\\" + str(folder) + agency
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path)



def export_setagent( df_employees, path, folder, agency=''):
    '''Создаем новых сотрудников через файл DMT_set_Agent.txt'''
                #код сотрудника|1|Имя сотрудника|Сокращенное имя сотрудника|||||5210004
                #код Тим Лида|1|Имя Тим лида|Сокращенное имя Тим лида|||||5210003
                #код сотрудника2|1|Имя сотрудника2|Сокращенное имя сотрудника2|||||5210004
                #код Тим Лида2|1|Имя Тим лида2|Сокращенное имя Тим лида2|||||5210003
    df_new_employees = df_employees[df_employees['EMPLOYEE_INNER_ID'].isna()]
    if len(df_new_employees) > 0:
        df_new_employees = pd.DataFrame(df_new_employees.groupby(by=['EMPLOYEE_ID','EMPLOYEE_NAME']).agg('size')).reset_index().drop(columns=0)
        df_new_employees[1] = df_new_employees['EMPLOYEE_ID']
        df_new_employees[3] = df_new_employees['EMPLOYEE_NAME']
        df_new_employees[4] = df_new_employees['EMPLOYEE_NAME']
        df_new_employees[2] = '1'
        df_new_employees[[5,6,7,8]] = np.NaN
        df_new_employees[9] = '5210004'
        df_new_employees = df_new_employees[[1,2,3,4,5,6,7,8,9]]
        if agency != '':     agency = "\\" + str(agency)
        export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_set_Agent.txt"
        df_new_employees.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
        print('')
        print('Данные сохранены в файл: ' + str(export_path))


def export_setclassify( df_employees, path, folder, agency=''):
    '''Связка кода сотрудника Вакант с кодом маршрута через файл DMT_Set_ClassifyEx.txt'''
    if 'EXT_ROUTE_ID' not in df_employees.columns:
        df_employees['EXT_ROUTE_ID'] =  df_employees['ROUTE_NAME'].str.split('_').str[-1].str.strip()
        df_employees['EXT_ROUTE_ID'] = '0000' + df_employees['EXT_ROUTE_ID'].astype('str')
        df_employees['EXT_ROUTE_ID'] =  df_employees['EXT_ROUTE_ID'].str[-4:] 

    # Опеределяем роль для маршрута
    df_employees['ROUTE_PREFIX'] = df_employees['ROUTE_NAME'].str.split('_').str[0].str.strip()
    df_employees.loc[df_employees['ROUTE_PREFIX'] == 'ASM', 'ROLE_ID'] = '15'
    df_employees.loc[df_employees['ROUTE_PREFIX'] == 'SV', 'ROLE_ID'] = '15'
    df_employees.loc[df_employees['ROUTE_PREFIX'] == 'TL', 'ROLE_ID'] = '15'
    df_employees['ROLE_ID'].fillna('16', inplace=True)

    # Создаем вакантов
    #df_employees['EMPLOYEE_ID'].fillna(df_employees['ROUTE_PREFIX'] + '_VACANT_' +  df_employees['EXT_ROUTE_ID'], inplace=True)
    #df_employees['EMPLOYEE_NAME'].fillna(df_employees['ROUTE_PREFIX'] + '_VACANT_' +  df_employees['EXT_ROUTE_ID'], inplace=True)
    
    df_employees = pd.DataFrame(df_employees.groupby(by=['ROLE_ID', 'EXT_ROUTE_ID', 'EMPLOYEE_ID', 'EMPLOYEE_NAME']).agg('size')).reset_index()
    #df_empty_employees = pd.DataFrame(df_employees_export[df_employees_export['EMPLOYEE_ID'] == '0'])
    df_employees[1] = df_employees['ROLE_ID']
    df_employees[2] = '0:0:"' + df_employees['EXT_ROUTE_ID'] +'";1:1:"' + df_employees['EMPLOYEE_ID'] + '"'
    df_employees[3] = '1#1'
    df_employees = df_employees[[1,2,3]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_ClassifyEx.txt"
    df_employees.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))



def export_setmobfaceadd(df_routes, path, folder, agency=''):
    '''DMT_Set_MobFaces_Add.txt с набором сотрудник-точка. Тут только 2 поля именно в этом порядке'''
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export = pd.DataFrame(df_routes_export.groupby(by=[ 'SHIP_TO', 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
    df_routes_export[1] = df_routes_export['EMPLOYEE_AND_VACANTS']
    #df_routes_export[2] = df_routes_export['SHIP_TO'].astype('float').astype('int').astype('str')
    df_routes_export = df_routes_export[[1,2]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_MobFaces_Add.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))


def export_setnodes(df_routes, path, folder, agency=''):
    #DMT_Set_Nodes
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export = pd.DataFrame(df_routes_export['ROUTE_NAME'].unique())
    df_routes_export[2] = df_routes_export[0]
    df_routes_export[5] = df_routes_export[0]
    df_routes_export[6] = df_routes_export[0]
    df_routes_export[[1,4,8]] = np.NaN
    df_routes_export[3] = '3'
    df_routes_export[[7,9,10]] = '1'
    df_routes_export = df_routes_export[[1,2,3,4,5,6,7,8,9,10]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_Nodes.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))


def export_setrouteheaders(df_routes, path, folder, agency=''):
    '''DMT_SET_ROUTEHEADERS_Batch - Разовые маршруты Создаем заголовки маршрутов и распределяем их по датам'''
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export = pd.DataFrame(df_routes_export.groupby(by=['ROUTE_NAME', 'DATE']).agg('size')).reset_index()
    df_routes_export['DATE'] = df_routes_export['DATE'].astype('str')
    df_routes_export[1] = df_routes_export['ROUTE_NAME']   + "_DATE_" +  df_routes_export['DATE']
    df_routes_export[[2,5,6,7,9,10,11]] = np.NaN
    df_routes_export[3] = df_routes_export['DATE']                                                                    #datetime.now().strftime("%Y-%m-%d")
    df_routes_export[4] = df_routes_export['ROUTE_NAME']   + "_DATE_" +  df_routes_export['DATE']                     #datetime.now().strftime("%d_%m_%Y")
    df_routes_export[8] = '1'
    df_routes_export = df_routes_export[[1,2,3,4,5,6,7,8,9,10,11]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_SET_ROUTEHEADERS_Batch.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))


def export_setrouteobjectsbatch(df_routes, path, folder, agency=''):
    '''DMT_Set_RouteObjects_Batch  - Разовые маршруты: Привязка сотрудникам к маршрутам'''
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export = pd.DataFrame(df_routes_export.groupby(by=['ROUTE_NAME', 'EMPLOYEE_AND_VACANTS', 'DATE']).agg('size')).reset_index()
    df_routes_export['DATE'] = df_routes_export['DATE'].astype('str')
    df_routes_export[1] = df_routes_export['ROUTE_NAME']   + "_DATE_" +   df_routes_export['DATE']
    df_routes_export[[2,5,7]] = np.NaN
    df_routes_export[3] = '2'
    df_routes_export[4] = df_routes_export['EMPLOYEE_AND_VACANTS']
    df_routes_export[6] = '1'
    df_routes_export = df_routes_export[[1,2,3,4,5,6,7]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RouteObjects_Batch.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))


def export_setroutepointsbatch(df_routes, path, folder, agency=''):
    '''DMT_Set_RoutePoints_Batch.txt - Разовые маршруты: Привязываем точки к маршрутам'''
    '''файл с заполнением по формату. В качестве кода маршрута тут должен быть тот же код, что и в файле'''
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export['VISIT_SEQUENCE'] = df_routes_export.groupby(['ROUTE_NAME','DATE']).cumcount() + 1   # Считаем порядок строк
    df_routes_export = pd.DataFrame(df_routes_export.groupby(by=['ROUTE_NAME', 'SHIP_TO', 'DATE','VISIT_SEQUENCE']).agg('size')).reset_index()
    df_routes_export['DATE'] = df_routes_export['DATE'].astype('str')
    df_routes_export[1] = df_routes_export['ROUTE_NAME']   + "_DATE_" +  df_routes_export['DATE']
    df_routes_export[[2,3,5,9,10,11,12,13,14,15,16,18]] = np.NaN
    df_routes_export[4] = df_routes_export['VISIT_SEQUENCE']
    #df_routes_export[6] = df_routes_export['SHIP_TO'].astype('float').astype('int').astype('str')
    df_routes_export[[7,8]] = np.NaN
    df_routes_export[17] = '1'
    df_routes_export = df_routes_export[[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]]
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RoutePoints_Batch.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))


def fix_export_route_file(df_routes, path):
    '''Экспорт файла фиксированных маршрутов в старом формате (до доработки по историчности)'''
    renames = { 'AGENCY_NAME'        : 'OwnerExid',     #-- Код Агенства
                'ROUTE_NAME'         : 'TerrName',      #-- Код маршрута"
                'ROUTE_TYPE'         : 'TerrRole',      #-- Роль сотрудника (мерчендайзер, Тим Лид)
                'SHIP_TO'            : 'FaceExid',      #-- Код торговой точки
                'FIRST_VISIT_DATE'   : 'StartDate' ,    #-- Дата начала посещений, не раньше чем завтра
                'REPEAT_DAYS'        : 'Period',        #-- Цикличность (1, 7, 14, 21, 28)
                'VISIT_NUMBER'       : 'SortID',        #-- порядок точки в дне на маршруте
                'VISIT_DURATION'     : 'Duration'}      #-- длительность посещения в минутах
    df_routes_export = df_routes[['AGENCY_NAME','ROUTE_NAME','ROUTE_TYPE','SHIP_TO','FIRST_VISIT_DATE','REPEAT_DAYS','VISIT_NUMBER','VISIT_DURATION']].rename(columns=renames)  
    #df_routes_export['SHIP_TO'] = df_routes_export['SHIP_TO'].astype('float').astype('int').astype('str')
    export_path = os.path.dirname(path) + "\ROUTES_Data" + ".txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8')
    print('')
    print('Данные сохранены в файл: ' + str(export_path))



def export_setclientservice(df_routes, path, folder, agency=''): 
    '''Экспорт единого файла фиксированных и гибких маршрутов в новом формате (ПОСЛЕ доработки по историчности) через файл DMT_User273_SetClientServiceMatrix_Batch.txt'''
    renames = { 'AGENCY_NAME'        : 'OwnerExid',                 # 1 --- Принадлежность записи к агентству
                'ROUTE_NAME'         : 'TerrName',                  # 2 --- Наименование маршрута"
                'EXT_ROUTE_ID'       : 'TerrExid',                  # 3 --- Короткий код маршрута (последние 4 цифры в названии)
                'ROUTE_TYPE'         : 'TerrRole',                  # 4 --- Роль сотрудника (мерчендайзер, Тим Лид)
                'SHIP_TO'            : 'FaceExid',                  # 5 --- Код торговой точки
                'FIRST_VISIT_DATE'   : 'StartDate',                 # 6 --- Дата начала посещений, не раньше чем завтра
                'REPEAT_DAYS'        : 'Period',                    # 7 --- Цикличность (0,1,4,7,14,21,28)
                'VISIT_NUMBER'       : 'SortID',                    # 8 --- порядок в дне на маршруте
                'VISIT_DURATION'     : 'Duration',                  # 9 --- длительность посещения в минутах
                'PHOTOS_TARGET'      : 'PlanFotoCountPerAudit',     # 10--- Плановое количество фотографий на один фотоаудит
                'DOCUMENTS_TARGET'   : 'PlanDocCountPerAudit',      # 11--- Плановое количество документов на один фотоаудит
                'PHOTOAUDIT_TARGET'  : 'PlanAuditCountPerVisit',    # 12--- Плановое количество фотоаудитов на один визит
                'END_DATE'           : 'EndDate'}                   # 13--- Дата окончания цикла планового посещения
    df_routes_export = df_routes[['AGENCY_NAME','ROUTE_NAME','EXT_ROUTE_ID','ROUTE_TYPE','SHIP_TO','FIRST_VISIT_DATE','REPEAT_DAYS','VISIT_NUMBER','VISIT_DURATION', 'PHOTOS_TARGET','DOCUMENTS_TARGET','PHOTOAUDIT_TARGET', 'END_DATE']].rename(columns=renames)  
    #df_routes_export['SHIP_TO'] = df_routes_export['SHIP_TO'].astype('float').astype('int').astype('str')
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_User273_SetClientServiceMatrix_Batch.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))
    
#DMT_USER273_DelRoutes_Batch.txt В нем 2 параметра 
#TerrExid nvarchar(50) - код маршрута
#OwnerExid nvarchar(50) - код площадки
#Последний столбец не обязателен, если запускать на агентстве Коды территорий/маршрутов должны быть с нулями
#0123|MAX GROUP|
#Процедура удаления сделает следующее:
#1.	Отвязать сотрудников от этих территории если есть связка
#2.	Удалит территорию из Территориального деления точек, вместе со всеми точками в нее входящими. Сами точки не деактивируем, только деклассифируем из этой ветки.
#3.	Деактивирует плановые маршруты на данную территорию начиная с завтрашнего дня из таблички DS_User273_Routes. 

def export_DelRoute(df_routes_to_delete, path, folder, agency=''):
    '''Экспорт маршрутов которые должны быть удалены через файл DMT_USER273_DelRoutes_Batch.txt'''
    df_routes_to_delete['ROUTE_ID'] = df_routes_to_delete['ROUTE_ID'].str.split('_').str[-1].str.strip()
    df_routes_to_delete['ROUTE_ID'] = '0000' + df_routes_to_delete['ROUTE_ID'].astype('str')
    df_routes_to_delete['ROUTE_ID'] = df_routes_to_delete['ROUTE_ID'].str[-4:]  
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_USER273_DelRoutes_Batch.txt"
    df_routes_to_delete.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
    print('')
    print('Данные сохранены в файл: ' + str(export_path))

#Было: DMT_Set_RouteObjects_Batch_Commit.txt – файл должен содержать перевод строки
#Нужно: DMT_Set_RouteObjects_Batch_Commit.txt – файл должен содержать 1 и перевод строки
#DMT_Set_RoutePoints_Batch_Init.txt – файл должен содержать перевод строки

def export_simple_files( path, folder, agency=''):
    if agency != '':     agency = "\\" + str(agency)
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_SET_ROUTEHEADERS_Batch_Init.txt", "w+")
    my_file.write("\n")
    my_file.close()
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RouteObjects_Batch_Init.txt", "w+")
    my_file.write("\n")
    my_file.close()
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RoutePoints_Batch_Init.txt", "w+") #<<<<<
    my_file.write("\n")
    my_file.close()
    #DMT_Set_MobFaces_Clear.txt с переводом каретки, он подготовит времянку для пакета
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_MobFaces_Clear.txt", "w+")
    my_file.write("\n")
    my_file.close()
    #DMT_Set_MobFaces_Confirm.txt с вот таким содержимым  0|1|11
    #В этом случае от сотрудника, которого подали в DMT_Set_MobFaces_Add будут отвязаны все точки, которых не было в пакете, остальные останутся или привяжутся
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_MobFaces_Confirm.txt", "w+")
    my_file.write("0|1|11")
    my_file.close()
    
    #Надо передать агентствам, чтобы в DMT_Set_RouteHeaders_Batch_Commit.txt подавали 1ку, тогда все чего нет в пакете будет деактивироваться. 
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RouteHeaders_Batch_Commit.txt", "w+")
    my_file.write("1")
    my_file.close()
    
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RouteObjects_Batch_Commit.txt", "w+")
    my_file.write("1\n")
    my_file.close()
    #DMT_Set_RoutePoints_Batch_Commit.txt – файл должен содержать 0 или 1.
    #Если 0, то на входе ожидаются только изменения, если 1, то пакет для маршрута считается полным и точки которых нет в пакете, будут исключены из маршрута
    my_file = open(os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_RoutePoints_Batch_Commit.txt", "w+")
    my_file.write("1")
    my_file.close()
    print("")
    print("Создан служебный файлы:  DMT_SET_ROUTEHEADERS_Batch_Init.txt")
    print("Создан служебный файлы:  DMT_Set_RoutePoints_Batch_Init.txt")  
    print("Создан служебный файлы:  DMT_Set_RouteObjects_Batch_Init.txt")
    print("Создан служебный файлы:  DMT_Set_RoutePoints_Batch_Init.txt") 
    print("Создан служебный файлы:  DMT_Set_MobFaces_Clear.txt")
    print("Создан служебный файлы:  DMT_Set_MobFaces_Confirm.txt")
    print("Создан служебный файлы:  DMT_Set_RouteHeaders_Batch_Commit.txt")
    print("Создан служебный файлы:  DMT_Set_RouteObjects_Batch_Commit.txt")
    print("Создан служебный файлы:  DMT_Set_RoutePoints_Batch_Commit.txt")




#def export_route_file(df_routes, df_employees, path, folder, agency=''): 
#    create_folder(path, folder, agency) 
#    show_routs_wo_employee(df_routes)
#    #export_setmobfaceadd(df_employees, path, folder, agency)
#    #flex_export_setnodes(df_routes, path, folder, agency)
#    #export_setrouteheaders(df_routes, path, folder, agency)
#    #export_setrouteobjectsbatch(df_routes, path, folder, agency)
#    #export_setroutepointsbatch(df_routes, path, folder, agency)
#    export_simple_files(path, folder, agency)

def export_new_routes_files(df_routes, path, folder, df_employees='',agency=''): 
    create_folder(path, folder, agency)
    export_setclientservice(df_routes, path, folder, agency)  
#    if len(df_employees) > 0:
#        export_setagent(df_employees, path, folder, agency)
#        export_setclassify(df_employees, path, folder, agency) 


def export_employees(df_employees, path, folder, agency=''): 
    create_folder(path, folder, agency)
    export_setagent( df_employees, path, folder, agency)
    export_setclassify( df_employees, path, folder, agency)


def export_routes_to_delete(df_routes_to_delete, path, folder, agency=''): 
    create_folder(path, folder, agency)
    export_DelRoute(df_routes_to_delete, path, folder, agency='')


