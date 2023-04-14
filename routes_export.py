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
    '''Создаем новых вакантов через файл DMT_set_Agent.txt'''
    df_employees_export = pd.DataFrame(df_employees.groupby(by=['ROUTE_NAME', 'EMPLOYEE_ID', 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
    if len(df_employees_export[df_employees_export['EMPLOYEE_ID'] == '0']) > 0:
        df_empty_employees = df_employees_export[df_employees_export['EMPLOYEE_ID'] == '0']
        df_empty_employees = pd.DataFrame(df_empty_employees.groupby(by=[ 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
        df_empty_employees[1] = df_empty_employees['EMPLOYEE_AND_VACANTS']
        df_empty_employees[3] = df_empty_employees['EMPLOYEE_AND_VACANTS']
        df_empty_employees[4] = df_empty_employees['EMPLOYEE_AND_VACANTS']
        df_empty_employees[2] = '1'
        df_empty_employees[[5,6,7,8]] = np.NaN
        df_empty_employees[9] = '5210004'
        df_empty_employees = df_empty_employees[[1,2,3,4,5,6,7,8,9]]
        if agency != '':     agency = "\\" + str(agency)
        export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_set_Agent.txt"
        df_empty_employees.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
        print('')
        print('Данные сохранены в файл: ' + str(export_path))


def export_setclassify( df_employees, path, folder, agency=''):
    '''Связка кода сотрудника Вакант с кодом маршрута через файл DMT_Set_ClassifyEx.txt'''
    df_employees_export = pd.DataFrame(df_employees.groupby(by=['EXT_ROUTE_ID', 'EMPLOYEE_ID', 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
    if len(df_employees_export[df_employees_export['EMPLOYEE_ID'] == '0']) > 0:
        df_empty_employees = pd.DataFrame(df_employees_export[df_employees_export['EMPLOYEE_ID'] == '0'])
        df_empty_employees[1] = '16'
        df_empty_employees[2] = '0:0:"' + df_empty_employees['EXT_ROUTE_ID'] +'";1:1:"' + df_empty_employees['EMPLOYEE_AND_VACANTS'] + '"'
        df_empty_employees[3] = '1#1'
        df_empty_employees = df_empty_employees[[1,2,3]]
        if agency != '':     agency = "\\" + str(agency)
        export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_Set_ClassifyEx.txt"
        df_empty_employees.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False, quoting=csv.QUOTE_NONE)
        print('')
        print('Данные сохранены в файл: ' + str(export_path))



def export_setmobfaceadd(df_routes, path, folder, agency=''):
    '''DMT_Set_MobFaces_Add.txt с набором сотрудник-точка. Тут только 2 поля именно в этом порядке'''
    df_routes_export = df_routes
    filt = df_routes_export['EMPLOYEE_AND_VACANTS'] != '0'
    df_routes_export = df_routes_export[filt]
    df_routes_export = pd.DataFrame(df_routes_export.groupby(by=[ 'SHIP_TO', 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
    df_routes_export[1] = df_routes_export['EMPLOYEE_AND_VACANTS']
    df_routes_export[2] = df_routes_export['SHIP_TO']
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
    df_routes_export[6] = df_routes_export['SHIP_TO']
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
    export_path = os.path.dirname(path) + "\ROUTES_Data" + ".txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8')
    print('')
    print('Данные сохранены в файл: ' + str(export_path))



def export_setclientservice(df_routes, path, folder, agency=''):
    '''Экспорт единого файла фиксированных и гибких маршрутов в новом формате (ПОСЛЕ доработки по историчности) через файл DMT_User273_SetClientServiceMatrix_Batch.txt'''
    renames = { 'AGENCY_NAME'        : 'OwnerExid',                 # --- Принадлежность записи к агентству
                'ROUTE_NAME'         : 'TerrName',                  # --- Наименование маршрута"
                'EXT_ROUTE_ID'       : 'TerrExid',                  # --- Короткий код маршрута (последние 4 цифры в названии)
                'ROUTE_TYPE'         : 'TerrRole',                  # --- Роль сотрудника (мерчендайзер, Тим Лид)
                'SHIP_TO'            : 'FaceExid',                  # --- Код торговой точки
                'FIRST_VISIT_DATE'   : 'StartDate',                 # --- Дата начала посещений, не раньше чем завтра
                'REPEAT_DAYS'        : 'Period',                    # --- Цикличность (0,1,4,7,14,21,28)
                'VISIT_NUMBER'       : 'SortID',                    # --- порядок в дне на маршруте
                'VISIT_DURATION'     : 'Duration',                  # --- длительность посещения в минутах
                'PHOTOS_TARGET'      : 'PlanFotoCountPerAudit',     # --- Плановое количество фотографий на один фотоаудит
                'DOCUMENTS_TARGET'   : 'PlanDocCountPerAudit',      # --- Плановое количество документов на один фотоаудит
                'PHOTOAUDIT_TARGET'  : 'PlanAuditCountPerVisit',    # --- Плановое количество фотоаудитов на один визит
                'END_DATE'           : 'EndDate'}                   # --- Дата окончания цикла планового посещения
    df_routes_export = df_routes[['AGENCY_NAME','ROUTE_NAME','EXT_ROUTE_ID','ROUTE_TYPE','SHIP_TO','FIRST_VISIT_DATE','REPEAT_DAYS','VISIT_NUMBER','VISIT_DURATION', 'PHOTOS_TARGET','DOCUMENTS_TARGET','PHOTOAUDIT_TARGET', 'END_DATE']].rename(columns=renames)  
    if agency != '':     agency = "\\" + str(agency)
    export_path = os.path.dirname(path) +  "\\" + str(folder) + agency + "\DMT_User273_SetClientServiceMatrix_Batch.txt"
    df_routes_export.to_csv(export_path, sep='|', index=False, encoding='utf-8', header=False)
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
    if len(df_employees) > 0:
        export_setagent(df_employees, path, folder, agency)
        export_setclassify(df_employees, path, folder, agency) 


