import pandas as pd
import os.path 
import csv
pd.options.mode.chained_assignment = None

def choose_convertor():
    print("")
    show_flex_routes_memo()
    print("+-------------------------- КОНВЕРТEР МАРШРУТОВ В ФОРМАТ CDC --------------------------+")   
    print("|                                                                                      |")
    print("|  Пожалуйста, выберите файл EXCEL, который вы хотели бы сконвертировать?              |")
    print("|                                                                                      |")
    print("|  1. Файл с маршрутами и точками для __фиксированных__ маршрутов (цикл. визиты)       |")
    print("|  2. Файл с маршрутами и точками для __гибких__ маршрутов                             |")
    print("|                                                                                      |")
    print("+--------------------------------------------------------------------------------------+")
    print("")
    convertor_type = input ("Введите цифру для выбора файла:") 
    while convertor_type not in ('1', '2', '3'): 
        print ("Вы ввели неверную цифру. Попробуйте еще раз.")
        convertor_type = input ("Введите цифру для выбора файла:") 
    else:
        print ("Выбран пункт " + convertor_type + ".")
    return convertor_type


def input_path():
    print("")
    PATH = str(input("Введите путь к файлу Excel:"))
    PATH = PATH.replace('"', '').replace("'", "")
    PATH = os.path.abspath(PATH)
    if os.path.isfile(PATH):
        print ("Файл найден.")
    else:
        print ("Файл не найден.")
        PATH = "error"
    return PATH


def input_load_date():
    print("")
    DATE_OF_LOAD = str(input("Введите дату начала действия маршрутов (в формате 01.12.2023): "))
    print("")
    DATE_OF_LOAD = pd.to_datetime(DATE_OF_LOAD, format='%d.%m.%Y')
    return DATE_OF_LOAD

def print_processed_agency(agency_name):
    spaces = "==================================="##
    add_spaces = int(88 - 49 - len(agency_name) - 28)
    print("")
    print("")
    print("========================================================================================") 
    print("==================== ОБРАБОТКА ДАННЫХ АГЕНТСТВА: " + agency_name + " ===========================" + spaces[:add_spaces]) 
    print("========================================================================================") 
    print("")
    print("")


def show_flex_routes_memo():
    print("")
    print("")
    print("  НАПОМИНАНИЕ ОБ ОСОБЕННОСТЯХ ЗАГРУЗКИ МАРШРУТОВ В СИСТЕМУ:") 
    print("")
    print("  1. Excel  файл  может  содержать  НЕ  все  активные  гибкие  маршруты,  а только  те,") 
    print("     в  которых  произведены  изменения;")
    print("  2. Функционал    деактивации   неактуальных    маршрутов   будет   добавлен  позднее;")
    print("  3. Гибкие маршруты прогружаются только на 60 дней вперед. Не забывайте  их  обновлять")
    print("     не реже чем 1 раз в 60 дней;")
    print("  4. Перевод  маршрута   из   фиксированных   в   гибкие  производится   только   после")
    print("     подтверждения  от  менеджера  заказчика;")
    print("  5. Маршруты  на  завтрашний  день  появятся  на  портале  не  сразу,  а только после")  
    print("     ночной  обработки  сервером;")
    print("  6. Полученные *.txt файлы необходимо выложить на sftp сервер для загрузки в систему;")
    print("  7. Если после  обработки файлов  sftp-сервером появились файлы с ошибкам вида *.err,") 
    print("     то необходимо  еще  раз  проверить  правильность Excel  и  если ошибок не найдено,") 
    print("     сообщить менеджеру заказчика.")    
    print("")




def show_new_routes(df_show_new_routes):
    if len(df_show_new_routes) > 0:
        spaces = "                                                                                   "##        
        print("+--------------------------------------------------------------------------------------+")
        print("|  ВНИМАНИЕ! Не найдены данные маршруты и территории для них.                          |")
        print("|            Они будут созданы автоматически.                                          |")
        print("|                                                                                      |")
        for key, row in df_show_new_routes.iterrows():
            add_spaces = int(88 -19  - len(row['ROUTE_NAME']) - 2)
            if add_spaces < 0: add_spaces = 0
            print('|  Создан маршрут: ' + row['ROUTE_NAME'] + spaces[:add_spaces]+" |")
        print("|                                                                                      |")
        #print("| * Маршруты появятся на портале на завтрашний день, либо после синхронизации          |")
        print("+--------------------------------------------------------------------------------------+")


def show_routs_wo_employee(df_employees):
    if len(df_employees.query("EMPLOYEE_AND_VACANTS.str.startswith('Вакант') and (EMPLOYEE_ID == '0')", engine='python')) > 0:
        spaces = "                                                                          "##        
        print("+--------------------------------------------------------------------------------------+")
        print("|  ВНИМАНИЕ! Не найдена привязка сотрудников к некоторым маршрутам.                    |")
        print("|            Для них будут созданы следующие фиктивные пользователи (ваканты).         |")
        print("|                                                                                      |")
        df_print = df_employees.query("EMPLOYEE_AND_VACANTS.str.startswith('Вакант') and (EMPLOYEE_ID == '0')", engine='python')
        df_print = pd.DataFrame(df_print.groupby(by=['ROUTE_NAME', 'EMPLOYEE_AND_VACANTS']).agg('size').reset_index())
        for key, row in df_print.iterrows():
            add_spaces = int(88 - 18 - len(row['EMPLOYEE_AND_VACANTS'])- 17 - len(row['ROUTE_NAME']) - 2)
            if add_spaces < 0: add_spaces = 0
            print('|  Создан вакант: '  + row['EMPLOYEE_AND_VACANTS'] + '   для маршрута: ' + row['ROUTE_NAME'] + spaces[:add_spaces]+" |")
        print("|                                                                                      |")
        print("+--------------------------------------------------------------------------------------+")




#def show_routs_wo_employee(df_show_new_routes):
#    df_routes_export = pd.DataFrame(df_show_new_routes.groupby(by=['ROUTE_NAME', 'SHIP_TO', 'EMPLOYEE_ID', 'EMPLOYEE_AND_VACANTS']).agg('size')).reset_index()
#    if len(df_routes_export[df_routes_export['EMPLOYEE_ID'] == '0']) > 0:
#        spaces = "                                                                          "##        
#        print("+--------------------------------------------------------------------------------------+")
#        print("|  ВНИМАНИЕ! Не найдена привязка сотрудников к некоторым маршрутам.                    |")
#        print("|            Для них будут созданы следующие фиктивные пользователи (ваканты).         |")
#        print("|                                                                                      |")
#        filt = df_routes_export['EMPLOYEE_ID'] == '0'
#        df_print = pd.DataFrame(df_routes_export[filt].groupby(by=['ROUTE_NAME', 'EMPLOYEE_AND_VACANTS']).agg('size').reset_index())
#        for key, row in df_print.iterrows():
#            add_spaces = int(88 - 18 - len(row['EMPLOYEE_AND_VACANTS'])- 17 - len(row['ROUTE_NAME']) - 2)
#            if add_spaces < 0: add_spaces = 0
#            print('|  Создан вакант: '  + row['EMPLOYEE_AND_VACANTS'] + '   для маршрута: ' + row['ROUTE_NAME'] + spaces[:add_spaces]+" |")
#        print("|                                                                                      |")
#        print("+--------------------------------------------------------------------------------------+")

