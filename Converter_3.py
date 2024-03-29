import pandas as pd
import os.path 
import csv
pd.options.mode.chained_assignment = None
from interface import * 
from fix_routes_converter  import *
from flex_routes_converter import *
from tl_routes_converter import *
from HnN_routes_converter import *
from routes_export import *
from time import sleep
import warnings
warnings.filterwarnings("ignore")


def fix_rtm_convertor(): 
    PATH = input_path()
    if PATH != 'error':
        DATE_OF_LOAD = input_load_date()
        df_rtm = fix_import_rtm_file(PATH)
        df_rtm = fix_rename_columns(df_rtm)
        df_rtm = fix_unpivot_rtm(df_rtm)
        df_calender = fix_create_calender(DATE_OF_LOAD)
        df_routes = fix_converting_rtm_to_routes(df_rtm, df_calender)
        df_routes = add_empty_shipto(df_routes, DATE_OF_LOAD)
        df_routes = zeroing_empty_routes(df_routes)
        export_new_routes_files(df_routes, PATH, 'Фиксированные_маршруты', '', )
        DATE_OF_LOAD, PATH = '',''



def flex_rtm_convertor():
    PATH = input_path()
    if PATH != 'error':
        DATE_OF_LOAD = input_load_date()
        df_rtm_flex  = flex_import_rtm_file(PATH)        
        df_rtm_flex = flex_rename_columns(df_rtm_flex)   
        df_employees = import_emploee_data(PATH) 
        df_employees, df_show_new_routes = convert_emploee_data(df_employees, df_rtm_flex)   
        for agency_name in set_agency_list(df_rtm_flex):
            print_processed_agency(agency_name)            
            df_rtm_tmp = flex_filters_data(df_rtm_flex, agency_name)
            df_rtm_tmp = flex_unpivot_rtm(df_rtm_tmp)
            df_calender = flex_create_calender(DATE_OF_LOAD)
            df_routes_flex = flex_converting_rtm_to_routes(df_rtm_tmp, df_calender)
            df_routes_flex = add_empty_shipto(df_routes_flex, DATE_OF_LOAD)
            #df_routes_flex = zeroing_empty_routes(df_routes_flex) - Не использовать
            show_new_routes(df_show_new_routes)
            #show_routs_wo_employee(df_employees) 
            export_new_routes_files(df_routes_flex, PATH, 'Гибкие_маршруты',df_employees, agency_name)        
        DATE_OF_LOAD, PATH = '',''

def HnN_rtm_convertor():
    show_HnN_routes_memo()
    PATH = input_path()
    if PATH != 'error':
        DATE_OF_LOAD = input_load_date()
        df_rtm  = HnN_import_rtm_file(PATH)        
        df_rtm = HnN_rename_columns(df_rtm)  
        df_rtm = HnN_filtering_rtm(df_rtm)     
        df_rtm = check_wrong_order_type(df_rtm) 
        df_rtm = check_wrong_route_type(df_rtm)
        for agency_name in set_agency_list(df_rtm):
            print_processed_agency(agency_name)           
            df_rtm = HnN_unpivot_rtm(df_rtm)
            df_vacants = create_HnN_vacants(df_rtm)
            df_calender = HnN_create_calender(DATE_OF_LOAD)
            df_routes_HnN_fix = HnN_converting_FIX_rtm_to_routes(df_rtm, df_calender)
            df_routes_HnN_flex = HnN_converting_FLEX_rtm_to_routes(df_rtm)
            df_routes = HnN_union_FLEX_n_FIX_routes(df_routes_HnN_fix, df_routes_HnN_flex)
            df_routes = add_empty_shipto_HnN(df_routes, DATE_OF_LOAD)
            #df_routes = zeroing_empty_routes(df_routes)  #- Не использовать
            df_routes = route_type_substitution(df_routes)            
            export_new_routes_files(df_routes, PATH, 'Маршруты HnN','', agency_name) 
            export_setagent( df_vacants, PATH, 'Маршруты HnN', agency_name) 
            export_setclassify( df_vacants, PATH, 'Маршруты HnN', agency_name)     
        DATE_OF_LOAD, PATH = '',''



def tl_rtm_convertor():
    print_warning_teamlead()
    PATH = input_path()
    if PATH != 'error':
        DATE_OF_LOAD = input_load_date()    
        df_rtm = tl_import_rtm_file(PATH)     
        df_rtm = tl_rename_columns(df_rtm)   
        for agency_name in set_agency_list(df_rtm):
            print_processed_agency(agency_name)            
            df_rtm_tmp =  tl_filters_data(df_rtm, agency_name)
            df_routes_tmp = tl_converting_rtm_to_routes(df_rtm_tmp)
            df_routes_tmp = tl_add_empty_shipto(df_routes_tmp, DATE_OF_LOAD)
            # df_routes_tmp = zeroing_empty_routes(df_routes_tmp)  - Не использовать
            export_new_routes_files(df_routes_tmp, PATH, 'Маршруты тимлидов', df_employees='', agency=agency_name)        
        DATE_OF_LOAD, PATH = '',''        
        
def employee_convertor():
    PATH = input_path()
    if PATH != 'error':
        print_warning_employee()
        df_employees = import_emploee_data(PATH)
        export_employees(df_employees, PATH, 'Привязка сотрудников к маршрутам', agency='')

def delete_routes_convertor():
    PATH = input_path()
    if PATH != 'error':
        print_warning_del_routes()
        df_routes_to_delete =  pd.read_excel(PATH, sheet_name='routes_to_delete', usecols="A:B", dtype='str')
        export_routes_to_delete(df_routes_to_delete, PATH, 'Удаление маршрутов', agency='')        


convertor = choose_convertor()
if convertor == '1':
    fix_rtm_convertor()    
elif convertor == '2':
    flex_rtm_convertor()    
elif convertor == '3':
    employee_convertor() 
elif convertor == '4':
    delete_routes_convertor()
elif convertor == '5':
    tl_rtm_convertor()
elif convertor == '6':
    HnN_rtm_convertor()
else:
    print ("Error")

print('')
print("-----------------------------------------------------------------------------------------")
print("")
print("Конвертация завершена. Можете закрыть окно.")
sleep(120)



#КОНВЕРТАЦИЯ:

#Были проблемы с запаковкой
#Для того чтобы их решить сделал новое виртуальное окружение с Питоном 3.9 (Виртуальное окружение 3_10)
#Установил туда пандас и эти пакеты
#conda install  pillow
#pip install openpyxl==3.0.9    # Это очень важно, без этого выдаёт ошибку при исподьзовании
                                # Но в последнюю установку pandas потребовал версию 3.0.10. Я ее установил и все нормально запаковалось.
###pip install datetime
#conda install pandas
#conda install numpy
#conda install  pyinstaller

#cd "C:\_Row_data\data_cdc\_convertor\_converter_project\"
#pyinstaller -F --hidden-import openpyxl --distpath ./compile/dist   --workpath  ./compile/build  -i  "C:\_Row_data\data_cdc\_convertor\_converter_project\compile\icons8-export-64.png"  Converter_3.py


# Можно еще попробовать вот так
# pyinstaller YOUR_FILE.py --hidden-import openpyxl.cell._writer