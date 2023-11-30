import pandas as pd
import time
from re import search
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime
import db_insertion_functions
import json
import os
from io import StringIO

def db_connection():
    cnx = psycopg2.connect(database="check",
                            host="localhost",
                            user="postgres",
                            password="myPassword",
                            port="5432")
    return cnx

def copy_from_stringio(conn, df, table):

    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False, sep=",")
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

def clean_the_files(df_base,df_matched,df_result):
    for index, row in df_base.iterrows():
        df_base.loc[index, 'Address']=str(row['Address']).replace(","," ")
        df_base.loc[index, 'Name']=str(row['Name']).replace(","," ")
    for index, row in df_matched.iterrows():
        df_matched.loc[index, 'Address']=str(row['Address']).replace(","," ")
        df_matched.loc[index, 'Name']=str(row['Name']).replace(","," ")
    for index, row in df_result.iterrows():
        df_result.loc[index, 'matching_type']=str(row['matching_type']).replace(",","")
        df_result.loc[index, 'matching_results']=str(row['matching_results']).replace(",","")
    
    return(df_base,df_matched,df_result)

def no_similar(row):
    address=str(row['base_address'])
    city_id=row['base_city_id']
    district_id=row['base_district_id']
    gps_longitude=row['base_longitude']
    if gps_longitude=='Undetected by Google Map API':
        gps_longitude=0
    gps_latitude=row['base_latitude']
    if gps_latitude=='Undetected by Google Map API':
        gps_latitude=0
    #district_id=db_insertion_functions.get_district_id(country_id,city_id,district_name)
    list_location=(address,country_id,city_id,district_id,gps_longitude,gps_latitude,current_dateTime,current_dateTime)
    ###### insertion in universal_account_location
    db_insertion_functions.universal_account_location_insertion(list_location)
    provided_address=provided_facility_image=provided_registarion_no=verified_facility_image=verified_phone=verified_email=has_valid_document=has_delivered_orders=has_registred_users=physically_visited=user_accessed_poc= False
    from_trusted=[]
    list_verification=(provided_address,provided_facility_image,provided_registarion_no,verified_facility_image,verified_phone,verified_email,has_valid_document,has_delivered_orders,has_registred_users,physically_visited,user_accessed_poc,from_trusted,current_dateTime,current_dateTime)
    ###### insertion in universal_account_verification
    db_insertion_functions.universal_account_verification_insertion(list_verification)
    universal_account_code=str(row['base_id'])
    universal_account_name=str(row['base_name'])
    universal_location_id=db_insertion_functions.get_location_id()
    universal_verification_id=db_insertion_functions.get_universal_account_verification_id()
    list_UA=(universal_account_code,universal_account_name,universal_location_id,universal_verification_id,current_dateTime,current_dateTime)
    ###### insertion in universal_accounts
    db_insertion_functions.universal_account_insertion(list_UA)

def matched(row,matched_data_source_id):
    ##### UA code is the matched item ####
    universal_account_code=str(row['matching_id'])
    print(universal_account_code)
    universal_account_id=db_insertion_functions.get_universal_account_id(universal_account_code)
    data_source_id=int(matched_data_source_id)
    ###### matched code is the base one ######
    matched_item_code=int(row['base_id'])
    liste_UACM=(universal_account_id,data_source_id,matched_item_code,current_dateTime,current_dateTime)
    ###### insertion in universal_account_code_mapping
    db_insertion_functions.universal_account_code_mapping_insertion(liste_UACM)


def data_importation(type_of_matching, country_id, base_file, matched_file, result_file):
    dfBase = pd.read_excel(base_file,'Sheet1')
    dfMatched = pd.read_excel(matched_file,'Sheet1') 
    dfResult = pd.read_excel(result_file,'Sheet1')

    dfBase,dfMatched,dfResult =clean_the_files(dfBase,dfMatched,dfResult)

    ####### in data-sources table insertion  #####
    print('in data_sources part')
    if data_source_to_insert == 'both':
        data_source_values=([(Path(base_file).stem,country_id,current_dateTime,current_dateTime,),
                        (Path(matched_file).stem,country_id,current_dateTime,current_dateTime,)])
        db_insertion_functions.both_data_source_insertion(data_source_values)
    if data_source_to_insert == 'base':
        data_source_values=(Path(base_file).stem,country_id,current_dateTime,current_dateTime,)
        db_insertion_functions.one_data_source_insertion(data_source_values)
    if data_source_to_insert == 'matched':
        data_source_values=(Path(matched_file).stem,country_id,current_dateTime,current_dateTime,)
        db_insertion_functions.one_data_source_insertion(data_source_values)

    ######## base items in data_source_items table insertion 
    base_data_source_id=db_insertion_functions.get_base_data_source_id(base_file)
    base_list=[]
    list_of_codes=[]
    dfBase = dfBase.fillna(np.nan).replace([np.nan], [None])
    print('in base_items_data part')
    id_data_source_items=db_insertion_functions.get_last_data_source_items_id() 
    for index, row in dfBase.iterrows():
        id_data_source_items=id_data_source_items+1
        code=str(row['ID'])
        name=str(row['Name']).replace("'","'"+"'")
        address=str(row['Address']).replace("'","'"+"'")
        city=str(row['City']).replace("'","'"+"'")
        district=str(row['District']).replace("'","'"+"'")
        if pd.notnull(dfBase.loc[index, 'Ward']):
            ward=str(row['Ward']).replace("'","'"+"'")
        else:
            ward=row['Ward']
        
        my_json=[]
        value=(id_data_source_items,base_data_source_id,code,name,address,city,district,ward,my_json,current_dateTime,current_dateTime)
        base_list.append(value)
        list_of_codes.append(value[2])
    
    
    df_of_base_list1 = pd.DataFrame(base_list, columns=('id_new','data_source_id','data_source_code','name','address','city','district','ward','other_information','created_at','updated_at'))
    df_of_base_list= df_of_base_list1.set_index("id_new")
    df_of_base_list.to_excel("resultat_a_voir.xlsx",index=False)
    table='data_source_items'
    copy_from_stringio(conn,df_of_base_list,table)   
   
###### matched items in data_source_items table insertion
    matched_data_source_id=db_insertion_functions.get_matched_data_source_id(matched_file)
    matched_data_source_id=int(matched_data_source_id)
    dfMatched = dfMatched.fillna(np.nan).replace([np.nan], [None])
    print('in matched_items part')
    matched_list=[]
    id_data_source_items=db_insertion_functions.get_last_data_source_items_id()  
    for index, row in dfMatched.iterrows():
        id_data_source_items=id_data_source_items+1
        code=str(row['ID'])
        name=str(row['Name']).replace("'","'"+"'")
        address=str(row['Address']).replace("'","'"+"'")
        city=str(row['City']).replace("'","'"+"'")
        district=str(row['District']).replace("'","'"+"'")
        if pd.notnull(dfMatched.loc[index, 'Ward']):
            ward=str(row['Ward']).replace("'","'"+"'")
        else:
            ward=row['Ward']
        my_json=[]
        value=(id_data_source_items,matched_data_source_id,code,name,address,city,district,ward,my_json,current_dateTime,current_dateTime)
        matched_list.append(value)
    df_of_matched_list1 = pd.DataFrame(matched_list, columns=('id_new','data_source_id','data_source_code','name','address','city','district','ward','other_information','created_at','updated_at'))
    df_of_matched_list= df_of_matched_list1.set_index("id_new")
    df_of_matched_list.to_excel("resultat_matched_a_voir.xlsx",index=False)
    table='data_source_items'
    copy_from_stringio(conn,df_of_matched_list,table)  

    ######### in matching_attempts table insertion
    type_id=db_insertion_functions.get_matching_type(type_of_matching)
    print('in matching_attempts part')
    try:
        number_of_items_in_base_list=int(len(dfBase.axes[0]))
    except:
        number_of_items_in_base_list=0
    try:
        number_of_items_in_matched_list=int(len(dfMatched.axes[0]))
    except:
        number_of_items_in_matched_list=0
    try:
        number_matches_found=int(dfResult['matching_results'].value_counts()['Surely Matched'])
    except:
        number_matches_found=0
    try:
        number_of_not_matches=int(dfResult['matching_results'].value_counts()['Surely No Similar Found'])
    except:
        number_of_not_matches=0
    try:
        number_of_manual_check_required=int(dfResult['matching_results'].value_counts()['Unsure but Likely Matched manual check required'] + dfResult['matching_results'].value_counts()['Unsure but Unlikely Matched manual check required'])
    except:
        number_of_manual_check_required=0
    number_of_wrong_matching_decisions=0
    matching=(type_id,base_data_source_id,matched_data_source_id,number_of_items_in_base_list,number_of_items_in_matched_list,number_matches_found,number_of_not_matches,number_of_manual_check_required,number_of_wrong_matching_decisions,current_dateTime,current_dateTime,)
    db_insertion_functions.matching_attempts_insertion(matching)
    
    matching_attempts_id=db_insertion_functions.get_matching_attempts_id(base_data_source_id,matched_data_source_id)

    ####### in base_items table insertion
    print('in base items table')
    data_values=[]
    last_base_id=db_insertion_functions.get_last_base_items_id()
    for element in list_of_codes:
        last_base_id=last_base_id+1
        list=(last_base_id,matching_attempts_id,element,current_dateTime,current_dateTime)
        data_values.append(list)
    df_of_base_items1 = pd.DataFrame(data_values, columns=('id_new','matching_attemps_id','base_item_code','created_at','updated_at'))
    df_of_base_items= df_of_base_items1.set_index("id_new")
    df_of_base_items.to_excel("resultat_base_items.xlsx",index=False)
    table='base_items'
    copy_from_stringio(conn,df_of_base_items,table) 

    ###### in possible_matched_items table insertion
    data_values_possible=[]
    last_possible_matched_id=db_insertion_functions.get_last_possible_matched_id()
    print('in possible_matched part')
    for index, row in dfResult.iterrows():
        last_possible_matched_id=last_possible_matched_id+1
        if pd.isnull(dfResult.loc[index, 'matching_id']):
            matched_item_code='Null'
            matching_method='Null'
            gps_distance=0
            similarity=0
            result_string=str(row['matching_results'])
            #print (result_string)
            if result_string=='Surely Matched':
                result=1
            elif result_string=='Surely No Similar Found':
                result=2
            elif result_string=='Unsure but Likely Matched manual check required' :
                result=3
            elif result_string=='Unsure but Unlikely Matched manual check required':
                result=4
                print ('result if', result)
            base_item_code=str(row['base_id'])
            data=(base_item_code, matching_attempts_id,)
            print('data', data)
            base_items_id=db_insertion_functions.get_base_items_id(data)
            matching_verification_result_id=None
            matching_verification_method=1
            list_possible=(last_possible_matched_id,matched_item_code,base_items_id,matching_method,gps_distance,similarity,result,matching_verification_method,matching_verification_result_id,current_dateTime,current_dateTime)
            data_values_possible.append(list_possible)     
        else:
            matched_item_code=row['matching_id']
            matched_item_code=str(matched_item_code)
            matching_method=row['matching_type']
            gps_distance=row['gps_distance']
            similarity=row['similarity']
            if pd.isnull(dfResult.loc[index, 'similarity']):
                similarity=0
            if pd.isnull(dfResult.loc[index, 'gps_distance']):
                gps_distance=0
            result_string=str(row['matching_results'])
            print(result_string)
            if result_string=='Surely Matched':
                result=1
            elif result_string=='Surely No Similar Found':
                result=2
            elif result_string=='Unsure but Likely Matched manual check required':
                result=3
            elif result_string=='Unsure but Unlikely Matched manual check required':
                result=4
            print ('result else', result)
            base_item_code=str(row['base_id'])
            data=(base_item_code,matching_attempts_id,)
            print('the data value is:', data)
            base_items_id=db_insertion_functions.get_base_items_id(data)
            matching_verification_method=1
            matching_verification_result_id=None
            list_possible=(last_possible_matched_id,matched_item_code,base_items_id,matching_method,gps_distance,similarity,result,matching_verification_method,matching_verification_result_id,current_dateTime,current_dateTime)
            data_values_possible.append(list_possible)
    df_of_possible_matched1 = pd.DataFrame(data_values_possible, columns=('id_new','base_items_id','matched_item_code','matching_methods','gps_distance','similarity','results','matching_verification_method','matching_verification_result_id','created_at','updated_at'))
    df_of_possible_matched= df_of_possible_matched1.set_index("id_new")
    df_of_possible_matched.to_excel("resultat_possible_matched_items.xlsx",index=False)
    table='possible_matched_items'
    copy_from_stringio(conn,df_of_possible_matched,table) 

    if type_of_matching=='monthly':

        for index,row in dfResult.iterrows():
            matching_result=str(row['matching_results'])
            if matching_result=='Surely No Similar Found':   
                no_similar(row)
            if matching_result=='Surely Matched':
                matched(row, matched_data_source_id)
            if matching_result=='Unsure but Unlikely Matched, manual check required' or matching_result=='Unsure but Likely Matched, manual check required':
                base_id = str(row['base_id'])
                matching_id=  str(row['matching_id'])
                manual_result = row['manual_check_results']
                if manual_result=='Surely Matched':
                    manual_result_int=1
                if manual_result=='Surely No Similar Found':
                    manual_result_int=2
                human_check_note = str(row['human_check_note'])
                human_check_by_name = str(row['human_check_by_name'])
                human_check_by_email = str(row['human_check_by_email'])
                list_to_insert=(manual_result_int,human_check_note,human_check_by_name,human_check_by_email,current_dateTime,current_dateTime)
                try:
                    db_insertion_functions.matching_verification_results_insertion(list_to_insert)
                    matching_verification_id=db_insertion_functions.get_matching_verification_results_id()
                    db_insertion_functions.update_possible_matched_items(matching_verification_id,base_id,matching_id)
                    print('it is ok !!!')
                except Exception as e:
                    print('Error:', e)
                if row['manual_check_results']=='Surely No Similar Found':
                    no_similar(row)
                if row['manual_check_results']=='Surely Matched':
                    matched(row, matched_data_source_id)





if __name__ == "__main__":

    current_dateTime = datetime.now()

    ##################################################
    ################# to modify ######################
    ################################################## 

    base_file="POC_test.xlsx"
    
    matched_file="UA1.3.xlsx"

    result_file="POC_Network_UA1.3.xlsx"

    country_id=1

    type_of_matching='ad_hoc' #type of matching

    conn=db_insertion_functions.db_connection()
    cursor=conn.cursor()

    # data_source_to_insert must be both, base or matched:
    # both if we need to insert both data sources in data_source table
    # base if we need to insert just the base file in data_source table (case when the matched file already exists)
    # matched if we need to insert just the matched file in data_source table (case when the base file already exists)
    data_source_to_insert='both'
    ##################################################


    start_time = time.time()
    data_importation(type_of_matching,country_id,base_file,matched_file,result_file)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print('Execution time:',execution_time)