import os
import sys

import config
from datetime import datetime
import pymysql
import pandas as pd

## connection to partitiion table to get required apps
def connect_to_mysql2():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name2,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection

import schedule
import time

def job():
    t1 = datetime.now()
    #os.chdir(config.proj_path)

    print(os.getcwd())
    conn2=connect_to_mysql2()

    ## fetching beta_feature value from db to check it is true or false
    with conn2.cursor() as cursor:
        # Read a  record
        sql = "select * from netsight.nsoptions where optionkey like '%BetaOptions.betaFeatures%'" 
        cursor.execute(sql)
        tr_temp = cursor.fetchall()
        if(tr_temp):
            tracked = pd.DataFrame(tr_temp)
        else:
            tracked = pd.DataFrame()
    
    if(len(tracked)>0):
        #runs only if beta feature in true
        if((tracked['OPTIONVALUE'][0]=='true') | (tracked['OPTIONVALUE'][0]=='True') | (tracked['OPTIONVALUE'][0]=='TRUE') ):
            os.system("python "+config.proj_path+"/parquet_file_creation_agg.py")
            os.system("python "+config.proj_path+"/p1_final_agg.py")
            os.system("python "+config.proj_path+"/p2_final_agg.py")
            os.system("python "+config.proj_path+"/p3_final_agg.py")
            os.system("python "+config.proj_path+"/p6_final_agg.py")
            os.system("python "+config.proj_path+"/p7_final_agg.py")
            os.system("python "+config.proj_path+"/p8_final_agg.py")
            os.system("python "+config.proj_path+"/p9_final_agg.py")
            os.system("python "+config.proj_path+"/p10_final_agg.py")
            
            ## For 48 hrs prediction
            os.system("python "+config.proj_path+"/p1_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p2_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p3_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p6_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p7_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p8_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p9_agg_48hrs.py")
            os.system("python "+config.proj_path+"/p10_agg_48hrs.py")
        


    t2 = datetime.now()

    print('Total time to run all scripts : ',str(t2-t1))

## scheduling the task every night 12am
schedule.every().day.at("00:00").do(job)


while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute


