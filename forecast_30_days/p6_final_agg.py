#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 15:25:03 2018

@author: roopeshp
"""

#--------- p6 - Number of targets(users) per location ---------------------------#

import pandas as pd
import sys
import config
import resource
import psutil

import pymysql
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
import logging
from joblib import Parallel, delayed

from pyspark.sql import SQLContext

from tqdm import tqdm
from fbprophet import Prophet
from sklearn.metrics import mean_squared_error as mse
import math

# flag to confirm the writting of forecasted value to db
real_flag = config.real_flag
total_t1 = datetime.now()
## Logging ##
import os
import sys
global cpu_perc_list

pid = os.getpid()
py = psutil.Process(pid)
cpu_perc_list = list()


def connect_to_mysql():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection



from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from pyspark.sql import SparkSession

full_t1 = datetime.now()


## Logging ##

newpath = config.proj_path+'/log_for_demo' 
if not os.path.exists(newpath):
    os.makedirs(newpath)
newpath = config.proj_path+'/log_for_demo/p6' 
if not os.path.exists(newpath):
    os.makedirs(newpath)



def create_prophet_m(app_name,z1,cpu_perc_list,delay=24):
    
    ### --- For realtime pred ---###
    
    full_df = z1.user_count.iloc[0:len(z1)]
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    #removing outliers
    q50 = full_df.y.median()
    q100 = full_df.y.quantile(1)
    q75  = full_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        full_df.loc[full_df.y>=(2*q75),'y'] = None
    
    
    #-- Realtime prediction --##
    #model 
    model_r = Prophet(yearly_seasonality=False,changepoint_prior_scale=.03,seasonality_prior_scale=0.2)
    model_r.fit(full_df)
    future_r = model_r.make_future_dataframe(periods=delay,freq='D')
    forecast_r = model_r.predict(future_r)
    forecast_r.index = forecast_r['ds']
    #forecast 
    pred_r = pd.DataFrame(forecast_r['yhat'][len(z1):(len(z1)+delay)])
    pred_r=pred_r.reset_index()
    #--- completes realtime pred ---#
    
    train_end_index=len(z1.user_count)-delay
    train_df=z1.user_count.iloc[0:train_end_index]
    
    test_df=z1.user_count.iloc[train_end_index:len(z1)]
    
    train_df=train_df.reset_index()
    test_df=test_df.reset_index()
    train_df.columns=['ds','y']
    
    #--- removing outliers in trainset  ---#
    #--- removing outliers in trainset  ---#
    
    q50 = train_df.y.median()
    q100 = train_df.y.quantile(1)
    q75  = train_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        train_df.loc[train_df.y>=(2*q75),'y'] = None
    
    
    test_df.columns=['ds','y']
    test_df['ds'] = pd.to_datetime(test_df['ds'])
    
    #model 
    model = Prophet(yearly_seasonality=False,changepoint_prior_scale=.03,seasonality_prior_scale=0.2)
    model.fit(train_df)
    
    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]

    future = model.make_future_dataframe(periods=len(test_df),freq='D')
    forecast = model.predict(future)
    forecast.index = forecast['ds']
    #forecast 
    pred = pd.DataFrame(forecast['yhat'][train_end_index:len(z1)])
    pred=pred.reset_index()
    pred_df=pd.merge(test_df,pred,on='ds',how='left')
    pred_df.dropna(inplace=True)
    
    df=pd.DataFrame()

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]


    
    if(len(pred_df)>0):
        
        pred_df['error_test']=pred_df.y-pred_df.yhat
    
        
    
        MSE=mse(pred_df.y,pred_df.yhat)
        RMSE=math.sqrt(MSE)
        pred_df['APE']=abs(pred_df.error_test*100/pred_df.y)
        MAPE=pred_df.APE.mean()
        min_error_rate = pred_df['APE'].quantile(0)/100
        max_error_rate = pred_df['APE'].quantile(1)/100
        median_error_rate = pred_df['APE'].quantile(.50)/100
        print("App name:",app_name)
        #print("MSE  :",MSE)
        print("RMSE :",RMSE)
        print("MAPE :",MAPE)
        
       
        mape_q98=pred_df['APE'][pred_df.APE<pred_df['APE'].quantile(0.98)].mean()
        std_MAPE = math.sqrt(((pred_df.APE-MAPE)**2).mean())

        df = pd.DataFrame({'length':len(z1),
                             'test_rmse':RMSE,
                             'test_mape':MAPE,
                             'std_mape':std_MAPE, #standerd deviation of mape
                             'min_error_rate':min_error_rate ,
                             'max_error_rate':max_error_rate ,
                             'median_error_rate':median_error_rate,
                 
                 'test_mape_98':mape_q98},
                   
                          index=[app_name])

    return(df,model,forecast,pred_df,pred_r)


#-- Function to select a combination for the run

def forcomb(s,df,ftime1,cpu_perc_list):
    
    df2 = df[ (df.location == s)]
   
    prophet_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()

    df2['date'] = df2.index.date
    
    df2 = pd.DataFrame(df2.groupby(by='date').user_count.max())
    
    df2 = df2.reset_index()
    df2 = df2.sort_values(by='date',ascending=True)
    df2.index = df2['date']
    del df2['date']
    
    df2['location'] = s

    print('length of data = ',len(df2))
   
    if(len(df2)>config.limit):
             
        prophet_analysis_df,ew_model,ew_forcast,prophet_df,prophet_future_df =(create_prophet_m(s,df2,cpu_perc_list,config.delay))

        
        cpu_perc_list.append(py.cpu_percent())
        cpu_perc_list = [max(cpu_perc_list)]

        
        t2 = datetime.now()
        prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)
        
        prophet_analysis_df['location'] = s
        
        prophet_future_df['location'] = s

        prophet_df['location'] = s

    df2 = df2.reset_index()
    return prophet_df, prophet_analysis_df, prophet_future_df , df2

## to create required combinations as per the report
def comb_creation(apps,cpu_perc_list):
    from pyspark.sql.functions import when

    q1 = datetime.now()

    df_t = df.registerTempTable('dummy')
    df_t = sqlContext.sql('select count(distinct target_address) as user_count, time_stamp, location   from dummy group by location, time_stamp')

    #df_needed = df[df.application.isin(apps)]
    df_t = df_t.registerTempTable('dummy')
    df_t = sqlContext.sql('select count(*) as count, location   from dummy group by location')

    df_t= df_t.withColumn('count_flag', when(df_t['count']>config.limit,1).otherwise(0))
    df_t = df_t[df_t.count_flag==1]
    
    # fetching the  location which is to be filtered from filter_db table
    with conn.cursor() as cursor:
        # Read a  record
        sql = "select * from filter_db" 
        cursor.execute(sql)
        so_result = pd.DataFrame(cursor.fetchall())
    
    #filtering
    from pyspark.sql.functions import col
    #print(so_result)
    #s_filter = list(so_result.location)
    #df_t = df_t.filter(~col('location').isin(s_filter))
    #df_t = df_t[df_t.location!='134.141.5.104']
    df2 = df_t.toPandas()

    q2 = datetime.now()

    print('time to refernce data prepration is ',str(q2-q1))
    print('length of table is ',len(df2))
    
    return df2


#def load_files(this_day):
def load_files(cpu_perc_list):

    global df
    global apps
    global conn
    global spark1
    global sqlContext 
    global sc
    
    conn=connect_to_mysql()


    # initialise sparkContext
    spark1 = SparkSession.builder \
        .master(config.sp_master) \
        .appName(config.sp_appname) \
        .config('spark.executor.memory', config.sp_memory) \
        .config("spark.cores.max", config.sp_cores) \
        .getOrCreate()
    
    sc = spark1.sparkContext

    # using SQLContext to read parquet file
    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)

    # Reading data from parquet
    print('satrt quering')

    df1 = sqlContext.read.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1')
    df2 =  sqlContext.read.parquet(config.proj_path+'/datas_new/appid_attribute_parquet')
    df2 = df2[['attribute_id','source','target_address','location']]

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]



    ### connection to partitiion table to get required apps
    #def connect_to_mysql2():
    #    connection = pymysql.connect(host = config.db_host,
    #                            port= config.db_port,
    #                            user= config.db_user,
    #                            password= config.db_pass,
    #                            db= config.db_name2,
    #                            charset='utf8',
    #                            cursorclass=pymysql.cursors.DictCursor)
    #    return connection

    #conn2=connect_to_mysql2()

    ## fetching tracked apps from db
    #with conn2.cursor() as cursor:
    #    # Read a  record
    #    sql = "select * from nsoptions where optionkey like '%tracked%'" 
    #    cursor.execute(sql)
    #    tracked = pd.DataFrame(cursor.fetchall())

    #t  = list(tracked[tracked.OPTIONKEY=='AppIdSystemOptions.trackedApps'].OPTIONVALUE)
    #apps = t.split(',')
 
    apps = config.apps
    df1 = df1[df1.application.isin(apps)]
    
    #renaming the column
    from pyspark.sql.functions import col

    df2 = df2.select(col("attribute_id").alias("target_attribute_id"),
                       col("source").alias("source_y"),
                           col("target_address").alias("target_address_y"),
                       col("location").alias("location"), 
                  )
    # merging the dfs

    df = df1.join(df2,how='left',on='target_attribute_id')

    ## For validation
    #tt = df.registerTempTable('dummy')
    #df = sqlContext.sql('select * from dummy where time_stamp<'+str(int(datetime.timestamp(this_day))*1000))
        
    



if __name__ == '__main__':
#def main(day):

    
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(filename=config.proj_path+'/log_for_demo/p6/p6.log',level=logging.INFO)

    this_day = datetime.now().date()
    #this_day = day.date()
    
    #load_files(day)
    load_files(cpu_perc_list)

    t1 = datetime.now()
    
    ap_list = config.apps

    rdf = comb_creation(apps,cpu_perc_list)
    
    pool = Parallel(n_jobs=2,verbose=5,pre_dispatch='all')

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]



    # Needed data extraction

    prophet_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    user_count_full_df = pd.DataFrame()
    #ap_list = ['Skype for Business','Outlook']

    t2 = datetime.now()
    time_to_fetch = str(t2-t1)

    qt1 = datetime.now()

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]



    df_t = df.registerTempTable('dummy')
    df_t = sqlContext.sql('select count(distinct target_address) as user_count, time_stamp, location from dummy group by location, time_stamp')
       
    # data cleaning
    user_count_df=df_t.toPandas()

    s_array = user_count_df.location.unique()
    #s_array = rdf.location.unique()

    user_count_df = user_count_df.sort_values(by='user_count',ascending=True)       
    dates_outlook = pd.to_datetime(pd.Series(user_count_df.time_stamp),unit='ms')
    user_count_df.index = dates_outlook   
    user_count_df = user_count_df.sort_values(by='time_stamp')
    #user_count_df.to_csv('p3/user_count_per_app_per_location_dataset.csv',index=False)
    print('quering is successfull')

    qt2 = datetime.now()
    query_time = str(qt2-qt1)

    # Running for all combiantions

    qt3 = datetime.now()

    r0  = pool(delayed(forcomb)(s,user_count_df,qt1,cpu_perc_list) for s in s_array) 

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]

    for i in range(0,len(r0)):
        prophet_df = prophet_df.append(r0[i][0])
        prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
        prophet_future_df = prophet_future_df.append(r0[i][2])
        user_count_full_df = user_count_full_df.append(r0[i][3])
  
    qt4 = datetime.now()
    model_time  = str(qt4-qt3)
  
   
    print(' -I- dataframe cteated ')
    logging.info(datetime.now())
    logging.info('-I- Model ran succesdfully...')


    # Writing the forecasted data to to mysql_db

    from sqlalchemy import create_engine
    engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))
    #res22.to_sql(con=engine, name='dummy', if_exists='replace')
    
    if(real_flag==1):
        #prophet_future_df.to_sql(con=engine, name='forecast_p3_beta', if_exists='replace', index=False )
        prophet_df.to_sql(con=engine, name='valid_nofusers_per_location', if_exists='replace', index=False )
        user_count_full_df.to_sql(con=engine, name='data_nofusers_per_location', if_exists='replace', index=False )

    ## checking whether forecast and anlyse table avilable or not
    with conn.cursor() as cursor:
        stmt = "SHOW TABLES LIKE 'forecast_nofusers_per_location'"
        cursor.execute(stmt)
        result = cursor.fetchone()
    if result:
        table_present_forecast_flag =1
    else:
        table_present_forecast_flag =0

    with conn.cursor() as cursor:
        stmt = "SHOW TABLES LIKE 'analyse_nofusers_per_location'"
        cursor.execute(stmt)
        result = cursor.fetchone()
    if result:
        table_present_analyse_flag =1
    else:
        table_present_analyse_flag =0

    if(table_present_forecast_flag ==1):
        ## to create new partition the inputed table -- for forecast table
        with conn.cursor() as cursor:
            # Read a  record
            sql = "alter table forecast_nofusers_per_location add partition (partition new values in ('"+str(this_day)+"'))" 
            cursor.execute(sql)
     
    

    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]

   

    prophet_future_df['run_date'] = this_day
    if((real_flag ==1) & (config.override_flag_forecast==1)):
        prophet_future_df.to_sql(con=engine, name='forecast_nofusers_per_location', if_exists='replace', index=False )
        table_present_forecast_flag = 0
    elif((real_flag ==1) & (config.override_flag_forecast==0)):
        prophet_future_df.to_sql(con=engine, name='forecast_nofusers_per_location', if_exists='append', index=False )

    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)

    #for analysis of our model in future

    prophet_analysis_df['run_date'] = this_day
    #prophet_analysis_df['total_run_time'] = total_real
    prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))

    
    if(table_present_analyse_flag == 1):
        ## to create new partition the inputed table -- for analysis table
        # renaming latest to old
        ## to make new as 'latest' partition the inputed table 
        with conn.cursor() as cursor:
            # Read a  record
            sql = " alter table analyse_nofusers_per_location remove partitioning" 
            cursor.execute(sql)




    if(config.override_flag_analysis == 1):
        prophet_analysis_df.to_sql(con=engine, name='analyse_nofusers_per_location', if_exists='replace',index=False)
        table_present_analyse_flag = 0
    else:
        prophet_analysis_df.to_sql(con=engine, name='analyse_nofusers_per_location', if_exists='append',index=False)

    ### if partitioned table already present or not
    if(table_present_analyse_flag == 1):
        ## to create new partition the inputed table -- for analysis table
    
        # selecting unique run_dates from analysis table
        with conn.cursor() as cursor:
            # Read a  record
            sql = "select distinct run_date as run_date from analyse_nofusers_per_location " 
            cursor.execute(sql)
            d_dates = pd.DataFrame(cursor.fetchall())

        date_t = list(d_dates['run_date'])
        dt_str = " "
        for z in range(0,len(date_t)):
            dt = date_t[z]
            if(z!= (len(date_t)-1)):
                dt_str = dt_str+"'"+str(dt)+"', "
            else:
                dt_str = dt_str+"'"+str(dt)+"' "
    
        # adding the partition
        with conn.cursor() as cursor:
            # Read a  record
            sql = "ALTER TABLE analyse_nofusers_per_location PARTITION BY LIST COLUMNS (run_date) ( PARTITION latest VALUES IN ("+dt_str+") );" 
            cursor.execute(sql)

    else:
        # adding the new partition to table
        with conn.cursor() as cursor:
            # Read a  record
            sql = "ALTER TABLE analyse_nofusers_per_location PARTITION BY LIST COLUMNS (run_date) ( PARTITION latest VALUES IN ('"+str(this_day)+"') );" 
            cursor.execute(sql)

    ## if partitioned table already present or not
    if(table_present_forecast_flag ==1):
        

        ## to delete 'latest' data  
        with conn.cursor() as cursor:
            # Read a  record
            sql = "alter table forecast_nofusers_per_location truncate partition latest" 
            cursor.execute(sql)
    

        ## to drop 'latest' partition  
        with conn.cursor() as cursor:
            # Read a  record
            sql = "alter table forecast_nofusers_per_location drop partition latest" 
            cursor.execute(sql)

        ## to make new as 'latest' partition the inputed table 
        with conn.cursor() as cursor:
            # Read a  record
            sql = "ALTER TABLE forecast_nofusers_per_location REORGANIZE PARTITION new INTO ( PARTITION latest VALUES IN ('"+str(this_day)+"') );" 
            cursor.execute(sql)
    else:
        # adding the partition
        with conn.cursor() as cursor:
            # Read a  record
            sql = "ALTER TABLE forecast_nofusers_per_location PARTITION BY LIST COLUMNS (run_date) ( PARTITION latest VALUES IN ('"+str(this_day)+"') );" 
            cursor.execute(sql)

    print(total_time)

    


    run_time_table = pd.DataFrame({'report':'p6(nofusers_per_location)','run_time':total_real,'run_date':this_day},index=[3])
    run_time_table.to_sql(con=engine, name='run_time_table_agg', if_exists='append',index=False)

    ## Logging
    
    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]
    print('-I- The maximum percentage of cpu used by this process is ',max(cpu_perc_list))

    logging.info('-I- The maximum percentage of cpu used by this process is ')
    logging.info(max(cpu_perc_list))

    mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024**2)
    logging.info('-I- The total amount of memory used by this process is ')
    logging.info(mem_usage)
    print('-I- The total amount of memory used by this process is ',mem_usage)

    logging.info(datetime.now())
    logging.info('-I- validation of model...')
    logging.info(prophet_analysis_df)

    logging.info('-I- Run time for fetching the data from parquet file is')
    logging.info(query_time)
    logging.info('-I- Run time for modelling is ')
    logging.info(model_time)
    logging.info('-I- The total run time  is ')
    logging.info(total_time)
    print ('Total run time  is ', total_time)
    print ('Total nof combinations = ', len(rdf))
    
    conn.close()
    spark1.stop()
    #conn2.close()