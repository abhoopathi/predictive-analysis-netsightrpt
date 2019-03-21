#--------- p1 - Number of targets(users) per application/source ---------------------------#

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
newpath = config.proj_path+'/log_for_demo/p1' 
if not os.path.exists(newpath):
    os.makedirs(newpath)



def create_prophet_m(app_name,z1,delay=30):
    
    ### --- For realtime pred ---###
    
    full_df = z1.app_rsp_time.iloc[0:len(z1)]
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    
    #removing outliers
    q50 = full_df.y.median()
    q100 = full_df.y.quantile(1)
    q75  = full_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        full_df.loc[full_df.y>=(2*q75),'y'] = 2*q75
    
    if(len(full_df.dropna())>=10):
        
        #-- Realtime prediction --##
        #model 
        model_r = Prophet(yearly_seasonality=False,changepoint_prior_scale=.1,seasonality_prior_scale=0.05,holidays=None)
        model_r.fit(full_df)
#        future_r = model_r.make_future_dataframe(periods=delay,freq='D')
#        forecast_r = model_r.predict(future_r)
#        forecast_r.index = forecast_r['ds']
#        #forecast 
#        pred_r = pd.DataFrame(forecast_r['yhat'][len(z1):(len(z1)+delay)])
#        pred_r=pred_r.reset_index()
        #--- completes realtime pred ---#


    return(model_r)


#-- Function to select a combination for the run

def forcomb(s,a,df,ftime1):
    
    df2 = df[ (df.source == s)]
#    prophet_df = pd.DataFrame()
#    prophet_analysis_df = pd.DataFrame()
#    prophet_future_df = pd.DataFrame()

    df2['date'] = df2.index.date
    
    df2 = pd.DataFrame(df2.groupby(by='date').app_rsp_time.max())
    
    df2 = df2.reset_index()
    df2 = df2.sort_values(by='date',ascending=True)
    df2.index = df2['date']
    del df2['date']
    df2['application'] = df.application[0]
    df2['source'] = s

    print('length of data = ',len(df2))
   
    if(len(df2)>config.limit):
             
        model_r =(create_prophet_m(a,df2,config.delay))

        
        t2 = datetime.now()
        

    df2 = df2.reset_index()
    return model_r




#def load_files(this_day):
def load_files():

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

    df = sqlContext.read.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1')

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
    df = df[df.application.isin(apps)]

    ## For validation
    #tt = df.registerTempTable('dummy')
    #df = sqlContext.sql('select * from dummy where time_stamp<'+str(int(datetime.timestamp(this_day))*1000))
        
    



#if __name__ == '__main__':
#def main(day):
def main():

    
    this_day = datetime.now().date()
    #this_day = day.date()
    
    #load_files(day)
    

    load_files()

    t1 = datetime.now()
    
    ap_list = list(config.apps)
    ap_list = [ap_list[0]]
    
    #pool = Parallel(n_jobs=2,verbose=5,pre_dispatch='all')

    # Needed data extraction

    #ap_list = ['Skype for Business','Outlook']

    t2 = datetime.now()
    time_to_fetch = str(t2-t1)

    for a in tqdm(ap_list):
    #for k in tqdm(range(0,1)):
        #a = ap_list[k]

        qt1 = datetime.now()
    
        data = df[(df.application == a )]


        df_t = data.registerTempTable('dummy')
        df_t = sqlContext.sql('select avg(app_rsp_time) as app_rsp_time, time_stamp, source , application  from dummy group by source, application, time_stamp')
       
        # data cleaning
        df_t = df_t[df_t.app_rsp_time!=0]
        app_rsp_time_df=df_t.toPandas()
    

        s_array = list(app_rsp_time_df.source.unique())
        #print(app_rsp_time_df.head())
        #print(s_array)
        #print(ap_list)
        s = s_array[0]
        #s_array = rdf.source.unique()

        app_rsp_time_df = app_rsp_time_df.sort_values(by='app_rsp_time',ascending=True)       
        dates_outlook = pd.to_datetime(pd.Series(app_rsp_time_df.time_stamp),unit='ms')
        app_rsp_time_df.index = dates_outlook   
        app_rsp_time_df = app_rsp_time_df.sort_values(by='time_stamp')
        #app_rsp_time_df.to_csv('p1/app_rsp_time_per_app_per_source_dataset.csv',index=False)
        print('quering is successfull')

        qt2 = datetime.now()
        query_time = str(qt2-qt1)

 
        # Running for all combiantions

        qt3 = datetime.now()


        model_r = forcomb(s,a,app_rsp_time_df,qt1)
        #r0  = pool(delayed(forcomb)(s,a,app_rsp_time_df,qt1) for s in s_array) 


#        for i in range(0,len(r0)):
#            
#            prophet_df = prophet_df.append(r0[i][0])
#            prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
#            prophet_future_df = prophet_future_df.append(r0[i][2])
#            app_rsp_time_full_df = app_rsp_time_full_df.append(r0[i][3])
  
        qt4 = datetime.now()
        model_time  = str(qt4-qt3)
  

    
    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)

    #for analysis of our model in future

    #prophet_analysis_df['run_date'] = this_day
    #prophet_analysis_df['total_run_time'] = total_real
    #prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))

    conn.close()
    spark1.stop()
    #conn2.close()
    
    return model_r


