
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
newpath = config.proj_path+'/log_for_demo/p1' 
if not os.path.exists(newpath):
    os.makedirs(newpath)



def create_prophet_m(app_name,z1,cpu_perc_list,delay=24):
    
    ### --- For realtime pred ---###
    
    full_df = z1.app_rsp_time.iloc[0:len(z1)]
    full_df = full_df.reset_index()
    full_df.columns = ['ds','y']
    
    #removing outliers
    q50 = full_df.y.median()
    q100 = full_df.y.quantile(1)
    q75  = full_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        full_df.loc[full_df.y>=(2*q75),'y'] = None
    
    if(len(full_df.dropna())>=10):
        
        #-- Realtime prediction --##
        #model 
        model_r = Prophet(yearly_seasonality=False,changepoint_prior_scale=.1,seasonality_prior_scale=0.05)
        model_r.fit(full_df)
        future_r = model_r.make_future_dataframe(periods=delay,freq='D')
        forecast_r = model_r.predict(future_r)
        forecast_r.index = forecast_r['ds']
        #forecast 
        pred_r = pd.DataFrame(forecast_r['yhat'][len(z1):(len(z1)+delay)])
        pred_r=pred_r.reset_index()
        #--- completes realtime pred ---#

    #----- validation ----#    
    train_end_index=len(z1.app_rsp_time)-delay
    train_df=z1.app_rsp_time.iloc[0:train_end_index]
    
    test_df=z1.app_rsp_time.iloc[train_end_index:len(z1)]
    
    train_df=train_df.reset_index()
    test_df=test_df.reset_index()
    train_df.columns=['ds','y']
    
    #--- removing outliers in trainset  ---#
    
    q50 = train_df.y.median()
    q100 = train_df.y.quantile(1)
    q75  = train_df.y.quantile(.75)
    
    if((q100-q50) >= (2*q75)):
        
        train_df.loc[train_df.y>=(2*q75),'y'] = None
        
    if(len(train_df.dropna())>=10):
    
        test_df.columns=['ds','y']
        test_df['ds'] = pd.to_datetime(test_df['ds'])
        
        #model 
        model = Prophet(yearly_seasonality=False,changepoint_prior_scale=.1,seasonality_prior_scale=0.05)
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

def forcomb(s,a,df,ftime1,cpu_perc_list):
    
    df2 = df[ (df.source == s)]
   
    prophet_df = pd.DataFrame()
    prophet_analysis_df = pd.DataFrame()
    prophet_future_df = pd.DataFrame()

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
             
        prophet_analysis_df,ew_model,ew_forcast,prophet_df,prophet_future_df =(create_prophet_m(a,df2,cpu_perc_list,config.delay))

        cpu_perc_list.append(py.cpu_percent())
        cpu_perc_list = [max(cpu_perc_list)]

        
        t2 = datetime.now()
        prophet_analysis_df['total_run_time'] = round(((t2-ftime1).seconds/60),2)
        
        prophet_analysis_df['application'] = a
        prophet_analysis_df['source'] = s
        
            
       
        prophet_future_df['application'] = a
        prophet_future_df['source'] = s
        
        prophet_df['application'] = a
        prophet_df['source'] = s

    df2 = df2.reset_index()
    return prophet_df, prophet_analysis_df, prophet_future_df , df2

## to create required combinations as per the report
def comb_creation(apps,cpu_perc_list):
    from pyspark.sql.functions import when

    q1 = datetime.now()

    df_t = df.registerTempTable('dummy')
    df_t = sqlContext.sql('select avg(app_rsp_time) as app_rsp_time, time_stamp, source , application  from dummy group by source, application, time_stamp')

    #df_needed = df[df.application.isin(apps)]
    df_t = df_t.registerTempTable('dummy')
    df_t = sqlContext.sql('select count(*) as count, source , application  from dummy group by source, application')

    df_t= df_t.withColumn('count_flag', when(df_t['count']>config.limit,1).otherwise(0))
    df_t = df_t[df_t.count_flag==1]
    
    # fetching the  source which is to be filtered from filter_db table
    with conn.cursor() as cursor:
        # Read a  record
        sql = "select * from filter_db" 
        cursor.execute(sql)
        so_result = pd.DataFrame(cursor.fetchall())
    
    #filtering
    from pyspark.sql.functions import col
    #print(so_result)
    s_filter = list(so_result.source)
    df_t = df_t.filter(~col('source').isin(s_filter))
    #df_t = df_t[df_t.source!='134.141.5.104']
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

    df = sqlContext.read.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1')

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
    
    pid = os.getpid()
    py = psutil.Process(pid)
    cpu_perc_list = list()



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
    app_rsp_time_full_df = pd.DataFrame()
    #ap_list = ['Skype for Business','Outlook']

    t2 = datetime.now()
    time_to_fetch = str(t2-t1)

    for a in tqdm(ap_list):
    #for k in tqdm(range(0,1)):
        #a = ap_list[k]

        qt1 = datetime.now()
    
        data = df[(df.application == a ) ]

        cpu_perc_list.append(py.cpu_percent())
        cpu_perc_list = [max(cpu_perc_list)]



        df_t = data.registerTempTable('dummy')
        df_t = sqlContext.sql('select avg(app_rsp_time) as app_rsp_time, time_stamp, source , application  from dummy group by source, application, time_stamp')
       
        # data cleaning
        df_t = df_t[df_t.app_rsp_time!=0]
        app_rsp_time_df=df_t.toPandas()
    

        s_array = app_rsp_time_df.source.unique()
        #s_array = rdf.source.unique()

        app_rsp_time_df = app_rsp_time_df.sort_values(by='app_rsp_time',ascending=True)       
        dates_outlook = pd.to_datetime(pd.Series(app_rsp_time_df.time_stamp),unit='ms')
        app_rsp_time_df.index = dates_outlook   
        app_rsp_time_df = app_rsp_time_df.sort_values(by='time_stamp')
        #app_rsp_time_df.to_csv('p1/app_rsp_time_per_app_per_source_dataset.csv',index=False)
        print('quering is successfull')



        logging.info(datetime.now())
        logging.info('-I- Fetching query for '+a+' is successfull...')

        qt2 = datetime.now()
        query_time = str(qt2-qt1)

 
        # Running for all combiantions

        qt3 = datetime.now()


        
        r0  = pool(delayed(forcomb)(s,a,app_rsp_time_df,qt1,cpu_perc_list) for s in s_array) 

        cpu_perc_list.append(py.cpu_percent())
        cpu_perc_list = [max(cpu_perc_list)]



        for i in range(0,len(r0)):
            
            prophet_df = prophet_df.append(r0[i][0])
            prophet_analysis_df = prophet_analysis_df.append(r0[i][1])
            prophet_future_df = prophet_future_df.append(r0[i][2])
            app_rsp_time_full_df = app_rsp_time_full_df.append(r0[i][3])
  
        qt4 = datetime.now()
        model_time  = str(qt4-qt3)
  
   

    cpu_perc_list.append(py.cpu_percent())
    cpu_perc_list = [max(cpu_perc_list)]

   

    prophet_future_df['run_date'] = this_day
    
    total_t2 = datetime.now()
    # calculating runtime in minuts
    total_real = (total_t2 - total_t1).seconds/60
    total_time = str(total_t2 - total_t1)

    #for analysis of our model in future

    prophet_analysis_df['run_date'] = this_day
    #prophet_analysis_df['total_run_time'] = total_real
    prophet_analysis_df.index = list(range(0,len(prophet_analysis_df)))

    conn.close()
    spark1.stop()
    #conn2.close()
    
    return prophet_future_df