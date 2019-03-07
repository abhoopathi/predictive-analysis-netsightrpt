import os
import shutil
## for debuging
#os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-oracle"
#os.environ['SPARK_HOME']= "/usr/local/spark/spark-2.3.0-bin-hadoop2.7"
#os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local[2] pyspark-shell"

from pyspark.sql import SQLContext
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from pyspark.sql import SparkSession
import pymysql
from tqdm import tqdm




import config

from datetime import datetime, time, timedelta
import pandas as pd

# creating the mysql link
def connect_to_mysql():
    connection = pymysql.connect(host = config.db_host,
                            port= config.db_port,
                            user= config.db_user,
                            password= config.db_pass,
                            db= config.db_name,
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    return connection


from sqlalchemy import create_engine
engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))

full_t1 = datetime.now()

  
      
def append_to_parquet_datapoint():
    

    ## FOR APPID_ATTRIBUTE
    print('--------------------------FOR APPID_DATAPOINT-----------------------')
 
    t1 = datetime.now()

    ## finding the max time_stamp from attribute and datapoint
    ## converting the df which less than a timestamp 
    df = datapoint_df.registerTempTable('dummy')
    ts = sqlContext.sql('select  max(time_stamp) from dummy ').collect()

    if(config.test_val==1):
        datapoint_df1 = datapoint_df.registerTempTable('dummy')
        datapoint_df1 = sqlContext.sql('select  *  from dummy where time_stamp<'+str(ts[0][0]))
    else:
        datapoint_df1 = datapoint_df

    df_len  = datapoint_df.count()
    print('length = ',df_len)
    
    
    ## new max time_stamp
    df = datapoint_df1.registerTempTable('dummy')
    ts_max = sqlContext.sql('select  max(time_stamp) from dummy ').collect()

    print('max time_stamp = ',ts_max[0][0])
    
    ts_max_date = pd.to_datetime(pd.Series(ts_max[0][0]),unit='ms')
    #ts_max2 = tt+timedelta(days=1)
    #ts_max2 = int(datetime.timestamp(ts_max2[0]))*1000
    
    old_schema = datapoint_df1.schema
    
    ##finding the nof datapoints available
    with conn.cursor() as cursor:
        # Read a  record
        sql = "select count(distinct time_stamp) as count from "+config.table_datapoint+" where time_stamp>"+str(ts_max[0][0])
        cursor.execute(sql)
        temp = (cursor.fetchall())
        if(temp):
            counts = pd.DataFrame(temp)
        else:
            counts = pd.DataFrame()
    
    print('nof data points available =',counts['count'])
    #for j in range(0,24):
    for j in range(0,counts['count'][0]):
        #print(j)
        try:    
            temp1 = ts_max_date[0] + timedelta(hours=j)
            ts_max1=(int(datetime.timestamp(temp1))*1000)
            temp1 = ts_max_date[0] + timedelta(hours=(j+1))
            ts_max2=(int(datetime.timestamp(temp1))*1000)
            
            print('---fetching from db---')
    
            ## fetching the latest data from the table in db
            with conn.cursor() as cursor:
                # Read a  record
                sql = "select * from "+config.table+" where time_stamp>"+str(ts_max1)+" and time_stamp <="+str(ts_max2)
                cursor.execute(sql)
                temp = (cursor.fetchall())
                if(temp):
                    datapoint_latest = pd.DataFrame(temp)
                else:
                    datapoint_latest = pd.DataFrame()

            if(len(datapoint_latest>0)):
                datapoint_latest = datapoint_latest[old_schema.names]
                datapoint_latest.to_csv('temp.csv',index=False)

                datapoint_latest = None
                datapoint_latest = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).load('temp.csv')

        
                print('filtered_max = ',ts_max1)
        
        
                # appending to existing parquet file
                datapoint_latest.write.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1',
                                        mode='append')
                #datapoint_final.write.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1',mode='overwrite')
            t2 = datetime.now()
            print('time taken  = ',str(t2-t1))
            print('real_max = ',ts[0][0])
        except:
            print('memory error  or connection error')
    
    
def append_to_parquet_attribute():

    ## FOR APPID_ATTRIBUTE
    print('--------------------------FOR APPID_ATTRIBUTE---------------------')
    
    t1 = datetime.now()
    
    ## finding the max time_stamp from attribute and datapoint
    ## converting the df which less than a timestamp 
    df = attribute_df.registerTempTable('dummy')
    ts = sqlContext.sql('select  max(time_stamp) from dummy ').collect()
    
    print(ts[0][0])

    if(config.test_val==1):
        attribute_df1 = attribute_df.registerTempTable('dummy')
        attribute_df1 = sqlContext.sql('select  *  from dummy where time_stamp<'+str(ts[0][0]))
    else:
        attribute_df1 = attribute_df

    df_len  = attribute_df.count()
    print('length = ',df_len)
    
    ## new max time_stamp
    df = attribute_df1.registerTempTable('dummy')
    ts_max = sqlContext.sql('select  max(time_stamp) from dummy ').collect()
    print('max timestamp from prquet . = ',ts_max[0][0])
    ts_max_date = pd.to_datetime(pd.Series(ts_max[0][0]),unit='ms')
    
    old_schema = attribute_df1.schema
    
    ##finding the nof datapoints available
    with conn.cursor() as cursor:
        # Read a  record
        sql = "select count(distinct time_stamp) as count from "+config.table_attribute+" where time_stamp>"+str(ts_max[0][0])
        cursor.execute(sql)
        temp = (cursor.fetchall())
        if(temp):
            counts = pd.DataFrame(temp)
        else:
            counts = pd.DataFrame()
    
    
    #for j in range(0,24):
    for j in range(0,counts['count'][0]):
        try:

            temp1 = ts_max_date[0] + timedelta(hours=j)
            ts_max1=(int(datetime.timestamp(temp1))*1000)
            temp1 = ts_max_date[0] + timedelta(hours=(j+1))
            ts_max2=(int(datetime.timestamp(temp1))*1000)
    
            print('--fetching from db--')

            ## fetching the latest data from the table in db
            with conn.cursor() as cursor:
                # Read a  record
                sql = "select * from "+config.table_attribute+" where time_stamp>"+str(ts_max1)+" and time_stamp <="+str(ts_max2)
                cursor.execute(sql)
                temp = (cursor.fetchall())
                if(temp):
                    attribute_latest = pd.DataFrame(temp)
                else:
                    attribute_latest = pd.DataFrame()
    
            if(len(attribute_latest>0)):
    
                attribute_latest = attribute_latest[old_schema.names]
                attribute_latest.to_csv('temp.csv',index=False)
        
                #print(attribute_latest.head())
                #datapoint_latest = sqlContext.createDataFrame(datapoint_latest1,schema=old_schema)
    
                attribute_latest = None
                attribute_latest = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).load('temp.csv')
    
                #appending existing parquet files
                attribute_latest.write.parquet(config.proj_path+'/datas_new/appid_attribute_parquet',
                                            mode='append')
        
                print('filtered_max = ',ts_max1)
            #attribute_final.write.parquet(config.proj_path+'/datas_new/appid_attribute_parquet',mode='overwrite')
            t2 = datetime.now()
            print('time taken  = ',str(t2-t1))
            print('real_max = ',ts[0][0])
        except:
            print('Memory error or connection error')
    
    


##to make parquet files of previouse 90 days data 
def append_to_parquet_attribute_not_present_90():

    ## FOR APPID_ATTRIBUTE
    print('--------------------------FOR APPID_ATTRIBUTE---------------------')
    
    ## finding the max time_stamp from attribute and datapoint
    t1 = datetime.now()

    ## new max time_stamp list
    midnight = datetime.combine(datetime.today(), time.min)
    #midnight1 = pd.to_datetime(pd.Series(1518188400000),unit='ms')
    #midnight = midnight1[0]

    for i in tqdm(range(90,0,-1)):
    
        
        yesterday_midnight = midnight - timedelta(days=i)
        #ts_max1=(int(datetime.timestamp(yesterday_midnight))*1000)
        #yesterday_midnight = midnight - timedelta(days=(i-1))
        #ts_max2=(int(datetime.timestamp(yesterday_midnight))*1000)

        for j in range(0,24):
            try:
                temp1 = yesterday_midnight + timedelta(hours=j)
                ts_max1=(int(datetime.timestamp(temp1))*1000)
                temp1 = yesterday_midnight + timedelta(hours=(j+1))
                ts_max2=(int(datetime.timestamp(temp1))*1000)
    
                #old_schema = attribute_df1.schema
                print('--fetching from db--')

                ## fetching the latest data from the table in db
                with conn.cursor() as cursor:
                    # Read a  record
                    sql = "select * from "+config.table_attribute+" where time_stamp>="+str(ts_max1)+" and time_stamp<"+ str(ts_max2)
                    cursor.execute(sql)
                    temp = (cursor.fetchall())
                    if(temp):
                        attribute_latest = pd.DataFrame(temp)
                    else:
                        attribute_latest = pd.DataFrame()

                if(len(attribute_latest)>0):
                    #attribute_latest = attribute_latest[old_schema.names]
                    attribute_latest.to_csv('temp.csv',index=False)


                    attribute_latest = None
                    attribute_final = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).load('temp.csv')


                    df = attribute_final.registerTempTable('dummy')
                    ts_max2 = sqlContext.sql('select  max(time_stamp) from dummy ').collect()

                    attribute_final.write.parquet(config.proj_path+'/datas_new/appid_attribute_parquet',
                                    mode='append')
                    t2 = datetime.now()
                    print('time taken  = ',str(t2-t1))
                    print('filtered_max = ',ts_max1)
                    print('appended final_max = ',ts_max2[0][0])
                else:
                    print("-I- No data available")
            except:
                print('Memory error or connection error')


def append_to_parquet_datapoint_not_present_90():
    

    ## FOR APPID_ATTRIBUTE
    print('--------------------------FOR APPID_DATAPOINT-----------------------')
    

    ## finding the max time_stamp from attribute and datapoint
    t1 = datetime.now()
    
    ## new max time_stamp
    midnight = datetime.combine(datetime.today(), time.min)
    #midnight1 = pd.to_datetime(pd.Series(1518188400000),unit='ms')
    #midnight = midnight1[0]
    for i in tqdm(range(90,0,-1)):
    #for i in tqdm(range(10,9,-1)):
    
        
        yesterday_midnight = midnight - timedelta(days=i)
        #ts_max1=(int(datetime.timestamp(yesterday_midnight))*1000)
        #yesterday_midnight = midnight - timedelta(days=(i-1))
        #ts_max2=(int(datetime.timestamp(yesterday_midnight))*1000)
        
        for j in range(0,24):
            try:
                temp1 = yesterday_midnight + timedelta(hours=j)
                ts_max1=(int(datetime.timestamp(temp1))*1000)
                temp1 = yesterday_midnight + timedelta(hours=(j+1))
                ts_max2=(int(datetime.timestamp(temp1))*1000)
    
                #old_schema = datapoint_df1.schema
                print('---fetching from db---')
                ## fetching the latest data from the table in db
                with conn.cursor() as cursor:
                    # Read a  record
                    sql = "select * from "+config.table_datapoint+" where time_stamp>="+str(ts_max1)+" and time_stamp<"+ str(ts_max2)
                    cursor.execute(sql)
                    temp = (cursor.fetchall())
                    if(temp):
                        datapoint_latest = pd.DataFrame(temp)
                    else:
                        datapoint_latest = pd.DataFrame()

                if(len(datapoint_latest)>0):
                    #datapoint_latest = datapoint_latest[old_schema.names]
                    datapoint_latest.to_csv('temp.csv',index=False)
    
                    #print(datapoint_latest1.head())
                    #datapoint_latest = sqlContext.createDataFrame(datapoint_latest1,schema=old_schema)

                    datapoint_latest = None
                    datapoint_final = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).load('temp.csv')


                    df = datapoint_final.registerTempTable('dummy')
                    ts_max2 = sqlContext.sql('select  max(time_stamp) from dummy ').collect()
    
                    datapoint_final.write.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1',
                                    mode='append')
                    t2 = datetime.now()
                    print('time taken  = ',str(t2-t1))
                    print('filtered_max = ',ts_max1)
                    print('appended final_max = ',ts_max2[0][0])
                else:
                    print("-I- no data available")
            except:
                print('Memory error or connection error')

def make_90day():

    df = sqlContext.read.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1')
    ts_max = df.registerTempTable('dummy')
    ts_max = sqlContext.sql('select  max(time_stamp) from dummy ').collect()
    max_date = pd.to_datetime(pd.Series(ts_max[0][0]),unit='ms')
    min_date = max_date[0]-timedelta(days=90)
    ts_min = int(datetime.timestamp(min_date))*1000

    df2 = df.registerTempTable('dummy')
    df2 = sqlContext.sql('select  * from dummy where time_stamp>='+str(ts_min))
    #os.system("rm -rf "+config.proj_path+"/datas_new/appid_datapoint_parquet1")
    print('final length of parquet file =',df2.count())
    df2.write.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet')

    datapoint_df = None
    df2 = None
    shutil.rmtree(config.proj_path+'/datas_new/appid_datapoint_parquet1')
    os.rename(config.proj_path+'/datas_new/appid_datapoint_parquet', config.proj_path+'/datas_new/appid_datapoint_parquet1')

    


if __name__=='__main__' :
#def main(this_day):

    
    global datapoint_df
    global attribute_df
    global conn
    global spark1
    global sqlContext 
    global sc
        
    try:
        conn=connect_to_mysql()
    except:
        conn=None
        print('Connection failed')


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

    datapoint_flag = 1
    attribute_flag = 1

    ## Reading the df from the parquet files 
    time_1 = datetime.now()

    newpath = config.proj_path+'/datas_new/appid_attribute_parquet' 
    if os.path.exists(newpath):
        attribute_df =  sqlContext.read.parquet(config.proj_path+'/datas_new/appid_attribute_parquet')

        ## For validation
        #tt = attribute_df.registerTempTable('dummy')
        #attribute_df = sqlContext.sql('select * from dummy where time_stamp<'+str(int(datetime.timestamp(this_day))*1000))
        
    else:
        os.makedirs(newpath)
        attribute_flag =0

    newpath = config.proj_path+'/datas_new/appid_datapoint_parquet1' 
    if os.path.exists(newpath):
        datapoint_df = sqlContext.read.parquet(config.proj_path+'/datas_new/appid_datapoint_parquet1')
        
        ## For validation
        #tt = datapoint_df.registerTempTable('dummy')
        #datapoint_df = sqlContext.sql('select * from dummy where time_stamp<'+str(int(datetime.timestamp(this_day))*1000))
        
    else:
        os.makedirs(newpath)
        datapoint_flag =0


    if( datapoint_flag==1):
        append_to_parquet_datapoint()
        make_90day()
    elif(datapoint_flag==0):
        append_to_parquet_datapoint_not_present_90()

    if( attribute_flag==1):
        append_to_parquet_attribute()
        #print('done')
    elif(attribute_flag==0):
        append_to_parquet_attribute_not_present_90()
        #print('done')
    try:
        conn.close()
        spark1.stop()
    except:
        print('closing failed')
    
    #conn2.close()
    time_2 = datetime.now()
    print('total time taken for writing table to parquet = ',str(time_2-time_1))