
#import csv
import pandas as pd
#import pymysql

import config
from tqdm import tqdm


from sqlalchemy import create_engine
engine = create_engine(str("mysql+pymysql://"+config.db_user+":"+config.db_pass+"@"+config.db_host+":"+str(config.db_port)+"/"+config.db_name))
    

  
    
def upload_csv(path,table_name):
    
    chunksize = 10000
    i = 0
    j = 1
    for df in tqdm(pd.read_csv(path, chunksize=chunksize, iterator=True)):
        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
        df.index += j
        i+=1
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False )
        j = df.index[-1] + 1
          
    #return df
      


if __name__=='__main__':
    
    print('-I- uploading datapoint_attribute_backup table ')
    print('-----------------------------------------------')
    upload_csv('/root/appid_attribute_full_final.csv','appid_attribute_backup')
    print('-------------------------------------------------')
    print('uploaded appid_attribute successfully')
    
    print('-I- uploading datapoint_datapoint_backup table ')
    print('-----------------------------------------------')
    upload_csv('/root/appid_datapoint_full.csv','appid_datapoint_backup')
    print('-------------------------------------------------')
    print('uploaded appid_datapoint successfully')
    