# project path
#proj_path = './..'
proj_path = '.'
#proj_path = '/root/ML/notebooks/notebooks/exploratory_analysis/docker_implementation/aggregated_1day_prediction'

## mysql db details
db_user = 'netsight'
db_pass = 'Enterasys'
db_name = 'netsightrpt'
db_name2 = 'netsight'
db_host = '10.65.47.80'
db_port = 4589
table_datapoint = 'appid_datapoint_backup_10days'
table_attribute = 'appid_attribute_backup'


## spark details
sp_master = 'local'
sp_appname = 'reports'
sp_memory = '12gb'
sp_cores = '2'

## the number of datapoints to be forcasted
delay = 30
delay_48 = 48

## sample applications using for the simple prediction 
#apps = ['DCERPC','DNS','SSH','Salesforce']
apps = ['DNS', 'DHCP', 'Radius', 'LDAP','Kerberos']

## for evaluation of parquet file creation
# test_val = 1 for evaluation 0 for realworld appending process
test_val = 0

## flag to enable and disable realtime prediction and writing the results to db
# 0-disable , 1-enable
real_flag = 1

## minimum number of datapoints that any combination's dataframe should contain...
limit = 60

## flag to enable and disable overriding of latest result to db
# 0 - to append the new result with existing , 1 - override the table with new result
override_flag_analysis = 0
override_flag_forecast = 0

