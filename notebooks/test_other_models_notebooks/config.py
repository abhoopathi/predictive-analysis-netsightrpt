# project path
proj_path = '.'
#proj_path = '/root/ML/notebooks/exploratory_analysis/project_final'

## mysql db details
db_user = 'netsight'
db_pass = 'Enterasys'
db_name = 'netsightrpt'
db_host = '10.65.47.80'
db_port = 4589


## spark details
sp_master = 'local'
sp_appname = 'reports'
sp_memory = '24gb'
sp_cores = '16'

## the number of datapoints to be forcasted
delay = 30

## flag to enable and disable realtime prediction and writing the results to db
# 0-disable , 1-enable
real_flag = 1

## minimum number of datapoints that any combination's dataframe should contain...
limit = 60

## apps
apps = ['DNS', 'DHCP', 'Radius', 'LDAP','Kerberos']


## flag to enable and disable overriding of latest result to db
# 0 - to append the new result with existing , 1 - override the table with new result
override_flag = 1

