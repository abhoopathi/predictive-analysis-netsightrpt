
# Predictive analysis netsightrpt

## Overview

Here in this project we predictiong the following things using the network data provided in the database 'netsightrpt' .
* Response time per application (P1): Here the ‘app response time’ of network data per source/application is forecasted.
* Number of applications (P2): Here the number of applications of network data per source is forecasted.
* Number of users per application (P3): Here the number of users(targets) of network data per application/source is forecasted.
* Number users per location (P6): Here the number of users(targets) of network data per location is forecasted. 
* Bandwidth per location (P7): Here the bandwidth of network data per location is forecasted.
* Number of users (P8): Here the number of users(targets) of network data per source is forecasted.
* Bandwidth per source (P9): Here the bandwidth of network data per source is forecasted.
* Bandwidth per application (P10): Here the bandwidth of network data per application/source is forecasted. 

These predictions are done for both 30 days and 48 hours.

## Data flow  overview

The main network data is in database 'netsightrpt' with names 'appid_datapoint_backup_10days'
and 'appid_attribute_backup' . It contains the hourly data of network. 

So here in our model we uses 90 days data for the forecasting of 30 days . 
And 90 hours data for the forecasting of 48 hours.

Poping data from db is takes more time , so that we decided to  put the 90 days data as parquet file . which is faster and consume less memory .

Below given is the flow of data .

![image.png](attachment:image.png)

### parquet file creation

'parquet_file_creation_agg.py' file is used to create parquet files from db. Here in this script it creates a directory to store the data , and then take 90 days data and  wirte it as parquet files.

 
If the output directory already exists it writes only the latest data to output directory and  keeps only 90 days data in it .

### forecasting reports

In forecasting reports, as per the desired output there are 8 reports are there (mensioned in the overview part) .

And they do the following things ...

- reads the data from the parquet files to spark dataframe inorder to reduce the memory and increase the processing speed.

- Then the required data preparation and cleaning is done on it , findining the required coombinations also ...

- Then it runs the model model and output is writing into db again. The outputs are the forecast , analysis and validation tables .
    - eg : analyse_apprsptime_per_app_per_source , forecast_apprsptime_per_app_per_source and valid_apprsptime_per_app_per_source



This report will be 30days forecast and 48hours forecast .

To run all script we created 1 script named 'run_all_script_beta_scheduled.py' . which will create the parquet file and run all forecasting reports too.

### Docker 

You may notice that the all process is running under docker. 

Docker is a software platform designed to make it easier to create, deploy, and run applications by using containers. It allows developers to package up an application with all the parts it needs in a container, and then ship it out as one package.

To create a docker image first create a dockerfile 'Dockerfile'  (The dockerfile used inthis project is also put in this repo .

Here we created a docker image `beta_agg_reports_30d_and_48h` which holds the parquet data files and all report scripts . And runs the code at 12am every day ( This scheduling is done using the schedule package in python ) , And writes the output on the 'netsightrpt' db .

Inoreder to run all script, it first check a flag (value in column ‘option value’ like 'BetaOptions.betaFeatures' in table nsoptions is true or false  in db netsight ) .

The docker container name used here is **beta_agg_repo_30d_and_48h**

Some of the main docker commmands are :

* **docker build -t beta_agg_reports_30d_and_48h  .**  :- to create docker image , here beta_agg_reports_30d_and_48h is the docker image name .
* **docker run --name beta_agg_repo_30d_and_48h -it -d  --cpuset-cpus='0-4' beta_agg_reports_30d_and_48h**  :- to create and run docker container , here beta_agg_repo_30d_and_48h is the container name , which uses only 4 cpu cores .
* **docker exec -i -t beta_agg_repo_30d_and_48h /bin/bash ** :- to get inside the docker container in interactive mode .
* **docker stop beta_agg_repo_30d_48h** :- used to stop the container.
* **docker rm beta_agg_repo_30d_48h ** :- used to remove the container.
* **docker rmi beta_agg_reports_30d_48h** :- used to remove the image.

## DB details

The that we deals with for this project is **netsightrpt** 

The input tables which is used to update the parquet files is `appid_attribute_backup` and `appid_datapoint_backup` .

For each run of the script 5 tables are updating for each reports .
*  anlyse_* 
*  forecast_*  
*  valid_*  
*  data_* 
*  run_time_table_agg 

### Analysis table :

This table contains the analysis result such as the rmse,mape,etc… values of different combinations in the different reports.
The columns used in this table are : 

- length : length of the daily based aggregated data used for modeling for each combinations .
- max_error_rate (bigInt): maximum error rate of forecast values (using test set ) for each combinations
- median_ error_rate (double): median error rate of forecast values (using test set ) for each combinations
- min_ error_rate (double): minimum error rate of forecast values (using test set ) for each combinations
- std_mape (double): standard deviation of absolute percentage error for each combinations
- test_mape (double): mean absolute percentage error of each combinations using the test dataset
- test_mape_98 (double): 98% of mean absolute percentage error of each combinations using the test dataset
- test_rmse (double): root mean squired  error of each combinations using the test dataset
- total_runtime (double): total runtime for each combinations
- application (text): application name
- source (text): source address
- location (text): location name
- run_date (date): the date when the report is ran

Here all columns (application,source and location) are not present in all reports , it varies as per the combination.

### Forecast table :

This table contains the forecast result such as the  predicted values of user_count or bandwidth for different combinations in the different reports.
The columns used in this table are : 

- ds (datetime): date of forecast
- yhat (double): predicted value for forecasted date
- application (text): application name
- source (text): source address
- location (text): location name
- run_date (date): date on which the report has been ran

Here all columns (application,source and location) are not present in all reports , it varies as per the combination.

### Validation table :

This table contains the evaluation (validation) result such as the actual and predicted values of user_count or bandwidth for different combinations in the different reports.
The columns used in this table are : 

- ds (datetime) : date of forecast
- y (double): real value of forcasted date
- yhat (double): predicted value for forecasted date
- error_test (double): error in the prediction (yhat-y)
- APE (double): absolute percentage error of each date of each combinations
- application (text): application name
- source (text): source address
- location (text): location name

Here all columns (application,source and location) are not present in all reports , it varies as per the combination.

### Data used for modeling :

This table contains the data that is used for modeling for different combinations in the different reports. This table used to draw the previous days values in the demo graphs.
The columns used in this table are : 

- date (date): date of value (user_count or bw)
- user_count (bigInt): nof users of that combination for that date
- bw (double): bandwidth of that combination for that date
- application (text): application name
- source (text): source address
- location (text): location name

Here all columns (user_count, bw, application, source and location) are not present in all reports , it varies as per the combination.

for **App Response time per application (P1) ** [both 30days and 48hrs pediction] :

* analyse_apprsptime_per_source_per_app and analyse_apprsptime_per_source_per_app_48hrs
* forecast_apprsptime_per_source_per_app and forecast_apprsptime_per_source_per_app_48hrs
* valid_apprsptime_per_source_per_app and valid_apprsptime_per_source_per_app_48hrs
* data_apprsptime_per_source_per_app and data_apprsptime_per_source_per_app_48hrs


for **Number of applications (P2) ** [both 30days and 48hrs pediction] :

* analyse_nofapp_per_source and analyse_nofapp_per_source_48hrs
* forecast_nofapp_per_source and forecast_nofapp_per_source_48hrs
* valid_nofapp_per_source and valid_nofapp_per_source_48hrs
* data_nofapp_per_source and data_nofapp_per_source_48hrs

for **Number of users per application (P3)** [both 30days and 48hrs pediction] :

* analyse_nofusers_per_app_per_source and analyse_nofusers_per_app_per_source_48hrs
* forecast_nofusers_per_app_per_source and forecast_nofusers_per_app_per_source_48hrs
* valid_nofusers_per_app_per_source and valid_nofusers_per_app_per_source_48hrs
* data_nofusers_per_app_per_source and data_nofusers_per_app_per_source_48hrs


for **Number users per location (P6) ** [both 30days and 48hrs pediction] :

* analyse_nofusers_per_location and analyse_per_location_48hrs
* forecast_nofusers_per_location and forecast_per_location_48hrs
* valid_nofusers_per_location and valid_nofusers_per_location_48hrs
* data_nofusers_per_location and data_nofusers_per_location_48hrs


for **Bandwidth per location (P7)** [both 30days and 48hrs pediction] :

* analyse_bw_per_location and analyse_per_location_48hrs
* forecast_bw_per_location and forecast_per_location_48hrs
* valid_bw_per_location and valid_bw_per_location_48hrs
* data_bw_per_location and data_bw_per_location_48hrs


for **Number of users per source (P8) ** [both 30days and 48hrs pediction] :

* analyse_nofusers_per_source and analyse_nofusers_per_source_48hrs
* forecast_nofusers_per_source and forecast_nofusers_per_source_48hrs
* valid_nofusers_per_source and valid_nofusers_per_source_48hrs
* data_nofusers_per_source and data_nofusers_per_source_48hrs


for **Bandwidth per source (P9)** [both 30days and 48hrs pediction] :

* analyse_bw_per_source and analyse_bw_per_source_48hrs
* forecast_bw_per_source and forecast_bw_per_source_48hrs
* valid_bw_per_source and valid_bw_per_source_48hrs
* data_bw_per_source and data_bw_per_source_48hrs


for **Bandwidth per application (P10) ** [both 30days and 48hrs pediction] :

* analyse_bw_per_app_per_source and analyse_bw_per_app_per_source_48hrs
* forecast_bw_per_app_per_source and forecast_bw_per_app_per_source_48hrs
* valid_bw_per_app_per_source and valid_bw_per_app_per_source_48hrs
* data_bw_per_app_per_source and data_bw_per_app_per_source_48hrs


_ Here for all tables we doing partitioning to avoid the crash in output (end-user side) while writing the new results to table _
