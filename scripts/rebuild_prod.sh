#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin
LOG_PATH=$APP_PATH/logs/rebuild.log

cd $APP_PATH

source /home/ubuntu/envs/campfin/bin/activate

touch $LOG_PATH

echo "Starting at `date`" >> $LOG_PATH
time python -m orchestration.generate_combined_ocd_data &>> $LOG_PATH
echo "Ending at `date`" >> $LOG_PATH