#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin
cd $APP_PATH

source /home/ubuntu/envs/campfin/bin/activate

touch $APP_PATH/logs/rebuild.log
echo "Starting at `date`"
time python -m orchestration.generate_combined_ocd_data >> $APP_PATH/logs/rebuild.log
echo "Ending at `date`"