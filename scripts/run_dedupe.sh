#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin
LOG_PATH=$APP_PATH/logs/dedupe.log
cd $APP_PATH

source /home/ubuntu/envs/campfin/bin/activate

touch $LOG_PATH
echo "Starting at `date`"
time python -m deduplication.deduper >> $LOG_PATH
echo "Ending at `date`"