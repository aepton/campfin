#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin/
cd $APP_PATH

source /home/ubuntu/.virtualenvs/campfin/bin/activate

python -m orchestration.generate_combined_ocd_data