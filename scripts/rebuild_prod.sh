#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin/
cd $APP_PATH

source /home/ubuntu/.virtualenvs/campfin/bin/activate

# Blow away the unneeded data directory
rm -rf data/OCD/

# Run the fetchers
python fetch/fec.py
python fetch/wa.py

# Run the transformers
python transform/fec.py
python transform/wa.py

# Upload to Socrata (uses -m for relative imports)
# python -m upload.uploader