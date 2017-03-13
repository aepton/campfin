#!/bin/bash

APP_PATH=/home/ubuntu/code/campfin/
cd $APP_PATH

source /home/ubuntu/.virtualenvs/campfin/bin/activate

# Blow away the unneeded data directories
rm -rf data/OCD/
rm -rf data/FEC/
rm -rf data/WA/

# Run the fetchers
python fetch/fec.py
python fetch/wa.py

# Run the transformers
python transform/fec.py
python transform/wa.py

# Upload to Socrata (uses -m for relative imports)
# python -m upload.uploader