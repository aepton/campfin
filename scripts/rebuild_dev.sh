#!/bin/bash

APP_PATH=/Users/abraham.epton/code/campfin/
cd $APP_PATH

# Blow away the unneeded data directory
rm -rf data/OCD/

# Run the fetchers
python fetch/fec.py
python fetch/wa.py

# Run the transformers
python transform/fec.py
python transform/wa.py

# Upload to Socrata (uses -m for relative imports)
python -m upload.uploader