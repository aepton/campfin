#!/bin/bash

rm -rf /Users/abraham.epton/code/campfin/data/OCD/
python /Users/abraham.epton/code/campfin/fetch/fec.py
python /Users/abraham.epton/code/campfin/fetch/wa.py
python /Users/abraham.epton/code/campfin/transform/fec.py
python /Users/abraham.epton/code/campfin/transform/wa.py
python /Users/abraham.epton/code/campfin/upload/uploader.py