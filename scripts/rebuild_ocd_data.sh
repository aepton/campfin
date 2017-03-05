#!/bin/bash

rm -rf /Users/abraham.epton/code/ocd_campaign_finance/*.csv
python /Users/abraham.epton/code/campfin/fec_ingest.py
python /Users/abraham.epton/code/campfin/wa_ingest.py