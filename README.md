A system for fetching campaign finance data from regulators, generating Open Civic Data Campaign Finance-compliant data files from it, and uploading them to Socrata for sharing and analysis.

Implemented so far
==================

US (FEC)
--------
  - contributions
    - 2004 - 2018

WA (PDC)
--------
 - contributions
   - 2007 - present
   - [Play with the data here](https://abrahamepton.demo.socrata.com/dataset/WA-Campaign-Finance-FEC-and-PDC/gth4-t69d)

Folder structure:
=================

data/
-----
  Data files for various states of individual operations

fetch/
------
  Base class, Fetcher, and scripts that fetch data from regulators

orchestration/
--------------
  Generates combined OCD data, in a tightly-coupled fashion allowing the system to clean up after itself as it goes.

scripts/
--------
  Utilities (in process of deprecation) that knit everything together.

settings/
---------
  Directory locations, datetime formats and other miscellaneous shared settings.

transform/
----------
  Base classes matching OCD spec and scripts to transform data from regulators into that spec

upload/
-------
  Utilities to upload OCD-compliant data files to Socrata