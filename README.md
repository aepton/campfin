A system for fetching campaign finance data from regulators, generating Open Civic Data Campaign Finance-compliant data files from it, and uploading them to Socrata for sharing and analysis.

Implemented so far
==================

US (FEC) - 2015/2016 contributions
WA (PDC) - 2007 - present contributions

Folder structure:
=================

data/
-----
  Data files for various states of individual operations

fetch/
------
  Base class, Fetcher, and scripts that fetch data from regulators

scripts/
--------
  Utilities that knit everything together.

transform/
----------
  Base classes matching OCD spec and scripts to transform data from regulators into that spec

upload/
-------
  Utilities to upload OCD-compliant data files to Socrata