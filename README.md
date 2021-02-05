# clods

[![Scc Count Badge](https://sloc.xyz/github/wardle/clods)](https://github.com/wardle/clods/)
[![Scc Cocomo Badge](https://sloc.xyz/github/wardle/clods?category=cocomo&avg-wage=100000)](https://github.com/wardle/clods/)

A web service and set of tools for manipulating UK health and care organisational data
together with supporting data such as geographical datasets from the ONS (e.g. the 
NHS postcode directory)

This is designed to provide "location" services down to the granularity of an organisational site.
More finely-grained location services (e.g. ward, bed) are provided by other modules
as part of a unified [concierge location service](https://github.com/wardle/concierge). 
Ward and bed location and status data are usually provided as part of a patient administrative system rather than reference data services, but
this aims to provide a seamless application programming interface (API) in order to 
appropriately record the context of the capture of any clinical data.

This service can provides a simple REST-based service or through a graph-like API. 

#### Design goals

- Data-first - structured, self-describing data.
- Properties as first-class abstractions rather than rigid class based 
  hierarchies. 
- Subject / predicate / object (entity / attribute / value) triples.
- Standards-based - providing foundational software and data services.
- An 'open-world' assumption 
- Client-driven graph-like queries.

Public sector data should be open, published and self-describing, with
mechanisms to permit computability. That means we can write software that
automatically updates against master indexes of a range of important 
public sector data. Unfortunately, even bringing together basic 
geographical data with NHS organisational data requires user registration,
manual downloads and processing. We need to change this.

# Getting started: 

You can get help at the command line by using '--help':

```shell
clj -M -m com.eldrix.clods.cli --help 
```

# Importing data

Create a database (default is jdbc:postgresql://localhost/ods).
You can change the database in the command-line options.

```shell
clj -M -m com.eldrix.clods.cli init-database
clj -M -m com.eldrix.clods.cli migrate-database
```

You must import data in the correct order to preserve
relational integrity, particularly for the first import. 
That means importing postcodes first. 
It isn't quite so important after the first import for ongoing updates.
Unfortunately, it does not yet seem possible to automate 
downloads of the NHS Postcode database.

### 1. Import NHS postcode data.

Download the most recent release of the NHSPD. 
For example, [https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-february-2020](https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-february-2020)

Then import: 

```shell
clj -M -m com.eldrix.clods.cli import-postcodes /Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv
```

```text
Importing postcodes from: /Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv ...
Imported  2640655  postcodes from ' /Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv '.
```

### 2. Download and import the NHS ODS XML files (current and archive)

You will need a TRUD API key. Login to the [NHS Digital TRUD](https://isd.digital.nhs.uk/) and find your API key under your profile. 

```shell
clj -M -m com.eldrix.clods.cli download-ods --api-key /path/to/api-key.txt
```

### 3. Download and import NHS general practitioner data. 

For these file types, data files are downloaded automatically.

```shell
clj -M -m com.eldrix.clods.cli download-gps
```


# Running a simple REST-ful server

```text
clj -M -m com.eldrix.clods.cli serve
```

# Keeping your data up-to-date

You can re-run the download steps at an interval to automatically get the latest versions of data.
The only manual step currently necessary is the NHS postcode directory.

# Development / contributing

Perform compilation checks (optional)

```shell
clj -M:check
```

Perform linting (optional)

```shell 
clj -M:lint/kondo
clj -M:lint/eastwood
```

Copyright © 2020-21 Eldrix Ltd and Mark Wardle
