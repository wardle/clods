# clods

A web service and set of tools for manipulating UK health and care organisational data
together with supporting data such as geographical datasets from the ONS (e.g. the 
NHS postcode directory)

This is designed to provide "location" services down to the granularity of an organisational site
with more finely-grained location services (e.g. ward, bed) provided by other modules
as part of a unified concierge location service. Ward and bed location and status data are usually
provided as part of a patient administrative system rather than reference data services, but
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

# Getting started: importing data

Create a database (default is jdbc:postgresql://localhost/ods)

```shell
clj -M -m com.eldrix.clods.cli init-database
clj -M -m com.eldrix.clods.cli migrate-database
```

You must import data in the correct order to preserve
relational integrity. Unfortunately, it isn't possible (yet)
to automate the download and import processes. 


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

You can download the most recent NHS ODS data from TRUD.
See [https://isd.digital.nhs.uk/trud3/user/guest/group/0/pack/5/subpack/341/releases](https://isd.digital.nhs.uk/trud3/user/guest/group/0/pack/5/subpack/341/releases)

Unzip the files and then run:

```shell
clj -M -m com.eldrix.clods.cli import-ods-xml /Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml
clj -M -m com.eldrix.clods.cli import-ods-xml /Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml
```

Download and import NHS general practitioner data. 
For these file types, data files are downloaded automatically.

```shell
clj -M -m com.eldrix.clods.cli import-gps
```

In the future, I hope to automate all of these downloads permitting automated
subscriptions and updates without manual intervention.

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
