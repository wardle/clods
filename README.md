# clods

[![Scc Count Badge](https://sloc.xyz/github/wardle/clods)](https://github.com/wardle/clods/)
[![Scc Cocomo Badge](https://sloc.xyz/github/wardle/clods?category=cocomo&avg-wage=100000)](https://github.com/wardle/clods/)

A web service and set of tools for manipulating UK health and care organisational data.

Health and Social Care Organisation Reference Data is published by NHS Digital 
under standard [DCB0090](https://digital.nhs.uk/data-and-information/information-standards/information-standards-and-data-collections-including-extractions/publications-and-notifications/standards-and-collections/dcb0090-health-and-social-care-organisation-reference-data).


`clods` is designed to provide "location" services down to the granularity of an organisational site using 
this reference data.

More finely-grained location services (e.g. ward, bed) are provided by other modules
as part of a unified [concierge location service](https://github.com/wardle/concierge). 
Ward and bed location and status data are usually provided as part of a patient administrative system rather than reference data services, but
this aims to provide a seamless application programming interface (API) in order to 
appropriately record the context of the capture of any clinical data. 

`clods` part of a suite of supporting foundational data and computing services that help to
answer questions about the 'who', 'what', 'when', 'how' and 'why' of health and care data.

This software provides both a library and web service. As a library, it can easily be embedded into a larger
application. As a microservice, it can easily be embedded into a suite of foundational platform services.

#### Design goals

At its core, the software provides a close representation of the original source
data while also providing a more abstract set of systems that will be federatable;
that means application software working across international boundaries with
runtime harmonisation and abstractions across multiple backend services.

- Data-first - structured, self-describing data.
- Properties as first-class abstractions rather than rigid class based 
  hierarchies. 
- Subject / predicate / object (entity / attribute / value) triples à la RDF.
- Standards-based - providing foundational software and data services.
- An 'open-world' assumption 
- Client-driven graph-like queries.

Public sector data should be open, published and self-describing, with
mechanisms to permit computability. That means we can write software that
automatically updates against master indexes of a range of important 
public sector data. 

When running as either a library or microservice, the only dependency is a filesystem.

In the olden days, we cared for individuals servers and gave them names, analogous to looking
after pets. They'd be long-lived and we'd run services on them. You'd have administrators manually
logging in to update-in-place, upgrading either the version of the software or updating the
backing data. 

You can still use this model for this and the other `PatientCare` software but the more modern approach is to treat your computing and data infrastructure as cattle. 
Unlike a pet, you don't usually name your cattle, and you might have a significant turnover in those cattle. As such
it is entirely reasonable to spin up new versions of this service with new updated data.

As such, this, and the other `PatientCare` services are designed to automate as many steps as possible.

# Getting started: 

You'll need to install [clojure](https://www.clojure.org) for the best experience
but it is possible to run using a pre-built jar. See below for information
about the jar files.

# Importing data

This service needs a directory on a filesystem to operate.

The NHS organisational data includes information about NHS organisations.
To enable geographical services, `clods` combines these data with NHS
geographical data using the 'NHS postcode directory'. You can use 
[nhspd](https://github.com/wardle/nhspd) as a standalone service, but for
convenience, `clods` includes that tooling.

## 1. Choose the location of your index files.

In these examples, we'll use

- /var/local/ods-2021-02   for our organisation index
- /var/local/nhspd-2020-11 for our postcode index

We also need to specify a temporary cache for downloaded data.

For these examples, we'll use

- /var/tmp/trud

You can choose to use a single directory and update-in-place, or build a new
repository at intervals. I prefer read-only, immutable backing data by default,
so favour the latter. 

## 2. Initialise the postcode service

You may already have [nhspd](https://github.com/wardle/nhspd) running; use the
index you use for that.

If not, let's get one set-up

```shell
clj -M:nhspd /var/local/nhspd-2020-11
```

After a few minutes, the NHS postcode directory index will have been downloaded
and imported. 

## 3. Initialise the organisation service

You will need a NHS Digital TRUD API key.  

Login to the [NHS Digital TRUD](https://isd.digital.nhs.uk/) and find your API key under your profile. 
Write that key to a file and link to it from the command-line:

```shell
clj -M:install --nhspd /var/local/nhspd-2020-11 --api-key /path/to/api-key.txt --cache-dir /var/tmp/trud /var/local/ods-2021-02
```

`clods` will proceed to download the latest distribution files from TRUD, or use the existing
downloaded version in your local cache if available, and create an organisation index.

While you could embed all of this into a single Docker image for deployment, it might be better to 
instead link to a shared read-only filesystem and simply link to the latest backend data.

# Running a simple REST server

To run as a microservice, you need to include the paths of the both 
an ODS index, and an NHSPD index as well as the port to run on.

```shell
clj -M:serve /var/local/ods-2021-02  /var/local/nhspd-2020-11 8080
```

There are three endpoints:

Let's get NHS postcode data about a postcode:

```shell
curl -H "Accept: application/json" localhost:8080/ods/v1/postcode/CF144XW
```
Result:
```json
{"CANNET":"N95","PCDS":"CF14 4XW","NHSER":"W92","SCN":"N95","PSED":"62UBFL16","CTRY":"W92000004","OA01":"W00009154","HRO":"W00","OLDHA":"QW2","RGN":"W99999999","OSWARD":"W05000864","LSOA01":"W01001770","OSNRTH1M":179319,"CANREG":"Y1101","OSHLTHAU":"7A4","CALNCV":"W99999999","OSGRDIND":"1","MSOA11":"W02000384","MSOA01":"W02000384","WARD98":"00PTMM","OLDHRO":"W00","CENED":"TNFL16","OLDPCT":"6A8","USERTYPE":"0","OSEAST1M":317551,"PCT":"7A4","PCD2":"CF14 4XW","NHSRLO":"W92","OSNRTH100M":1793,"DOTERM":"","STP":"W92","OSLAUA":"W06000015","OSHAPREV":"Q99","EDIND":"1","LSOA11":"W01001770","UR01IND":"5","CCG":"7A4","OSEAST100M":3175,"DOINTR":"199906","PCON":"W07000051","ODSLAUA":"052","OA11":"W00009154","OSCTY":"W99999999"}
```

Let's get ODS data about a known organisation:

```shell
curl -H "Accept: application/json" localhost:8080/ods/v1/organisation/7A4BV
```

Let's search for an organisation:

Simple search by name:

```shell
curl -H "Accept: application/json" 'localhost:8080/ods/v1/search?s=University%20Hospital%20Wales'
```

The underlying library supports search by organisation type/role as well as geographic
searches to find organisations close to a particular postcode - ie useful in building
operational user-facing applications or analytics. Documentation on how to use
from web service forthcoming.

# Running a FHIR-compatible server

```shell
clj -M:fhir-r4 /var/local/ods-2021-02 /var/local/nhspd-2020-11 8080
```

Let's try it:

```shell
http --json 'http://localhost:8080/fhir/Organization/2.16.840.1.113883.2.1.3.2.4.18.48|W93036' 
```

Result:
```json
{
  "resourceType": "Organization",
  "id": "W93036",
  "identifier": [ {
    "use": "official",
    "system": "https://fhir.nhs.uk/Id/ods-organization",
    "value": "W93036"
  }, {
    "use": "old",
    "system": "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48",
    "value": "W93036"
  } ],
  "active": true,
  "type": [ {
    "coding": [ {
      "system": "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.507",
      "code": "RO72",
      "display": "OTHER PRESCRIBING COST CENTRE"
    } ]
  }, {
    "coding": [ {
      "system": "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.507",
      "code": "RO177",
      "display": "PRESCRIBING COST CENTRE"
    }, {
      "system": "urn:oid:http://hl7.org/fhir/ValueSet/organization-type",
      "code": "prov",
      "display": "Healthcare Provider"
    } ]
  } ],
  "name": "CASTLE GATE MEDICAL PRACTICE",
  "telecom": [ {
    "system": "phone",
    "value": "01600 713811"
  } ],
  "address": [ {
    "line": [ "REAR OF MONNOW STREET" ],
    "city": "MONMOUTH",
    "district": "GWENT",
    "postalCode": "NP25 3EQ",
    "country": "WALES"
  } ],
  "partOf": {
    "type": "Organization",
    "identifier": {
      "use": "official",
      "system": "https://fhir.nhs.uk/Id/ods-organization",
      "value": "7A6"
    },
    "display": "ANEURIN BEVAN UNIVERSITY LHB"
  }
}
```

# Development / contributing

Check for outdated dependencies:

```shell
clj -M:outdated
```

Perform compilation checks (optional)

```shell
clj -M:check
```

Perform linting (optional)

```shell 
clj -M:lint/kondo
clj -M:lint/eastwood
```

# Building executable files 

If you prefer, you can generate jar files which can be run easily at the command line.

Build a utility uberjar and run it.

```shell
clj -X:uberjar
java -jar target/clods-full-v0.1.0.jar --help
```

Build a server uberjar and run it:
```shell
clj -X:server-uberjar
java -jar target/clods-rest-server-v0.1.0.jar /var/local/ods-2021-02 /var/local/nhspd-2020-11 8080
```

Build a FHIR server uberjar and run it:
```shell
clj -X:fhir-r4-uberjar
java -jar target/clods-fhir-r4-server-v0.1.0.jar /var/local/ods-2021-02 /var/local/nhspd-2020-11 8080
```

You can pass these standalone jar files around; they have no dependencies.

Copyright © 2020-21 Eldrix Ltd and Mark Wardle
