# clods

[![Scc Count Badge](https://sloc.xyz/github/wardle/clods)](https://github.com/wardle/clods/)
[![Scc Cocomo Badge](https://sloc.xyz/github/wardle/clods?category=cocomo&avg-wage=100000)](https://github.com/wardle/clods/)

A web service and set of tools for manipulating UK health and care organisational data.

This is designed to provide "location" services down to the granularity of an organisational site.

More finely-grained location services (e.g. ward, bed) are provided by other modules
as part of a unified [concierge location service](https://github.com/wardle/concierge). 
Ward and bed location and status data are usually provided as part of a patient administrative system rather than reference data services, but
this aims to provide a seamless application programming interface (API) in order to 
appropriately record the context of the capture of any clinical data. 

It's part of a suite of supporting foundational data and computing services that help to
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
but it is possible to run using a pre-built jar.

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

# Running a FHIR-compatible server

```shell
clj -M:fhir-r4 /var/local/ods-2021-02 /var/local/nhspd-2020-11 8080
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

Build an uberjar and run it.

```shell
clj -X:uberjar
java -jar target/clods-full-v0.1.0.jar --help
```

Copyright © 2020-21 Eldrix Ltd and Mark Wardle
