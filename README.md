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

You can use a pre-built jar file, or simply use from source code at the command-line. 
To do the latter, you'll need to install [clojure]

# Importing data

This service needs a directory on a filesystem to operate.

In these examples, we'll use `/var/tmp/ods-2021-01`

You can choose to use a single directory and update-in-place, or build a new
repository at intervals. I prefer read-only, immutable backing data by default,
so favour the latter.

You will need a TRUD API key. 
Login to the [NHS Digital TRUD](https://isd.digital.nhs.uk/) and find your API key under your profile. 
Write that key to a file and link to it from the command-line:

```shell
clj -M:download --api-key /path/to/api-key.txt --cache-dir /var/trud /var/tmp/ods-2021-01
```

The software will initialise a data repository at that location, and automatically
include the NHS postcode directory (NHSPD) and the NHS organisational data service. 

If you already have an NHSPD service available, you can instead provide a link
to that repository and save download bandwidth!

```shell
clj -M:download --nhspd /var/tmp/nhspd-2020-10 --api-key /path/to/api-key.txt --cache-dir /var/trud /var/tmp/ods-2021-01 
```

While you could embed all of this into a single Docker image for deployment, it might be better to 
instead link to a shared read-only filesystem and simply link to the latest backend data.

# Running a server

To run as a microservice,

```shell
clj -M:serve --port 8080 /var/tmp/clods-2021-01
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
