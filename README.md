# clods

A web service and set of tools for manipulating UK organisational data services.

This is designed to provide "location" services down to the granularity of an organisational site
with more finely-grained location services (e.g. ward, bed) provided by other modules
as part of a unified concierge location service.

This service can provides a simple REST-based service or through a graph-like API.

## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

## Running

To start a web server for the application, run:

    lein ring server

## License

Copyright © 2020 Eldrix Ltd and Mark Wardle
