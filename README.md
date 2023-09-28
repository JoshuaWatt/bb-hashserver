Bitbake Hash Equivalence Server
===============================

This is an alternate implementation of the Hash Equivalence server that can be
used by bitbake. While the reference implementation included in bitbake is
sufficient for many use cases, it focuses on correctness and minimal
dependencies. As such, there are some features that are not appropriate for it
to implement. This implementation is designed as an alternative server that can
have more features since it drops some of the restrictions of the bitbake
internal implementation.

Specifically, this implementation:
1. Uses websockets as the transport layer for compatibility with modern
   web-based services
2. Utilizes SQL Alchemy for the database implementation so it can work with
   almost any SQL database backend
3. Is designed to be stateless so that multiple instances can be run for
   load-balancing/redundancy
4. Provides Docker images for easy integration into K8s et. al. (TODO: images
   still need to be built & published)
5. Authentication (See below)
6. Role Based Access Control (TODO: Not implemented yet)

Websockets are chosen as transport mechanism because it makes it easy to run
the server on typical web service infrastructure (e.g. K8s), which provides a
number of "free" features, such as SSL support and authentication support via
the cluster ingress

Quickstart Guide
----------------

This repository provides a `docker-compose.yaml` file to make it easy to
quickly spin up an instance of the server with a postgesql database backend. To
start the server, simply the following command in this directory:

    docker-compose up

Then, configure bitbake to use the this running server by adding the following
to your `local.conf` file:

    BB_HASHSERVE = "ws://localhost:9000"
