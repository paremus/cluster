# The Paremus Gossip Repository

This repository contains components used to build large-scale clusters.

The primary (and currently only) implementation uses a "gossip style" infection algorithm. The components here provide the basic functionality for assembling a cluster, discovering new, departing, and updated cluster members, and for sharing small amounts of data (tens of bytes)

## Repository Contents

This repository contains:

### com.paremus.cluster.api

This project provides the Paremus Clustering API

### com.paremus.gossip.netty & com.paremus.gossip.netty.test

The `com.paremus.gossip.netty` component is an implementation of the clustering API which implements a gossip algorithm using netty to provide an asynchronous communications layer. Security is achieved using TLS and DTLS via the `com.paremus.core.netty.tls` component.

The integration tests for the gossip netty component demonstrate the use of the service, and validate the ability to form gossip clusters

Note that this implementation makes use of functions from the [Netty](https://netty.io) project

# How to build this repository

This repository can be built using Maven 3.5.4 and Java 9. The output bundles will work with Java 8, however DTLS 1.2 support is only available within the JDK since Java 9. On Java 8 the bouncy castle DTLS provider must be used instead. 

## Build profiles

By default the build will run with all tests, and lenient checks on copyright headers. To enable strict copyright checking (required for deployment) then the `strict-license-check` profile should be used, for example

    mvn -P strict-license-check clean install

If you make changes and do encounter licensing errors then the license headers can be regenerated using the `generate-licenses` profile

    mvn -P generate-licenses process-sources
