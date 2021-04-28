Yamcs: Prometheus Plugin
========================

This is a Yamcs plugin for monitoring Yamcs performance using Prometheus.

More specifically this plugin adds an endpoint to the Yamcs HTTP API that can
be scraped by Prometheus or any other monitoring product supporting the
Prometheus metrics format (e.g. Zabbix).

Current exposed metrics include:

* Process-specific metrics (fds, start time, cpu time)
* Hotspot JVM metrics (GC, memory pools, classloading, thread counts)
* Version info of Yamcs and available plugins
* Yamcs Server ID
* Instance counts (total, and by-state)
* Link counters (in/out)
* Packet counters
* API request/error counters

The metrics output is human-readable and annotated with comments for each
metric.


.. rubric:: Usage with Maven

Add the following dependency to your Yamcs Maven project. Replace ``x.y.z`` with the latest version. See https://mvnrepository.com/artifact/org.yamcs/yamcs-prometheus

.. code-block:: xml

   <dependency>
     <groupId>org.yamcs</groupId>
     <artifactId>yamcs-prometheus</artifactId>
     <version>x.y.z</version>
   </dependency>


.. toctree::
    :titlesonly:
    :caption: Table of Contents

    scrape-config
    http-api/index
    example-scrape
