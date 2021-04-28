Get Metrics
===========

Exposes Yamcs server metrics in Prometheus format.

.. rubric:: URI Template

.. code-block::

    GET /api/prometheus/metrics

.. note::

   For convenience, this plugin also binds to the ``/metrics`` endpoint.
   
   This is the default location that Prometheus uses for configured targets.
   This will work, but it is just a redirect to ``/api/prometheus/metrics``. If
   you want to reduce access logs, use the more specific endpoint.


.. rubric:: Security

If your installation is password-protected, you will need to make a password-based user to be used by
Prometheus.

Authenticated users require the privilege: ``Prometheus.GetMetrics``.
