.. _intellect:

CyberItaly data processing management and development
================

*CyberItaly data processing management and development* is available at https://iride-cyberitaly.space/intellect.

CyberItaly data processing management and development provides a way to:

* Discover,
* Execute,
* Monitor, and
* Develop processing services (Digital Twin processors).

A processing service is a service that takes as an input data (typically Earth Observation data) and generates some added value data, extracting information from the original inputs. This process might increase the level of knowledge for a particular domain, case study or knowledge area.

CyberItaly data processing management and development supports different execution modes:

- Standard: the user selects the input data and start the processing. This is the most common and option for users.
- Systematic: the user selects criteria for the input selection. It enables bulk processing. *Only available to users with role Expert User*.
- Event driven: the user selects criteria for the input selection and the set of services to trigger. It enables bulk processing. *Only available to users with role Expert User*.

A user visiting CyberItaly data processing management and development will find an interface similar to:

.. image:: ../../images/intellect_1.png
   :alt: CyberItaly data processing management and development

It provides the options:

* Process some data, that is a way to select and run the available services.
* Monitor you processing, to monitor the execution of the services and access the produced results.
* Integrate your own algorithm, to develop new services and eventually made them available to the user community. *Only available to users with role Expert User*.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   process
   systematic
   event
   monitor
   develop
   services/index
