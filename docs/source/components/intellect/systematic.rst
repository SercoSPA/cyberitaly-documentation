.. _systematic:

Systematic Processing
=====================

The systematic processing is an automated, regular processing of satellite data to generate value-added products. This process is triggered by the availability of new data.

Prerequisites
+++++++++++++

The systematic processing is only available to users with role *Expert User*.

Accessing the feature
+++++++++++++++++++++

* Visit <base_url>/intellect.
* Click on *Process some data*.
* Identify the card *Systematic processing*.
* Click on *Explore services* under that card.
* Select one of the available services.

  * Note that systematic processing is only available for services that have at least one input from a Catalogue.

Input criteria
++++++++++++++

The systematic processing allows you specifying *criteria* for the input satellite data selection, instead of product identifiers.
For example:

* collection
* processing level or other specification of the satellite data to use
* start and end of the processing

  * the end date is optional, in case you need to process data forever in the future
* area of interest
* product identifier
* other filtering parameters (e.g., cloud coverage) depending upon the collection

.. image:: ../../images/intellect_systematic_1.png
   :alt: Insula Intellect Systematic Processing

Operating mode
++++++++++++++

Once this service is running:

1. It will periodically search for data that matches the criteria
2. If it finds any, then it will launch a job to process the new data using the specified service, making the output available in the same way as for any other job. This means that once the job is started, there might first be a bulk processing of any historic data, then periodic processing of any new data that arrives.

If an end date is specified then no data will be processed after this date, otherwise the processing will continue.

Usage credits
-------------

Any processing will be conditional upon the user having a sufficient credit. If the user's balance expires then the processing will be paused, then resume once the balance has been augmented.
At the moment the system does not foresee any warning when the user's credit is about to expire.
