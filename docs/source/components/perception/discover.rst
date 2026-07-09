.. _discover:

Data discovery
==============

You can activate the Data discovery by just clicking the magnifier lens icon in the top right bar:

.. image:: ../../images/perception_4.png
   :alt: Insula Perception
   :width: 30%

After clicking on it, it opens a panel on the right side, with the following entries:

* Catalog, to access the catalog(s) of products available.
* Events, to access products produced by the :ref:`events`.
* My storage, to access products organized in My Bookmarks, Processing outputs, and Uploaded data.
* External WMS, to access data served my external services exposing a `Web Map Service (WMS) <https://www.ogc.org/standard/wms/>`_ interface, such as for example the `NASA GIBS <https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/gibs>`_.

.. image:: ../../images/perception_5.png
   :alt: Insula Perception
   :width: 30%

Catalog
+++++++

Catalog provides access to the following groups of collections:

* *Open Data*, containing open data collections. *Note that this group might not be available*.
* *Reference*, containing reference and in situ data uploaded by users on :ref:`awareness` or :ref:`perception`
   * Both :ref:`awareness` and :ref:`perception` allow users to upload products. For more information, see :ref:`uploaded_data` (Awareness) and :ref:`perception_my_storage` (Perception).
   * *Reference data* is also referred as *Uploaded data*, but it has the same meaning. 
* *Output products*, containing outputs of services generated on :ref:`intellect`.

.. _reference_data:

Reference Data
--------------

Reference data provides access to user uploaded data (e.g., in-situ data). These can be searched according to the following criteria:

* *Collection* (required), the collection (or folder) in which the outputs have been grouped.
* *Owner* (optional), the id of the owner of the product.
* *Product date* (optional), the date at which the data is valid, e.g. the measurement data for an in situ measurement.
* *Publication date* (optional), the date when this data was uploaded.
* *Area of Interest (AOI)* (optional), this can be drawn directly on the geobrowser (as box or polygon), or using a previously saved area, or by importing a shapefile.
* *Identifier* (optional), a string identifying the product.

Note that only data which have been made visible (i.e., shared) by their owner will appear in the search results.

.. image:: ../../images/perception_7.png
   :alt: Insula Perception
   :width: 50%


Output Products
---------------

:ref:`intellect` produces the products in this group. They can be filtered for according to the same criteria as the :ref:`reference_data`.

Note that only data which have been made visible (i.e., shared) by their owner will appear in the search results.

Events
++++++

Events provides access to products produced by the :ref:`events`. *Stay tuned, more information will come on this topic!*

.. _perception_my_storage:

My storage
++++++++++

My storage in Insula Perception provides a quick access to your storage, with the following storage types:

* *My Bookmarks*. For more information, see :ref:`bookmarks`.
* *Processing outputs*. For more information, see also :ref:`processing_outputs`.
* *Uploaded data*. For more information, see also :ref:`uploaded_data`.

Some features, like uploading, downloading or removing data are available in Insula Perception.

For the other features related to your storage, and further details for each storage type, please refer to :ref:`awareness`.

*TIP:* My storage in Insula Perception allow to visualise the content of your storage in the map. For more details, see :ref:`visualise`.

External WMS
++++++++++++

External is a way to access external data through Catalog exposing the `WMS interface <https://www.ogc.org/standard/wms/>`_.
Currently, it is supported the `NASA GIBS <https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/gibs>`_.

More information about how to leverage External WMS is available in :ref:`visualise`.

.. image:: ../../images/perception_8.png
   :alt: Insula Perception
   :width: 50%
