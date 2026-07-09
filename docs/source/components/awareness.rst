.. _awareness:

Insula Awareness
================

Insula Awareness is available at <base_url>/awareness and allows you to:

* Manage your storage and sharing options.
* Manage your own account details, including quotas and transactions.

When accessing Insula Awareness, it provides a screen similar to:

.. image:: ../images/awareness_1.png
   :alt: Awareness

Manage my storage
-----------------

It is a way to explore the available storage options, that is:

* My Bookmarks, a way to organize and keep track of heterogeneous data.
* Processing Outputs, the output of processing services.
* Uploaded Data, collections of custom data (e.g., in-situ data), uploaded by the users.

To access the service:

* Click on *Open my storage page*.
* See the different options available (see image below).

.. image:: ../images/awareness_2.png
   :alt: Awareness

.. _bookmarks:

My Bookmarks
++++++++++++

My Bookmarks is a way to organize and keep track of heterogeneous data. You can think about them as a folder of *references* or links to data.
The following is an example:

.. image:: ../images/awareness_3.png
   :alt: Awareness

The bookmark *Global Temperature* in the example above contains references to:

* an OUTPUT_PRODUCT (i.e., the result of a processing service),
* an EXTERNAL_PRODUCT (i.e., remote data contained in a Data Catalog),
* a REFERENCE_DATA (i.e., data uploaded by the user).

The bookmark itself can be shared with other users setting the proper permissions. For example:

.. image:: ../images/awareness_4.png
   :alt: Awareness
   :width: 50%

*TIP:* A bookmark can be used as an input of a compatible processing service, to process an arbitrary set of data (e.g., sparse data from a Data Catalog).

.. _processing_outputs:

Processing outputs
++++++++++++++++++

This type of storage is related to the outputs of processing services. It is a set of collections of data, or folders.

Insula Awareness provides a way to manage these collections, in particular:

* to share them, similarly to :ref:`bookmarks`,
* to set a download cost. This is a particularly useful option when a dataset is shared within Insula, and the owner wants to decide the cost of downloading its content (*please note that no revenue models are currently supported*), 
* to download data from the collection,
* to remove data from the collection.

.. image:: ../images/awareness_5.png
   :alt: Awareness

.. _uploaded_data:

Uploaded data
++++++++++++++

You can upload data on the platform and use it for visualisation or processing purposes.
Similarly to :ref:`processing_outputs`, these data is organized in collections, or folders, and Insula Awareness allows:

* to share them, similarly to :ref:`bookmarks`,
* to set a download cost, as for :ref:`processing_outputs`,
* to download data from the collection,
* to remove data from the collection.

Restrictions on filenames:

* any file with a valid Unix filename can be uploaded to Insula, except those containing the following characters: ", $, :, <, >
* using these characters may lead to unexpected behavior during the file’s lifecycle.

When uploading data, it is possible to define additional metadata to them, such as:

* :ref:`filetype`
* Title
* Description
* Additional metadata, that is a JSON object with arbitrary information
* Geometry
* Time

To upload data:

* Create a folder, if it does not exist already. Click on the *New folder* button (the one with the '+' sign).
   * Provide *Name*, *Description*, and *Data type*. *Data type* can be *Generic* or *Raster mosiac*. In the latter case, compatible data will be stored in an `ImageMosaic <https://docs.geoserver.org/latest/en/user/data/raster/imagemosaic/index.html>`_. This could be useful to assemble a set of overlapping geospatially rectified images into a contiguous image.
* Within the folder, click on the *Upload data* button.
* Drag and drop a file or click to select one from your local filesystem.
* Define additional metadata, if needed. See the example below.

.. image:: ../images/awareness_6.png
   :alt: Awareness
   :width: 50%

Discover uploaded data using Insula Perception
**********************************************

As in the previous example we provided two essential metadata for the data discovery, *time and spatial information*, data can be easily discoverable in :ref:`perception`. For example, here below you can see how the data jsut uploaded can be discovered and visualised:

.. image:: ../images/awareness_7.png
   :alt: Awareness (using Perception)
   :width: 30%

.. image:: ../images/awareness_8.png
   :alt: Awareness (using Perception)
   :width: 50%

.. _filetype:

File type
*********

File type determines the way Insula will process the uploaded data. The available options are:

* *GeoTiff*. In such a case Insula Awareness will try to automatically extract the available information from this type of *raster* data, and ingest it for visualisation, storing it as a `Layer <https://docs.geoserver.org/latest/en/user/data/webadmin/layers/index.html>`_.
* *Shapefile (zip)*. In such a case Insula Awareness will try to automatically extract the available information from this type of *vectorial* data, and ingest it for visualisation, storing it as a `Layer <https://docs.geoserver.org/latest/en/user/data/webadmin/layers/index.html>`_.
* *Mosaic*. In such a case Insula Awareness will try to automatically extract the available information from the data, and ingest it for visualisation, storing it in an `ImageMosaic <https://docs.geoserver.org/latest/en/user/data/raster/imagemosaic/index.html>`_.
* *Other*. Insula Awareness will not try to extract any information from this file type.


Manage my account
-----------------

*Manage my account* allows to get details about:

* Your account
* Your quotas and subscriptions
* Your transactions
* Your groups

My profile
++++++++++

* From the Insula Awareness landing page, cLick on *Open my account page*,
* Click on *My profile*,
* See a page similar to:

.. image:: ../images/awareness_9.png
   :alt: Awareness

Account info
************

This section provides an overview of your account details:

* Username: your unique identifier associated with your account.
* Role: your role, determining the privileges within :ref:`perception`, :ref:`intellect`, and :ref:`awareness`.
* ID: your unique integer identifier associated with your account. This might be useful when using the API (*not supported at the moment*)
* Credits Balance: representing your available credits for using certain features.

Quota and Subscriptions
***********************

This section provides information about your storage quota and any active subscriptions.

* Used storage: Provide details on your storage limitations, e.g., total space allocated or remaining space.
* Max runnable jobs: List any active subscriptions you have, e.g., for additional storage or premium features.
* Persistent storage folder will be soon deprecated.

Other features
**************

My profile offers also:

* Export Resource: This allows you to export your data for backup or accounting purposes.
* Transaction History: Here you can view your past transactions and track your credit usage.

My groups
+++++++++

*My groups* allows you to create, edit, and manage user groups within the application.

* From the Insula Awareness landing page, cLick on *Open my account page*,
* Click on *My groups*.

By selecting one of them, it is possible to add/remove users to the group or edit/delete it.

.. image:: ../images/awareness_10.png
   :alt: Awareness
