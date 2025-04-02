.. toctree::
   :maxdepth: 3
   :caption: Authors:


Authors & Citations
===================

Development leads
^^^^^^^^^^^^^^^^^

* Oleg Smirnov <osmirnov@gmail.com>
* Sphesihle Makhathini <sphemakh@gmail.com>

Contributors
^^^^^^^^^^^^

* Jonathan Kenyon
* Landman Bester
* Simon Perkins
* Benjamin Hugo
* Athanaseus Ramaila
* Lexy Andati

Citations
^^^^^^^^^

To reference Stimela in a scholary work, please cite our `reference paper <https://doi.org/10.1016/j.ascom.2025.100959>`_. Here is a BibTeX entry: 

.. code-block:: latex

   @article{Stimela2,
      title = {Africanus IV. The Stimela2 framework: Scalable and repeatable workflows, from local to cloud compute},
      journal = {Astronomy and Computing},
      volume = {52},
      pages = {100959},
      year = {2025},
      issn = {2213-1337},
      doi = {https://doi.org/10.1016/j.ascom.2025.100959},
      url = {https://www.sciencedirect.com/science/article/pii/S2213133725000320},
      author = {O.M. Smirnov and S. Makhathini and J.S. Kenyon and H.L. Bester and S.J. Perkins and A.J.T. Ramaila and B.V. Hugo},
      keywords = {Standards – techniques, Interferometric – Computer systems organization, Pipeline computing – Software and its engineering, Data flow architectures – Software and its engineering, Cloud computing – Software and its engineering, Interoperability},
      abstract = {Stimela2 is a new-generation framework for developing data reduction workflows. It is designed for radio astronomy data but can be adapted for other data processing applications. Stimela2 aims at the middle ground between ease of development, human readability, and enabling robust, scalable and repeatable workflows. Stimela2 defines a YAML-based domain specific language (DSL), which represents workflows by linear, concise and intuitive YAML-format recipes. Atomic data reduction tasks (binary executables, Python functions and code, and CASA tasks) are described by YAML-format cab definitions detailing each task’s schema (inputs and outputs). The Stimela2 DSL provides a rich syntax for chaining tasks together, and encourages a high degree of modularity: recipes may be nested into other recipes, and configuration is cleanly separated from recipe logic. Tasks can be executed natively or in isolated environments using containerization technologies such as Apptainer. The container images are open-source and maintained through a companion package called cult-cargo. This enables the development of system-agnostic and repeatable workflows. Stimela2 facilitates the deployment of scalable, distributed workflows by interfacing with the Slurm scheduler and the Kubernetes API. The latter allows workflows to be readily deployed in the cloud. Previous papers in this series used Stimela2 as the underlying technology to run workflows on the AWS cloud. This paper presents an overview of Stimela2’s design, architecture and use in the radio astronomy context.}
   }