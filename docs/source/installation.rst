.. highlight: yml
.. _installation:

Getting started
###############

Stimela will run with Python 3.9 through 3.12. For containerization support, you'll also need `Singularity (Apptainer) <https://apptainer.org/>`_ installed. You can still run Stimela in "native" mode without Singularity. 

If you plan to use the Slurm and Kubernetes backends, you'll also need the `Slurm scheduler <https://slurm.schedmd.com/>`_ and/or the `Python Kubernetes client <https://pypi.org/project/kubernetes/>`_.

Installing from PyPI
====================

Stimela is available from PyPI, thus you can simply do (possibly, in a virtual environment)::
    
    $ pip install stimela

A companion package called ``cult-cargo`` contains standard, curated :ref:`cab definitions <basics>` for popular radio astronomy packages. It is technically independent of Stimela, but for any practical radio work you'll probably want to install it::

    $ pip install cult-cargo

Installing from github
======================

Developers and bleeding-edge users may want to run Stimela directly off the repository. In this case, install as follows::

    # create and activate virtualenv
    (venv) $ pip install -U pip
    (venv) $ gh repo clone caracal-pipeline/stimela
    (venv) $ pip install -e stimela
    (venv) $ gh repo clone caracal-pipeline/cult-cargo
    (venv) $ pip install -e cult-cargo


Running Stimela
===============

The main function of Stimela is to run workflows defined by :ref:`recipes <basics>`. Recipes come as YAML files. 
You can ask Stimela to run a recipe as follows::

  $ stimela run recipe.yml [recipe_name] [foo=x bar=y baz.qux=z]

A ``recipe_name`` is only needed if the YAML file contains more than one recipe (one can also pass the ``-l/--last-recipe`` flag to select the *last* recipe in the file). Note also that the ``.yml`` suffix is optional, and will be added implicitly if omitted.

Arguments such as ``foo=x`` can be used to specify recipe parameters. An alternative form is ``-a/--assign foo x`` (useful because it allows shell tab-completion on values.)

If you want to run just a single cab, use::

  $ stimela run cultcargo::wsclean ms=my.ms prefix=test size=1024 scale=1asec

This will load ``wsclean.yml`` from ``cult-cargo``, and invoke the ``wsclean`` cab defined within (because the document only defines one cab in this case -- otherwise an extra cab name argument would be needed), passing it the listed parameters. Use ``run -S`` to invoke the Apptainer/Singularity backend.


Getting help
============

Use ``stimela --help`` to get overall help and a list of commands. To get help on a particular command, run e.g.::

    $ stimela run --help

Recipes (and things called :ref:`cabs <cabdefs>`) can come with their own embedded documentation. You can access this by running::

    $ stimela doc recipe.yml [anything]

...where ``anything`` can be a recipe name or a cab name (provided that cab is loaded by the recipe). To get help on a cab provided by cult-cargo without specifying a recipe, use e.g.::

    $ stimela doc cultcargo::wsclean

This will load ``wsclean.yml`` from the ``cultcargo`` package, and print help on the content. To get a list of all the cabs defined in cultcargo, use:

.. code-block:: bash

    $ stimela doc cultcargo::

The trailing ``::`` tells Stimela to load all the YAML documents in the specified package. A wildcard form will give similar results (but will not include any subdirectories that cultcargo may excplicitly specify, such as ``./casa``)::

    $ stimela doc cultcargo::*.yml






