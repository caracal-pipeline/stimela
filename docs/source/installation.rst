.. highlight: yml
.. _installation:

Installing and running stimela
##############################


Stimela requires Python 3.7 or higher. For containerization support (WIP), you'll also need Singularity installed. You can still run stimela in "native" mode without Singularity.

Installing from PyPI
====================


Stimela is available from PyPI, thus you can simply do (possibly, in a virtual environment)::
    
    $ pip install stimela

A companion package called ``cult-cargo`` contains standard, curated :ref:`cab definitions <basics>` for popular radio astronomy packages. It is technically independent of stimela, but for any practical radio work you'll probably want to install it::

    $ pip install cult-cargo

Installing from github
======================

Developers and bleeding-edge users may want to run stimela directly off the repository. In this case, install as follows::


    # create and activate virtualenv
    ...
    $ pip install poetry
    $ gh repo clone caracal-pipeline/stimela
    $ cd stimela
    $ poetry install
    $ cd ..
    $ gh repo clone caracal-pipeline/cult-cargo
    $ cd cult-cargo
    $ poetry install


Running stimela
===============

The main function of stimela is to run workflows defined by :ref:`recipes <basics>`. Recipes come as YaML files. 
You can ask stimela to run a recipe as follows::

  $ stimela run recipe.yml [recipe_name] [foo=x bar=y baz.qux=z]

A ``recipe_name`` is only needed if the YaML file contains more than one recipe (one can also pass the ``-l/--last-recipe`` flag to select the *last* recipe in the file).

Arguments such as ``foo=x`` can be used to specify recipe parameters. An alternative form is ``-a/--assign foo x`` (useful because it allows shell tab-completion on values.)

Getting help
============

Use ``stimela --help`` to get overall help and a list of commands. To get help on a particular command, run e.g.::

    $ stimela run --help

Recipes (and things called :ref:`cabs <cabdef>`) can come with their own embedded documentation. You can access this by running::

    $ stimela doc recipe.yml [anything]

...where ``anything`` can be a recipe name or a cab name.




