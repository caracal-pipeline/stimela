
===========
stimela 2.x
===========


|Pypi Version|
|Python Versions|

A workflow management framework for radio interferometry data processing pipelines.

`Documentation page <https://stimela.readthedocs.io/>`_

`Reference paper <https://doi.org/10.1016/j.ascom.2025.100959>`_


Installation - User
-------------------

Stimela can be installed using ``pip``. Simply run ``pip install stimela``.

Installation - Developer
------------------------

`uv <https://docs.astral.sh/uv/>`_ - ``uv`` managed environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After cloning the repo, install with ``uv sync --group dev``. Then run ``uv run pre-commit install`` to set up the pre-commit hooks. By default, you should end up with a correctly configured environment in ``.venv``. `ruff <https://docs.astral.sh/ruff/>`_ (linter and code formater) can be invoked manually with ``uv run ruff check`` and ``uv run ruff format``.

``uv`` - ``uv pip`` managed environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After cloning the repo, create a virtual environment with ``uv venv -p {python_version} path/to/env``. Activate the environment and install with ``uv pip install -e . --group dev``. Then run ``pre-commit install`` inside the environment to set up the pre-commit hooks. ``ruff`` can be invoked manually with ``ruff check`` and ``uv run ruff format``.

``pip`` managed environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

After cloning the repo, create a virtual environment with ``virtualenv -p {python_version} path/to/env``. Activate the environment and install with ``pip install -e . --group dev``. Then run ``pre-commit install`` inside the environment to set up the pre-commit hooks. ``ruff`` can be invoked manually with ``ruff check`` and ``uv run ruff format``.

.. |Pypi Version| image:: https://img.shields.io/pypi/v/stimela.svg
                  :target: https://pypi.python.org/pypi/stimela
                  :alt:


.. |Python Versions| image:: https://img.shields.io/pypi/pyversions/stimela.svg
                     :target: https://pypi.python.org/pypi/stimela
                     :alt:
