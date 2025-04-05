.. highlight: yml
.. _clickify:


Clickify parameters
===================


For any given command-line tool, most of the information in the cab schema (i.e. argument names and types, help strings) directly mirrors that already provided to the tool's command-line parser. When wrapping a third-party package in a cab, this leads to an unavoidable duplication of effort (with all the attendant potential for inconsistencies) -- after all, the package developer has already implemented their own command-line interface (CLI) parser, and this CLI needs to be described to Stimela. Note, however, that the schema itself provides all the information that would be needed to construct a CLI in the first place. For newly-developed packages, this provides a substantial labour-saving opportunity. Stimela includes a utility function that can convert a schema into a CLI using the `click <https://click.palletsprojects.com>`_ package. For a notional example, consider this 
``hello_schema.yml`` file defining a simple schema with two inputs::

    inputs:
        name: 
            dtype: str
            info: Your name
            required: true
            policies:
                positional: true
            
        count:
            dtype: int
            default: 1
            info: Number of greetings

This file can be instantly converted into a CLI as follows:

.. code-block:: python

    #!/usr/bin/env python
    import click
    from scabha.schema_utils import clickify_parameters

    @click.command()
    @clickify_parameters("hello_schema.yml")
    def hello(count, name):
        """Simple program that greets NAME for a 
            total of COUNT times."""
        for x in range(count):
            print(f"Hello {name}!")

    if __name__ == '__main__':
        hello()

The resulting tool now has a fully-functional CLI:

.. code-block:: none

    $ ./hello.py --help
    Usage: hello.py [OPTIONS] NAME

    Simple program that greets NAME for a total 
    of COUNT times.

    Options:
    --count INTEGER  Number of greetings
    --help           Show this message and exit.


To integrate the tool into Stimela, all we need is a cab definition, which can directly include the schema file::

    cabs:
        hello:
            _include: hello_schema.yml
            command: hello.py


This mechanism ensures that all inputs and outputs need only be defined by the developer once, in a single place -- and provides both a CLI and Stimela integration with no additional effort, while ensuring that these 
are mutually consistent by construction. The `QuartiCal <https://quartical.readthedocs.io/en/latest/>`_, `pfb-imaging <https://github.com/ratt-ru/pfb-imaging>`_ and  `breizorro <https://github.com/ratt-ru/breizorro>`_ packages, for example, make extensive use of this.

In the above example, ``clickify_parameters()``  is passed a filename to read the schema from. An alternative to this is to pass it a Dict containing ``inputs``, ``outputs`` and (optionally) ``policies`` sections (see :ref:`policies_reference`). One can also pass a second argument containing a Dict of policies that will override the policies in the first Dict. This is useful when you ship a package containing full cab definitions, and want to read the schemas directly from the latter. Here we combine it with click's subcommand feature:

.. code-block:: python
    import click
    from scabha.schema_utils import clickify_parameters
    from omegaconf import OmegaConf

    schemas = OmegaConf.load(os.path.join(os.path.dirname(__file__), "cabs/mypackage.yml"))

    @cli.command("hello",
        help=_schemas.cabs.get("hello-world").info,
        no_args_is_help=True)
    @clickify_parameters(_schemas.cabs.get("hello-world"))
    def hello_world(name, count):
        for x in range(count):
            print(f"Hello {name}!")

where ``mypackage.yaml`` contains::

    cabs:
        hello-world:
            info: Greets NAME for a total of COUNT times
            inputs:
                name: 
                    dtype: str
                    info: Your name
                    required: true
                    policies:
                        positional: true
                    
                count:
                    dtype: int
                    default: 1
                    info: Number of greetings

If your package defines multiple commands, it can be useful to create a new decorator that you can then reuse for multiple functions:

.. code-block:: python

    import click
    from scabha.schema_utils import clickify_parameters
    from omegaconf import OmegaConf

    def clickify(command_name, schema_name=None):
        schema_name = schema_name or command_name
        return lambda func: \
            cli.command(command_name, help=schemas.cabs.get(schema_name).info, no_args_is_help=True)(
                    clickify_parameters(schemas.cabs.get(schema_name))(func)
            )

    @clickify("hello", "hello-world"):
    def hello_world(name, count):
        for x in range(count):
            print(f"Hello {name}!")

