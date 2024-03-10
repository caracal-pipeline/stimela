.. highlight: yml
.. _schema_reference:


Parameter schema reference
===========================

The basics of parameters and schemas are discussed in :ref:`params`. In this section, we document the more obscure details of IO parameter definitions (aka *schemas*). Refer also to `comments in the source code <https://github.com/caracal-pipeline/stimela/blob/3a74f8acbb93e2594a47f08ea83a5592aec96e43/scabha/cargo.py#L98>`_ for more information.


Basic schema attributes
-----------------------


``dtype``: data types
^^^^^^^^^^^^^^^^^^^^^

The ``dtype`` attribute of the schema determines the data type of the parameter. This generally follows the Python `typing <https://docs.python.org/3/library/typing.html>`_ module syntax. Basic types such as ``str``, ``int``, ``float``, ``bool``, as well as compound types such as ``List``, ``Tuple``, ``Dict``, ``Union`` and ``Optional`` are recognized. In addition, Stimela defines the ``File``, ``Directory``, ``MS`` and ``URI`` types. The latter two refer to Measurement Sets and uniform record identifiers (for Dask-ms based tools that support both traditional MSs and, e.g., S3-backed storage.)

The default dtype is a ``str``. 

Some more terminology is in order. The term *cargo* refers to the underlying tool for which the interface is being defined. The cargo can be a sub-recipe, or a cab wrapping an underlying piece of software:

* *File-type* inputs and outputs refer to files, directories, Measurement Sets, URIs. All of these are associated with some kind of persistent stored object. 
  
* All other inputs/outputs are called *value-type* (e.g. ``str``, ``int``, and the like.) 

* *Named outputs* are file-type outputs for which the underlying software package offers control over how the file is named (think of a command-line tool with an ``-o output_filename`` option.) In a way, a named output is *sort of* an input as well, in the sense that the output filename must be supplied to the cab.

* *Implicit outputs* are file-type outputs named by the underlying package automatically. This can be a fixed, predefined output filename, or a filename derived from a string-valued input parameter (think of how `WSClean <https://wsclean.readthedocs.io/>`_ names all its output files based on the ``-name`` parameter as a filename prefix.)

``default`` and ``required``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``default`` attribute can be used to provide a default value for an input (or a named output.) If a default is not supplied, the parameter is treated as optional, unless marked with a ``required: true`` attribute.

For outputs, ``required`` has a slightly different meaning. Stimela will normally check that the cab has produced the expected file-type outputs, and flag up an error if it hasn't. However, ``required: false`` may be used to mark optional outputs, which do not necessarily need to exist at the end of the run.

Note that Stimela also recognized an alternative way to specify default values via a separate ``defaults`` section::

    inputs:
        foo:
            dtype: str
        bar:
            dtype: int
            default: 0
    defaults:
        foo: "foodef"
    



Shorthand schemas
-----------------

The above attributes, as well as the optional ``info`` field (basically, just a help string), can also be specified using the **shorthand schema** syntax::

    inputs: 
        foo:
            dtype: str
            default: "foodef"
            required: true
            info: "this is foo"
        bar:
            dtype: List[File]

is equivalent to::

    inputs:
        foo: str = "foodef" * "this is foo"    # '*' indicates required: true
        bar: List[File]                        # dtype only -- default and info are optional

If any additional schema attributes need to be specified, you must use the normal longhand (structured) syntax.


Hierarchical schemas
--------------------

Schemas (both shorthand and structured) can be arranged inside nested mappings, for example::
    
    inputs:
        foo:
            x: str = "foodef" * "this is foo"    # '*' indicates required: true
            y: 
                dtype: int
        bar: List[File]

defines inputs called ``foo.x``, ``foo.y``, and ``bar``. Stimela can usually infer whether a nested mapping is a subgroup of schemas or a single schema (just please don't go naming an input something confusing like ``dtype``, as that could break this logic.)

Implicit parameters
-------------------
.. _implicit_params:

*Implicit* parameters (indicated by providing an ``implicit: value`` attribute) are not exposed as part of the external interface, but are nonetheless passed to the cargo. 

Implicit inputs have a value set by the schema (though this is not necessarily fixed, as it is subject to :ref:`subst`). A simple example is `given here <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/casa-flag.yml#L41>`_, where the ``mode`` parameter of the CASA ``flagdata`` task is fixed for this particular cab.

Implicit outputs typically arise when the cargo has a file-type output named automatically (usually based on an input parameter -- this is where substitutions are particularly useful.) WSClean provides a `typical example <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/genesis/wsclean/wsclean-base.yml#L170>`_.



Other schema attributes
-----------------------

Choices
^^^^^^^

The ``choices`` attribute makes the input a choice-type parameter, i.e., only specific listed values are allowed.

The ``element_choices`` attribute has a similar effect for parameters of type ``List[X]``, but restricts the choices for the elements of the list.

Aliases
^^^^^^^

The ``aliases`` attribute describes the aliases of a recipe-level parameter. See :ref:`aliases` for an extended discussion of this.

The ``nom_de_guerre`` attribute relates to a totally different kind of aliasing. If, for some reason, you want to name the parameter **differently** from the actual command-line option (or function argument) of the underlying cargo, you can use ``nom_de_guerre`` to specify the "internal" underlying name. For example, many of the ``cult-cargo`` CASA-based cabs 
`use something like this <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/casa-flag.yml#L33>`_::

    ms:                      # the parameter of the cab is called 'ms'
        dtype: MS
        nom_de_guerre: vis   # the parameter of the underlying CASA task is called 'vis'

Obviously, this feature ought to be used sparingly, and then only with very good reason. Users making the transition to Stimela may remember the command-line interface (CLI) of their favourite packages by heart -- keeping parameter names consistent is helpful, while the gratuitous renaming of parameters can be actively irritating. (In the example above, this is outweighed by the ``cult-cargo`` convention of using ``ms`` for the input Measurement Set across all tools. Whether this is a good enough reason remains to be seen.)



File-related attributes
^^^^^^^^^^^^^^^^^^^^^^

A number of attributes can be used to modify the behaviour of Stimela with respect to file-type parameters:

* ``writable: true`` will mark an input as read/write, aka input/output (for example, think of a Measurement Set that is both read and written to by the underlying tool).

* ``mkdir: true`` will tell Stimela to create the parent directory(ies) of an output, if it doesn't exist

* ``access_parent_dir: true`` tells Stimela that the cargo needs to access the parent directory of the object (and write to it, if ``write_parent_dir: true`` is set). This is meant for the (not uncommon) scenario where tools want to create intermediate or scratch files in the same directory as their inputs -- Stimela needs to be aware of this, as its container-based :ref:`backends <backend_reference>` are pretty strict about allowing access to the underlying filesystem.

* ``must_exist`` changes the default file existence check logic. By default, input files **must** exist at the start of the run, while output files **don't** have to exist at the end of the run. This logic may be flipped by setting ``must_exist: false`` in the former case, and ``must_exist: true`` in the latter case.

* ``skip_freshness_checks: true`` omit this parameter from ``skip_if_outputs: fresh`` logic (see :ref:`skips`.) For inputs, this implies omitting the file from the "most recent input" calculation. For outputs, this implies ignoring the freshness of the output.

* ``remove_if_exists: true``: remove existing output file before running the cargo.

Attributed related to the command line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``policies`` attribute is an entire (optional) sub-section describing how the parameter is converted into a command-line argument accepted by the underlying package. Obviously, this is only relevant to cabs that wrap external software -- see :ref:`policies_reference` for details.

Two other attributes of the schema are ignored by Stimela per se, but are used by the ``scabha.schema_utils`` module. The latter provides some tools for constructing CLIs from Stimela-style schemas. The advantage of using ``schema_utils`` over a standard CLI package such as `argparse <https://docs.python.org/3/library/argparse.html>`_ or `click <https://click.palletsprojects.com/en/8.1.x/>`_ is that both a Stimela cab definition and a CLI can be constructed from the same underlying YaML, eliminating duplication of effort (see e.g. `pfb-clean <https://github.com/ratt-ru/pfb-clean>`_ and `QuartiCal <https://github.com/ratt-ru/QuartiCal>`_).

The ``metavar`` attribute has the `same meaning as in argparse <https://docs.python.org/3/library/argparse.html#metavar>`_, and only affects the help strings. The ``abbreviation`` attribute is used to specify a shorthand version of the corresponding CLI option. For example::

    inputs:
        input-file:
            dtype: File
            required: true
            metavar: FILENAME
            abbreviation: f
            info: this is the input filename

tells ``schema_utils`` that this parameter needs define both an ``-f`` and an ``--input-file`` option. Its ``--help`` will print something like
    
    ``-f/--input-file FILENAME        this is the input file``


Informational attributes
^^^^^^^^^^^^^^^^^^^^^^^^

The following attributes are defined for informational purposes only:

* ``tags`` may be set to an arbitrary list of tags. The intended purpose of this is to logically group related parameters together. At present, Stimela doesn't use this information.

* ``metadata``: can be used to add an arbitrary mapping of user-defined metadata. At present, Stimela doesn't use any of this information.

* ``category`` defines the category of the parameter, and can be set to one of ``Required``, ``Optional``, ``Implicit``, ``Obscure`` or ``Hidden``. This determines at which level of detail ``stimela doc`` documents the parameter (see ``stimela doc --help``). 

  Stimela will normally categorize a parameter automatically -- the first three categories are directly derived from the schema, while the "obscure" and "hidden" categories arise when :ref:`automatic step aliases <auto_aliases>` are created. This attribute can be used to override the automatic classification.






