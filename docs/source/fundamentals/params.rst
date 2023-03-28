.. highlight: yml
.. _params:

Parameters and schemas
######################

Cabs and recipes (collectively known as *cargo*) correspond to some tasks, and tasks have well-defined inputs and outputs (IOs). 
These inputs and outputs are described by the *schema* of the cargo (in programming-speak, the schema is the function signature).
To invoke a task, a set of *parameters* must be supplied so that stimela can validate it against the 
inputs (in programming-speak, this is like calling a function with some arguments). Once the task is complete, stimela also 
validates its outputs against the output schema.

Here's an example of a notional cab schema::

    cabs:
        mycab:
            inputs:
                foo:
                    dtype: int
                    default: 0
                    info: "this is the foo parameter. It has a default"
                bar:
                    baz:
                        dtype: File
                        required: true
                        info: "this is the bar.baz parameter. It's required!"
                    qux:
                        dtype: File
                        info: "this is the bar.qux parameter. It's not required"
            outputs:
                numbers:
                    dtype: List[int]
                    info: "this is an output called 'result'. It is a list of integers"
                output-file:
                    dtype: File
                    required: false 

Here's what we can learn from the above:

* Inputs and outputs are specified in two separate sections. Each IO item is specified in its own subsection.

* IOs can include a help string (the ``info`` property).

* Schemas can be nested, grouping related IOs together. Nested IOs can be referred to using dot syntax, e.g. ``bar.baz`` and ``bar.qux``.

* IOs have a data type. This is specified using the standard Python type annotation syntax (see https://docs.python.org/3/library/typing.html). Stimela supports the full range of the annotation syntax, but also extends it with some extra types (``File``, ``Directory``, ``MS``). This permits very elaborate type specifications, e.g. (the not very useful, but possible)::

        Union[float, List[int], Tuple[File, File]]

* Inputs can have a default value, and can be marked as required. An input with no default and no ``required: true`` property is considered optional (conversely, a required input with a default is a tautology.)
 
* Outputs can be marked as *not* required. This is relevant for file-type outputs. Normally, stimela will check that output files exist after the task has completed, and will report an error otherwise. However, for ``required: false`` outputs, stimela will omit this check.


Shorthand schemas
-----------------

If you only need to specify type, default and info string, you can use a shorthand schema syntax, instead
of specifying a whole section longhand. Here is a shorthand version of the inputs above::

    inputs:
        foo: int = 0 "this is the foo parameter. It has a default"
        bar.baz: File * "this is the bar.baz parameter. It's required!"
        bar.qux: File "this is the bar.qux parameter. It's not required"

Note how ``*`` instead of ``=default`` indicates a required parameter. Note also how nested schemas can be specified with a dot-syntax, as an alternative to defining a subsection.


Named and implicit outputs
--------------------------

Consider the ``output-file`` output above. Let's say ``mycab`` is a command-line tool that generates an output file, and that the name of the file **must** be specified as ``-output-file NAME``. This is an example of a *named output*. Named outputs must be included in the parameters when validating a task (in some sense, the *filename* is actually a required input of sorts, while the *file* itself is the output.)

There are also command-line tools that name their output files automatically (using hardwired names, or some kind of naming convention derived from an input value). For these cases, stimela supports an ``implicit`` property, which tells it how the output file is named. For example::

    inputs:
        input-file: 
            dtype: File 
            required: true
    outputs:
        output-file:
            dtype: File
            implicit: output.dat 
        another-output-file:
            dtype: File
            implicit: '{current.input-file}.out' 

This defines two implicit output files. One is always named "output.dat", the other one has the same name as the input, plus an ".out" extension. The latter makes use of the substitution mechanism (see :ref:`subst`).


Other schema properties
-----------------------

Schemas have a number of other optional properties, see :ref:`schema_reference` for details. Here we cover a few more common ones.

* the ``nom_de_guerre`` property tells stimela to call the parameter something else when invoking the underlying tool (i.e., use a different name for the command-line option or function argument). The default is to use the name of the parameter.

* the ``choices`` property (a list) tells stimela that an input can only take on a restricted set of values.
 
* the ``element_choices`` property, which is only applicable to list-type inputs, tells stimela that elements of the list can only take on a restricted set of values.

A few other properties are primarily relevant to file-type IOs:

* the ``writable`` property indicates mixed IO, i.e. an input that can also be written to (for example, think of a Measurement Set that is updated by the underlying tool)

* the ``mkdir`` property tells stimela to create a directory, if the directory component of a mixed output does not exist. 

* setting the ``must_exist`` property to False tells stimela that an input file does not need to exist. Normally, missing input files raise an error during validation.

* setting the ``remove_if_exists`` property on an output tells stimela to remove the output file before running the task, should it exist.


Parameter validation
--------------------

As intimated above, stimela tries to be fairly proactive (and protective of the user) in terms of parameter validation. There are few things more frustrating than starting a long workflow overnight, only to discover the next morning that it failed 10 minutes in due to a missing parameter. 

Validation is performed on mutiple levels. *Prevalidation* is done before running a recipe. This checks the recipe for self-consistency inasmuch as possible, i.e. that all required parameters (of the recipe itself, and of the constituent steps) are present, that parameter types match the schemas, etc. These checks are, by necessity, limited in scope -- some parameters (e.g. those that depend on the outputs of a step) may only become valid and available at runtime. This is where *runtime validation* steps in. Before running a step, stimela will do a final check, ensuring that all required inputs are present, all inputs match the schema, and all required named file outputs are supplied. After a step is executed, stimela will likewise check all outputs for validity.

When checking parameters against a schema, type checking is enforced, but strings are (usually) sensibly parsed, YaML-style. For example, an ``int`` input will hapily accept the string ``"5"`` (but not ``"a"``), and a ``List[int]`` can be specified as ``"[0, 2]"``. 


Parameter policies for cabs
---------------------------

At the end of the day, stimela needs to know how to communicate the inputs (and named outputs) to the underlying cargo. If the cargo is a recipe or a Python function, this is straightforward, and no additional information is required. If the cargo is a cab that wraps a command-line tool, stimela needs to know how to form up a command line correctly. Different tools employ different command-line conventions -- think positional arguments, versus ``-option value``, versus ``--option value``, versus ``option=value``, versus ``--option`` and ``--no-option`` for boolean flags, versus ``--option X Y`` versus ``--option X --option Y`` for list-type arguments. This information about conventions is supplied via a ``policies`` property.

Policies can be defined both at cab level (in which case they apply to all parameters) and at the parameter level. For example, consider a cab definition wrapping the standard shell ``mv`` command::

    cabs:
        mv:
            command: mv
            policies:
                prefix: "--"
            inputs:
                source: 
                    dtype: List[File]
                    required: true
                    policies:
                        positional: true
                        repeat: list
                update:
                    dtype: bool
                verbose:
                    dtype: bool
            outputs:
                dest: 
                    dtype: Union[File, Directory]
                    required: true
                    policies:
                        positional: true

Here we've told stimela that the ``mv`` command expects its parameters to be prefixed by a double-dash (``--update``, ``--verbose``), except for the ``source`` and ``dest`` parameters, which are passed as positional arguments. Finally, if the ``source`` parameter is a list of files, it will be passed as multiple positional arguments (``repeat: list``). 

The ``policies`` section has a whole slew of other properties, which can be used to describe the most esoteric command-line conventions. Refer to :ref:`policies_reference` for further details.
