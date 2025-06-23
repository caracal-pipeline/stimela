.. highlight: yml
.. _cabdefs:

Cab definition reference
########################

A *cab* represents an atomic task that can be executed by Stimela. Cabs come in a number of flavours: executables, Python functions, 
direct Python code snippets, and CASA tasks. Cabs have *inputs* and *outputs* defined by their :ref:`schema<params>`.

Basics of cab definitions
*************************

A cab is defined by an entry in the ``cabs`` section of the :ref:`global namespace<options>`. Alternatively, a cab definition may be given 
directly inside's a step's ``cab`` section. For example, the following two are equivalent::


    cabs:
        cp:
            command: /bin/cp

    my-recipe:
        steps:
            copy:
                cab: cp

and::

    my-recipe:
        steps:
            copy:
                cab: 
                    command: /bin/cp

...though the former is probably more useful if you plan to invoke the cab more than once.

Here is a `typical cab definition <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/breizorro.yml#L1>`_ from the cult-cargo package -- this one is for the ``breizorro`` masking tool::

    cabs:
        breizorro:
            command: breizorro
            image: 
                _use: vars.cult-cargo.images
                name: breizorro
            policies:
                replace: {'_': '-'}
            inputs:
                restored-image:
                    dtype: File
                mask-image:
                    dtype: File
                merge:
                    dtype: Union[str, List[str]]
                subtract:
                    dtype: Union[str, List[str]]
                threshold:
                    dtype: float
                    default: 6.5
                dilate:
                    dtype: int
                number-islands:
                    dtype: bool
                ...
            outputs:
                mask:
                    dtype: File
                    nom_de_guerre: outfile
                    required: true 

This illustrates a number of imporant points:

* all cabs have a ``command`` attribute. In this case, it specifies the command that must be invoked;

* the ``image`` mapping specifies an image (as name, version and registry -- the latter two attributes being :ref:`reused <use_statement>` from a global cult-cargo `variable <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/genesis/cult-cargo-base.yml#L3>`_). The image setting takes effect if a :ref:`containerized backend<backends>` such as Singularity or Kubernetes is in use; 

* the ``policies`` section defines how names of inputs and outputs are mapped into command-line arguments of the breizorro command. There are many rich options for this, seehe :ref:`policies_reference` for details. In this case, we are merely telling Stimela to turn underscores into dashes.

* the ``inputs`` and ``outputs`` sections specify the IOs of the cab. These use the standard :ref:`schema language<params>`.

Other cab properties that you may come across are:

* a ``parameter_passing`` property determines how inputs are passed to the cab. The default is ``args``, i.e. they are mapped to command-line arguments using specified policies. (The rather exotic alternative is ``yaml``, which passes inputs as a YAML string via the first command-line parameter. This is not used anywhere at time of writing, but is retained for historical reasons.)

* a ``backend`` section allows you to specify a non-default backend for the cab, or to tweak backend options. See :ref:`backend_reference` for details.

* a ``management`` section, explained below.

Advanced cab features
*********************

The ``management`` section can specify some interesting cab behaviours. Here is a `real-life example <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/casa-flag.yml#L45>`_ from cult-cargo::

    casa.flagsummary:
        info: Uses CASA flagdata to obtain a flag summary
        command: flagdata
        flavour: casa-task
        image: 
            _use: vars.cult-cargo.images
            name: casa
        inputs:
            ms: 
                dtype: MS
                required: true
                nom_de_guerre: vis
            spw:
                dtype: str
                default: ""
            mode:
                implicit: 'summary'
        outputs:
            percentage:
                dtype: float
        management:
            wranglers:
                'Total Flagged: .* Total Counts: .* \((?P<percentage>[\d.]+)%\)':
                  - PARSE_OUTPUT:percentage:float
                  - HIGHLIGHT:bold green

This demonstrates the use of **output wranglers**. Wranglers tell Stimela to trigger certain actions 
based on seeing certain patterns of text in the cab's console output (i.e. stdout/stderr). This can be a 
very powerful way to wrangle (pun intended) information out of third-party packages, or even just to prettify 
their console output. 

.. _wranglers:

Output wranglers
----------------

The (entirely optional) ``management.wranglers`` section consists of a mapping. The *keys* of the mapping are regular expressions (often containing *named groups* -- via the ``(?P<name>)`` construct -- see Python ``re`` module for documentation). These are matched to every line of the cab's console output. The *values* of the mapping are lists of **wrangler actions**, which are  applied to each matching line one by one. The following actions are currently implemented:

* ``PARSE_OUTPUT[:name]:groupname:type`` converts the text matched by the named group to the given type, and returns it as the named output of the cab. In the example above, we use this to extract the flag percentage out of the CASA task's output. 

* ``HIGHLIGHT:style`` applies a Rich text style when dispaying the matching line (e.g. to draw the user's attention). The above example highlights the output line that is reporting the flag percentage.
 
* ``REPLACE:text`` replaces the entire text matching the regex by the specified replacement text (which can reference named groups from the regex -- see Python ``re.sub()`` for details).

* ``SEVERIY:level`` issues the output line to the logger at a given severity level (i.e. ``warning``, ``error``), as opposed to the default level (which is normally ``info``).

* ``SUPPRESS`` suppressed the matching line from the output entirely.
 
* ``WARNING:message`` notes a warning message, which will be displayed by Stimela when the cab is finished running.

* ``ERROR[:message]`` declares an error condition. The cab's run will be marked as a failure, even if its exit code indicates success. An optional error message can be supplied. 

* ``DECLARE_SUCCESS`` declares the cab's run a success, even if a non-zero exit code is returned.

Two other actions can be used to parse out output values in a specific way (Stimela uses these internally to pass information out of some specific cab flavours -- see below -- but they're also available to all user-defined cabs):

* ``PARSE_JSON_OUTPUTS`` parses the text matching each named group in the regex as JSON, and associates the resulting value with an output of the same name.

* ``PARSE_JSON_OUTPUT_DICT`` parses the text matching the first ()-group in the regex as JSON. The result is expected to be a ``dict``, whose keys are assigned to outputs of matching names.

Other management features
-------------------------

The optional ``management.environment`` section can be used to tell Stimela to set up some specific environment variables before invoking a cab.

The ``management.cleanup`` section can be used to specify a list of filename patterns that need cleaning up after the cab has been run. Use this if the underlying tool generates some junk output files you don't want to keep (the cleanup feature is currently not implemented as of 2.0, but will be implemented in a future version).

.. _cab_flavours:

Cab flavours
************

A cab can also correspond to a Python function or a CASA task. This is specified via the ``flavour`` attribute -- we saw an example of this just above with the ``casa.flagsummary`` cab. Its definition tells Stimela that the cab is implemented by invoking a CASA task underneath. Other flavours are ``python`` (for Python functions) and ``python-code`` (for inline Python code). The default flavour, corresponding to a binary command, is called ``binary``.

Specifying flavour options
--------------------------

An alternative way to specify flavours is to make ``flavour`` a sub-section, and use its ``kind`` attribute to specify the flavour. This then allows for some flavour-specific options to be set::

    cabs:
        casa.flagman:
            info: "Uses CASA flagmanager to save/restore/list flagversions"
            command: flagmanager
            flavour: 
                kind: casa-task
                path: /usr/local/bin/casa
                opts: [--nologger]

The above tells Stimela to use a non-default CASA intepreter, and to pass it specific extra options on the command line (see more detail below). 

Note that the CASA path and option settings can also be defined globally via :ref:`Stimela configuration <options>`.

Callable flavours: python calls and CASA tasks
----------------------------------------------

The ``casa-task`` and ``python`` flavours are very similar, in that they both invoke an external interpreter, and use it to call a function.  The ``command`` field of the cab then names a CASA task, or a Python callable (using the normal ``package.module.function`` Python naming). In the latter case, ``package.module`` will be imported using the normal Python mechanisms: this can refer to a standard Python module, or your own code (in which case it must be installed appropriately so the import statement can find it.)

Arguments to the function or task are described using the normal inputs/outputs schema; Stimela will convert these appropriately and invoke the function or task. The return value of the function can be treated as a cab output and propagated out to Stimela. Here is a notional example::

    cabs:
        get-load-avg:
            info: "returns the system load averages using Python's os.getloadavg() function"
            flavour:
                kind: python
                output: load
            command: os.getloadavg
            outputs:
                load: Tuple[float, float, float]

The ``flavour.output`` option here specifies that the return value of the function in propagated out as the output named ``load``, while the outputs schema decribes what data type to expect.

What if you would like to provide some Python code returning several outputs? This can be done by having your function return a ``dict``, setting the ``flavour.output_dict`` option to true, and providing an outputs schema. In this case, the returned dict is expected to contain a key for every output named in the schema. 

Inline Python code
------------------

The ``python-code`` flavour allows for snippets of Python code to be specified directly in the cab::

    cabs:
        simple-addition:
            info: "returns c=a+b"
            flavour: python-code
            command: |
                c = a + b
            inputs:
                a: float *
                b: float *
            outputs:
                c: float

Note how we use :ref:`abbreviated schemas<shorthand_schemas>` here for succinctness, and the ``": |"`` feature of YAML, which starts a multiple-line string, and uses indentaton to detect where the string ends.

The operation of the ``python-code`` flavour is quite intuitive. All inputs are converted into Python variables with the corresponding name, the Python code specified by ``command`` is invoked, and any outputs are collected from Python variables of the corresponding name. 

A few flavour attributes can be used to tweak this behaviour. If you would prefer to pass the inputs in as a ``dict`` (keyed by input name), set ``input_dict: true``. If you define cab outputs but **don't** want them to be picked up from Python variables for some reason (perhaps because you're using output :ref:`wranglers` instead?), you can set ``output_vars: false``. Finally, unlike the other flavours, the ``command`` field is by default **not** subject to {}-substitution (as this usually adds nothing but hassle to inline code), but this can be changed by setting ``subst: true``.

Additional flavour options
--------------------------

python and python-code
^^^^^^^^^^^^^^^^^^^^^^

The following additional options are available for both the ``python`` and ``python-code`` flavours:

* ``flavour.interpreter_binary`` determines which Python interpreter binary to call, default is ``"python"``

* ``flavour.interpreter_command`` determines how the interpreter command line is formed, default is ``"{python} -u"``. Note that this is not subject to Stimela's full {}-substitutions, but does recognize ``{python}``, and inserts ``interpreter_binary`` as set above.

* ``flavour.pre_commands`` adds optional Python code to be executed up front. This can be useful for housekeeping operations, such as disabling warnings, etc. Multiple commands can be specified and will be run in order.::

    flavour:
        kind: python
        pre_commands:
            filter-warnings: |
                import warnings
                warnings.filterwarnings("ignore", category=SyntaxWarning)

* ``flavour.post_commands`` adds optional Python code to be executed after the command.

casa-task
^^^^^^^^^

The following additional options are available for the ``casa-task`` flavour. Note that default values for these may be specified via the :ref:`Stimela configuration <options>`, using the ``runtime.casa`` section -- if they are not set there, then the normal defaults indicated below apply.

* ``flavour.path`` specifies a path to the CASA binary. Normal default is ``"casa"``.


* ``flavour.opts`` specifies additional command-line options passed to the CASA binary, as a list of strings. Normal default is ``[--log2term, --nologger, --nologfile]``.

* ``flavour.wrapper`` wraps the CASA binary invocation in a wrapper command. Normal default is ``"xvfb-run -a"``, which fakes a virtual X11 display for CASA.


Bat country! Dynamic schemas
****************************

Some cabs need to support *variadic* interfaces, in the sense that their set of available 
parameters can change depending on the settings of other parameters. Two specific examples are:

* QuartiCal has a `solver.terms <https://quartical.readthedocs.io/en/latest/options.html#solver>`_ input that determines the set of active Jones matrices. If this is set to, e.g., ``[G,B]``, a whole slew of options associated with G and B (``G.type``, ``B.type``, etc.) becomes available.

* The structure of WSClean's file outputs changes substantially depending on whether polarization imaging, MFS imaging, etc. is enabled or not.

Stimela supports these scenarios via the concept *dynamic schemas*. The ``dynamic_schema`` attribute of the cab definition can be set to the name of a callable Python function (using the standard ``package.module.function`` syntax). Here is an `example from cult-cargo <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/quartical.yml#L18>`_, and here is the `corresponding function itself <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/genesis/quartical/external.py#L30>`_. The function takes three arguments: ``params``, ``inputs`` and ``outputs``, and returns a tuple of ``inputs, outputs`` that have been modified based on the contents of ``params``.

Dynamic schemas should be deployed with great care -- the complexity can get quite confusing. Consider simpler alternatives. For example, if the underlying tool has clearly distinct "modes of operation" (i.e., a mode setting, and different subsets of parameters applicable to different modes), it can be much simpler to provide a separate cab definition for each mode, using an :ref:`implicit mode parameter <implicit_params>` within each. Here is `an example <https://github.com/caracal-pipeline/cult-cargo/blob/22cd21fd3c40894214bef253ee683abde2cc454a/cultcargo/casa-flag.yml#L25>`_.


