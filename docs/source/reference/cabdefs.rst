.. highlight: yml
.. _cabdefs:

Cab definitions
###############

A *cab* represents an atomic task that can be executed by Stimela. Cabs come in a number of flavours: executables, Python functions, 
direct Python code snippets, and CASA tasks. Cabs have *inputs* and *outputs* defined by their :ref:`schema<params>`.

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

Here is a typical cab definition from the cult-cargo package -- this one if for the breizorro masking tool::

    cabs:
        breizorro:
            command: breizorro
            image: ${vars.cult-cargo.registry}/breizorro:cc${vars.cult-cargo.version}
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

* all cabs have a ``command`` field. In this case, it specifies the command that must be invoked;

* the ``image`` field specifies an image name. This only takes effect if a :ref:`containerized backend<containers>` such as Singularity or Kubernetes is in use; 

* the ``policies`` section defines how names of inputs and outputs are mapped into command-line arguments. There are many rich options for this, see the :ref:`policies reference<policies>` for details. In this case, we are merely telling stimela to turn underscores into dashes.

* the ``inputs`` and ``outputs`` sections specify the IOs of the cab. These use the standard :ref:`schema language<params>`.

Cab flavours
============

A cab can correspond to a Python function or a CASA task. This is specified via the ``flavour`` field::

    cabs:
    casa.flagman:
        info: Uses CASA flagmanager to save/restore/list flagversions 
        command: flagmanager
        flavour: casa-task
        image: ${vars.cult-cargo.registry}/casa:cc${vars.cult-cargo.version}
        inputs:
        ms: 
            dtype: MS
            required: true
            nom_de_guerre: vis
        versionname:
            info: "flag version name"
        mode: 
            choices: [save, restore, list]
            required: true







