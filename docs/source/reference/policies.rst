.. highlight: yml
.. _policies_reference:

Parameter policies reference
============================

Parameter policies determine how cab parameters are converted into command-line options of the underlying executables (with one exception pertaining to Python callables -- see below). Most tools follow an ``-option value`` or ``--option value`` convention on the command line -- the latter is taken to be the default, but see ``prefix`` below.

Policies can be defined for all the parameters of the cab as a whole, via a ``policies`` section in the cab definition. They can also be tweaked on a per-parameter basis, via a ``policies`` section in the appropriate schema.

The ``policies`` section can contain one or more of the following attributes (see also the `source code <https://github.com/caracal-pipeline/stimela/blob/3a74f8acbb93e2594a47f08ea83a5592aec96e43/scabha/cargo.py#L33>`_ for details):

* ``prefix`` determines the prefix of command-line options. The default is ``--``, while another common setting is ``-``. The examples below use ``--option`` -- this becomes ``-option`` if ``prefix: -`` is used.

* ``key_value: true`` causes the parameter to map to a ``name=value`` argument.

* ``positional: true`` causes the parameter to be passed as a positional argument. The examples below use ``--option`` -- if ``positional`` is set, the ``--option`` argument is omitted, and the parameter value is passed directly.

* ``positional_head: true`` causes the parameter to be passed as a positional argument ahead of all non-positional options. The default is to pass positional arguments after non-positional arguments.

* ``repeat`` determines how to treat list-type parameters. This policy has no default, and must be set for any list types. Possible settings are:

  * ``list`` to pass the list as multiple arguments, i.e. given a parameter named "option" with a value of [X, Y], this results in three command-line arguments: ``--option``, ``X``, ``Y`` (or two command-line arguments ``X`` and ``Y``, if a positional policy is set.)

  * ``[]`` to use a YaML-formatted list, i.e. ``--option``, ``[X,Y]``.

  * ``repeat`` to repeat the option for every list element, i.e. ``--option``, ``X``, ``--option``, ``Y``.

  * any other value is used as a separator to paste list elements together: ``repeat: ,`` results in the command-line arguments ``--option``, ``X,Y``.

* ``skip: true`` causes the parameter to be omitted from the command line entirely.

* ``skip_implicits: true`` causes the parameter to be omitted if it is :ref:`implicit <implicit_params>`.

* ``disable_substitutions: true`` disables :ref:`substitutions and formula evaluations <subst>`` on the parameter.

* ``explicit_true: value`` causes true-valued boolean parameters to be passed as ``--option value``. If not given, they are simply passed as ``--option``.

* ``explicit_false: value`` causes false-valued boolean parameters to be passed as ``--option value``. If not given, false-valued booleans are omitted.

* ``split: separator`` causes string-valued parameters to be split using the separator, and passed as separate arguments. For example, given ``split: ,``, the parameter ``option`` with a value of ``X,Y`` will be passed as ``--option X Y``.

* ``replace`` gives a dictionary of ``from: to`` pairs that effect replacements in the *name* of the parameter when converting it to a command-line option. A common example is ``replace: {_: -}``, which results in, e.g., parameter ``option_name`` becoming ``--option-name``.

* ``format`` determines how parameter values are converted into command-line arguments (which are intrinsically always strings). The default behaviour is to simply use Python's ``str()`` function. If a value for ``format`` is specified, Stimela uses it as a format string, invoking the ``str.format(**params)`` method. Note that the entire parameter dictionary is passed in, so the format string can, in principle, refer to other parameters.

  If the value of the parameter is a list, the format string is applied to each element, making a list of command-line arguments (unless ``format_list`` is specified). Thus, the entire list is formatted uniformly.

* By contrast, ``format_list`` should be used when a list needs to be formatted non-uniformly. Each element of ``format_list`` is treated as a format string, invoked as ``str.format(*value, **params)``. The resulting list of strings becomes a list of command-line arguments.

* Finally, ``format_list_scalar`` can be used to turn a single parameter value into a list of command-line arguments. Each element of ``format_list_scalar`` is treated as a format string, invoked as ``str.format(value, **params)``. The resulting list of strings becomes a list of command-line arguments.

* ``pass_missing_as_none: true`` is the one policy attribute that applies to :ref:`Python callable flavour <cab_flavours>` cabs. Setting this causes missing parameters (i.e. non-required parameters defined by the schema and not supplied within the recipe) to be passed to the underlying callable anyway, using values of ``None``. If this is not set, missing parameters are not passed to the callable. 
