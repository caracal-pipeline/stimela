.. highlight: yml
.. _options:


Configuration namespace
=======================

Stimela's configuration namespace can be thought of as a single global mapping containing everything pertinent to Stimela operations. When recipe or config files are loaded in, they are simply (with one :ref:`exception below <config_loading_recipes>`) merged into this mapping one by one.

At the top level, the namespace has the following sections:

* ``cabs``: cab definitions, populated by including files from e.g. ``cult-cargo``, as well as the user's recipes, if these include any custom cabs.

* ``image``: image definitions for standard images used by Stimela (such as Python and CASA).

* ``opts``: various options, such as:

  * ``opts.backend``, defining :ref:`backend <backends>` settings;
    
  * ``opts.log``, defining :ref:`logging <logfiles>` settings;

  * ``opts.profile``, defining profiling settings;

  * ``opts.include``, giving a set of paths to search for when :ref:`_include statements <include>` are used.

* ``run``: runtime information about the Stimela session, such as

  * ``run.date``: the date at the start of the session, in YYYYMMDD format;

  * ``run.time``: the time at the start of the session, as HHMMSS format;

  * ``run.datetime``: the concatenation of the above, i.e. YYYYMMDD-HHMMSS;

  * ``run.hostname``: the name of the host machine, possibly including domain;

  * ``run.node``: the first part of the hostname (before the first dot);

  * ``run.env``: a mapping of all the environment variables of the current shell session, e.g. ``run.env.HOME``.

  The contents of this subsection are primarily useful for (via :ref:`substitutions <subst>`) such as ``"{config.run.datetime}"``

* ``vars``: this is a free-form section containing configuration variables that may be defined and reused (via :ref:`substitutions <subst>`) by packages and recipes.

* ``lib``: libraries of various reusable objects that may be defined and reused (via the :ref:`use statement <use_statement>`) by packages and recipes:

  * ``lib.recipes`` for recipes (these can be invoked as sub-recipes by specifying ``recipe: name`` in a step definition);

  * ``lib.steps`` for standard step definitions;

  * ``lib.params`` for standard parameter definitions;

  * ``lib.misc`` a free-form section for anything and everything.

The entire content of the configuration mapping is available for substitution (and formula evaluation) via the ``config.*`` namespace, in any context where substituions are supported (thus, primarily, in parameter and variable :ref:`assignment <assign>` sections of recipes and steps). It can also be referenced as a key in :ref:`assign-based-on sections <assign_based_on>`. Finally, the config namespace itself can be an assignment target::

    my-recipe:
        assign:
            config.vars.some.variable: foo

(Caution, this is a power tool, use with care. Anything in the config namespace may be assigned to like this -- indicriminate tweaking can break your Stimela session in weird ways.)

Recipes and configuration
-------------------------
.. _config_loading_recipes:


In practice, there's little difference between recipes and configuration files, since recipe files can include configuration tweaks (see :ref:`anatomy` for an example). However, when loading recipe YaML files via the ``run``, ``doc`` or ``build`` commands, Stimela takes one additional post-processing step. Any content corresponding to the top-level configuration sections listed above is merged into the configuration namespace as expected. However, any sections **not** listed above are treated as recipe definitions, and implicitly moved into ``lib.recipes``. Thus, a recipe file containing::

    opts:
        backend:
            select: singularity

    my-recipe:
        steps:
            ...

is actually equivalent to::

    opts:
        backend:
            select: singularity

    lib:
        recipes:
            my-recipe:
                steps:
                    ...

but requires less identation, and makes the recipe body more readable.

Note that any :ref:`_use statements <use_statement>` inside ``my-recipe`` are processed before the move to ``lib.recipes`` takes place. Thus, the intuitive usage below is the correct one::

    my-recipe:
        steps:
            foo:
                params:
                    x: 1
                    y: 2
            bar:
                params:
                    _use: my-recipe.steps.foo.params  # reuse from foo, but adjust y
                    y: 3
            ...


Startup config files
--------------------

The configuration namespace can be tweaked at startup by providing a ``stimela.conf`` file at one or more locations. Stimela will load these file(s), if they exist, from a number of locations:

* the ``stimela`` Python package directory

* the virtual environment directory (if any)

* the ``cult-cargo`` Python package directory, if installed

* ``~/.stimela/stimela.conf``

* ``~/.config/stimela.conf``

* ``./stimela.conf``

Any files found will be merged in one by one, in the order given above (thus, content from files lower in the list will augment or override earlier content). 