.. highlight: yml
.. _best_practices:

Best practices
##############

  "If you make up your own mind, you can only blame yourself."  *(George Best)*

Stimela 2.0 is a fairly young package, so this section will be short and sweet. We hope to expand it as we (and you) gain more experience with the software.

Structuring recipes
-------------------

Stimela itself does not impose any particular structure on the recipe YaML, so one could, in principle, make it one big flat document containing everything: cab definitions, recipes, configuration, backend settings, etc.

However, the ``_include`` and ``_use`` features can be used to promote a far more modular and structured approach, with separate documents for:

* cab definitions used by the recipe (if non-standard cabs are required in the first place), pulled in via an ``_include``;

* the recipe body itself;

* backend settings and configuration tweaks.

This neatly separates the task definitions (i.e. cabs), the logical sequence of operations (the recipe), and the runtime environment definition (configuration and tweaks).

Note that the configuration tweaks themselves should not need to be explicitly included into the recipe. The recipe itself can specify fairly generic configuration settings appropriate for most environments (e.g. using the Singularity backend). A separate ``tweaks.yml`` file could be provided to e.g. configure a Kubernetes backend and :ref:`apply CPU and RAM allocations <backend_reference>` to individual cabs and steps. Since Stimela merges everything together at runtime, simply calling::

    $ stimela run recipe.yml tweaks.yml

will apply the tweaks to the recipe. This practice promotes the writing of generic and platform-independent recipes.


Layering your cab definitions
-----------------------------

The standard set of cabs in ``cult-cargo`` is, at present, far from complete. It is also (by design) very generic. Any non-trivial workflow will probably require a few more custom cabs to be defined. In particular, this pertains to Python callable cabs, which provide "glue code" for any non-standard operations in the workflow.

The PARROT recipe (:ref:`anatomy`) provides an example of how to do this with a layered approach:

* ``parrot-cabs.yml`` provides custom cab definitions for operations that are highly specific to the PARROT workflow. This is the top, highly specialized cab layer.

* ``parrot-cabs.yml`` uses ``_include`` to pull in standard ``cult-cargo`` cabs, as well as some cab definitions from `omstimelation <https://github.com/o-smirnov/omstimelation/blob/parrot1/oms-cabs.yml>`_. The latter is an intermediate layer of semi-experimental cabs that are generic enough to be worth sharing between different projects. As these evolve and become more generic, some may be submitted for promotion to ``cult-cargo``.

* The bottom layer is ``cult-cargo``, providing highly generic and standardized cabs.


Use and reuse of step parameters
--------------------------------

Complex tools (e.g. WSClean, CubiCal, QuartiCal) tend to require a large number of parameters to be specified for any given invocation. Doing this directly in the recipe body can lead to lengthy recipes that are difficult to read (particularly when such tools are invoked multiple times in the workflow).

Judicious deployment of ``_use`` and ``_include`` can mitigate this complexity. An example of this is given in :ref:`anatomy`: "templated" step definitions are inserted into ``lib.steps`` via an `included file <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/parrot-cabs.yml#L452>`_, and invoked `within the actual step <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/image-parrot.yml#L137>`_ via ``_use``. Only the parameters that are different from the template then need to be specified.

There are pros and cons to this approach. The recipe reads simpler because unnecessary details are hidden away. On the other hand, precisely because the details are hidden away, looking for them inside nested includes can get confusing. Use judiciously, and consider the meaning of "unnecessary".

An alternative pattern is to recycle previous steps like so::

    my-recipe:
        steps:
            foo-1:
                params:
                    a: 1
                    b: 2
                    c: 3
                    d: "four"
                    e: V
            foo-2:
                _use: my-recipe.steps.foo-1
                params:
                    e: E

This makes for an intuitively pleasing pattern of "reuse all the settings from that other step, except for these specific ones here".


Aliases vs formulas vs substitutions
------------------------------------

A common scenario arises when a recipe input must be directly passed to a step. Consider the following four different ways of skinning this same cat. Firstly, a :ref:`formula <subst>`::

    my-recipe:  
        inputs:
            ms:
                dtype: MS
        steps:
            foo:
                cab: wsclean 
                params:
                    ms: =recipe.ms

A {}-:ref:`substitution <subst>`::

    my-recipe:  
        inputs:
            ms:
                dtype: MS
        steps:
            foo:
                cab: wsclean 
                params:
                    ms: {recipe.ms}

An inline :ref:`alias <aliases>` declaration::

    my-recipe:  
        inputs:
            ms:
                aliases: [foo.ms]
        steps:
            foo:
                cab: wsclean 

And finally, a separate alias declaration::

    my-recipe:  
        aliases:
            ms: [foo.ms]
        steps:
            foo:
                cab: wsclean 

Which one is best? There are some subtle differences.

Firstly, the two alias declarations are completely equivalent. Pick whatever makes your recipe more readable, in your opinion.

Aliases are the most rigorous and robust way of linking recipe and step parameters. An alias declaration tells Stimela that the two are, strictly, one and the same entity. (The schema for the recipe's input is copied from the cab's schema.) Stimela can then fully validate these inputs before starting the recipe. This promotes catching user errors up front.

The ``=recipe.ms`` approach provides a somewhat looser linkage. This approach defers validation to runtime, when the actual step is being executed. If ``recipe.ms`` is an MS, and the cab expects an MS, great, everything just works. If there's any mismatch, the recipe will fail. The error is still caught, but not up front.

The ``{recipe.ms}`` substitution is the least robust way of doing this linkage (but still often seen in older recipes, for historical reasons -- it was the first such feature in early versions of Stimela). {}-substitutions are purely string-based operations. ``{recipe.ms}`` is evaluated to a string, and if this happens to be a valid MS name, great, the step works. In this case, the effect is no different from that of using ``=recipe.ms``. For aesthetic reasons, we prefer the latter -- save {}-substitutions for when more complex strings need to be formed up.
