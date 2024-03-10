.. highlight: yml
.. _assign:

Recipe variable assignments
###########################

In addition to inputs and outputs (IOs), recipes can define internal *variables* (a close analogy would be the local variables of a Python function, as opposed to its parameters). 

Variables are defined by providing an optional ``assign`` section within the recipe::

    my-recipe:
        assign:
            foo: x
            bar: 
                baz: '5'
                qux: 5
                quux: =recipe.bar.qux * 2
            bar.corge: y
            grault: z 
        inputs:
            grault:
                dtype: str

Things to note from the above:

* Variables do not have a fixed schema. Their types are set dynamically at time of assignment, following normal YaML syntax (in the case of constants). In the example above, ``foo``, ``bar.baz`` and ``bar.corge`` would be strings, while ``bar.qux`` is an integer. 
* Variables can be nested into subsections, such as ``bar`` above. Using dot-syntax (``bar.corge``) is equivalent to putting a variable inside a subsection.
* Variables are subject to :ref:`subst`, and can be used inside formulas and substitutions themselves, via the ``recipe`` namespace (this is, in fact, the primary *raison d'etre* of variables.)
* Inputs may also be assigned to (here, we assign a value of "z" to the ``grault`` input). In this case, it has the same effect as specifying ``default: z`` in grault's schema. In fact using a default is the recommended practice, being more transparent -- default values are automatically documented when the user asks for help using ``stimela doc`` while variables assignments aren't -- but see :ref:`anatomy` for more appropriate examples. 

Assignment immunity
===================

In the above example, you may wonder how ``grault`` can be set to anything other than "z" (and why have it as an input rather than a variable in the first place then?) The answer is that the user is allowed to override this -- any inputs that have been explicitly specified by the user are marked as *immune* from further assignment. If the user runs the recipe with ``grault=zztop`` on the command line, all assignments to ``grault`` within the recipe are ignored.

In passing, note that variable assignments can also be overridden from the command line. The user can run the recipe with ``bar.quux=5``, and the variable then becomes immune to assignment. This trick can be a handy debugging aid, but it is not recommended to make it part of your "official" recipe interface, again for reasons of non-transparency.

Step assignments
================

Any step definition can contain an ``assign`` section of its own. Use this if you need to change the value of recipe variable just for that one step.

Note that the recipe-level assign section is reevaluated before each step, followed by the step-level assignment section. In effect this means that any step-level assignments pertain to that step alone. For example::

    my-recipe:
        assign:
            foo: x
        steps:
            a:
                cab: my-cab
                params:
                    bar: =recipe.foo
            b:
                cab: my-cab
                assign:
                    foo: y
                params:
                    bar: =recipe.foo
            c:
                cab: my-cab
                params:
                    bar: =recipe.foo

Here, the ``bar`` parameter of steps ``a`` and ``c`` will be set to "x", and that of step ``b`` to "y".

Logging options and config assignments
======================================

Two special ``assign`` subsections are ``log`` and ``config``::

    my-recipe:
        assign:
            foo: x
            log:
                dir: logs/log-{config.run.datetime}
                name: log-{info.fqname}-{recipe.foo}
                nest: 2
                symlink: log
            config.opts.backend.rlimits.NOFILE: 10000

The ``config`` subsection gives access to the entire :ref:`configuration namespace <options>`. You can use this if you want to tweak some configuration setting on a per-recipe or per-step basis. In the example above, we're changing a resource limit -- in particular, the number of open files that a process may have. See https://docs.python.org/3/library/resource.html for details. 

The ``log`` subsection gives access to :ref:`logging options <logfiles>`, and is equivalent to assigning to ``config.opts``. 
Here, we're telling Stimela that:

1. We want the logs from every run placed into a subdirectory called ``./logs/log-DATETIME``. This way, logs from each run are kept separate.

2. We want logfiles to be split up by step, and named in a specific way (``log-recipe.step-FOO.txt``), where ``FOO`` is the value of the ``foo`` variable. This can be useful in :ref:`for_loops`, in order to log every iteration of the loop into a separately named file.

3. ...but only to a nesting level of 2 -- that is, if a top-level step happens to be a sub-recipe with steps of its own, these nested steps will not have their own logfiles -- rather, all their output will be logged into the logfile of the outer step. If we wanted nested recipe steps to be logged separately, we could increase the nesting level.

4. We want a symlink named ``logs/log`` to be updated to point to the latest log subdirectory -- this allows for quick examination of logs from the latest run.

Note that a simpler and more typical pattern is to set up logging options via a top-level ``opts.log`` section (as in :ref:`anatomy`). Assigning log options within a recipe or step is somewhat more exotic, and is only necessary if you want finer control of your logs (e.g. for :ref:`for_loops`).


Assign-based-on
===============
.. _assign_based_on:

What if you wanted to assign a variable based on the value of another variable? The ``assign_based_on`` section (at recipe or step level) can be used to accomplish this. Here is an example::

    my-recipe:
        inputs:
            obs:
                dtype: str
                choices: [a, b, c]
        assign_based_on:
            obs:
                a:
                    ms: data-a.ms
                    band: L
                b:
                    ms: data-b.ms
                    band: UHF
                DEFAULT:
                    ms: data-c.ms
                    band: UHF
            band:
                L:
                    pixel-size: 1arcsec
                UHF:
                    pixel-size: 2arcsec

Note the following:

* This recipe has an input called ``obs``, which can be set to one of "a", "b" or "c" (i.e. by running the recipe with ``obs=a`` on the command line.)
* If ``obs`` is set to "a", we set the ``ms`` variable to "data-a.ms", and the ``band`` variable to "L".
* If ``obs`` is set to "b", we set the ``ms`` variable to "data-b.ms", and the ``band`` variable to "UHF".
* In all other cases (see ``DEFAULT``), we set the ``ms`` variable to "data-c.ms", and the ``band`` variable to "UHF". If ``DEFAULT`` was missing, Stimela would report an error if ``obs`` was set to something other than "a" or "b".
* Assign-based-ons can be chained: once ``band`` is set to "L" or "UHF" based on ``obs``, Stimela then knows to set ``pixel-size`` variable accordingly.

The sub-sections of ``assign_based_on`` can refer to recipe inputs, variables, or :ref:`configuration <options>` settings (using dot-syntax, as e.g. ``config.run.node`` below).

This feature is particularly useful if you want to set up a whole bunch of things based on a single input (``obs``, in this case). Tip: you can combine this with :ref:`include` to structure your reduction recipes into a specific "configuration" file, and a generic "recipe proper". Here's a real-life example. Let's say we have a "configuration" file called ``rrat-observation-sets.yml`` :ref:`with these contents <variables_rrat>`. In the recipe itself, we then have::

    my-recipe:
        assign_based_on:
            _include: rrat-observation-sets.yml
        inputs:
            obs:
                info: "Selects observation, see rrat-observation-sets.yml"

The "configuration" file then contains all the details of the specific datasets to which the recipe can be applied, while the recipe itself remains completely generic; the dataset (and all its associated options and tweaks) can be selected at runtime by setting the ``obs`` parameter. As a bonus (see the ``config.run.node`` entry), we can also specify different defaults based on which particular node the recipe is executed on. Note that ``run.node`` is an entry in the :ref:`configuration namespace <options>`.






