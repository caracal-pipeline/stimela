
.. highlight: yml
.. _anatomy:

Anatomy of a complex recipe
###########################

In this section, we'll take a look at a fairly elaborate real-life recipe and explain its moving parts. 

Preliminaries
*************

First, we have a shebang::

    #!/usr/bin/env -S stimela run

this is not necessary, but it's handy, as it allows the recipe file to be executed directly from the shell (if you chmod the recipe file to be executable), implicitly invoking ``stimela run``. Next, we can have an include block::

    _include:
        - (cult_cargo)wsclean.yml
        - oms_cabs.yml

Include works exactly as advertised -- any YaML files listed in the include block are included into the recipe. Typically, these are cab definitions. The first form -- ``(cult_cargo)wsclean.yml`` -- will look for the given file inside the Python package named ``cult_cargo``. This is a typical way to include standard cab definitions. The second form -- ``oms_cabs.yml`` -- will look for the file in the current directory, then in a few standard locations such as ``~/lib/stimela``. You can set the ``STIMELA_INCLUDE`` environment variable to spcify a custom set of paths to look in.

Option settings
****************

Next, we might want to set some stimela configuration options. This is done by tweaking the ``opts`` section (see :ref:`options` for details)::

    opts:
        log:
            dir: logs/log-{config.run.datetime}
            name: log-{info.fqname}
            nest: 2
            symlink: log
        rlimits:
            NOFILE: 100000  # set high limit on number of open files

The first of these have to do with logfile handling (see :ref:`logfiles` for details). Logfiles are extremely useful, since they
capture the entire output of all the steps of a recipe. Here, we're telling Stimela that:

1. We want the logs from every run placed into a subdirectory called ``./logs/log-DATETIME``. This way, logs from each run are kept separate.

2. We want logfiles to be split up by step, and named in a specific way (``log-recipe.step.txt``)...

3. ...but only to a nesting level of 2 -- that is, if a top-level step happens to be a sub-recipe with steps of its own, these nested steps will not have their own logfiles -- rather, all their output will be logged into the logfile of the outer step. If we wanted nested recipe steps to be logged separately, we could increase the nesting level.

4. We want a symlink named ``logs/log`` to be updated to point to the latest log subdirectory -- this allows for quick examination of logs from the latest run.

The ``opts.rlimits`` setting above adjusts a process resource limit (the max number of open files, in this case. The normal default of 1024 may be too small for some operations.) See https://docs.python.org/3/library/resource.html for details of the resource limits that may be adjusted. 

Body of the recipe
******************

Next, we have the body of the recipe proper::

    rrat:
        name: ratt-rrat
        info: imaging of RRAT follow-up

This starts with a section name. Because the section name is not one of the standard ones (i.e. `cabs`, `opts`), stimela will treat
it as a recipe definition. Note that it is also possible to define recipes directly in the library, like this::

    lib:
        recipes:
            rrat:

...and, in fact, any recipe section defined at the top level will be implicitly inserted into ``lib.recipes``. Defining things at top level is simply a shortcut that saves on identation.

Then, we have an optional ``name`` attribute (if missing, the section name will be used as the recipe name), and an ``info`` string describing the recipe.


Recipe variables
****************

Next, we define some :ref:`variable assignments <assign>`::

    assign:
        dir-out: '{recipe.dirs.base}/{recipe.dirs.sub}{recipe.output-suffix}'                     # output products go here
        image-prefix: '{recipe.dir-out}/im{info.suffix}{recipe.variant}/im{info.suffix}{recipe.variant}'  # prefix for image names at each step
        log.dir: '{recipe.dir-out}/logs/log-{config.run.datetime}'          # put logs into output dir
        # some more directory assignments
        dirs:
            ms: ../msdir       # MSs live here
            temp: "{config.run.env.HOME}/tmp"   # temp files go here
            base: .            # base project directory -- directory of recipe by default, but see below

Note a few crucial details:

* the above makes heavy use of :ref:`{}-substitutions <subst>` to define a set of naming conventions. For example, the ``recipe.dir-out`` variable (which this recipe used consistently throughout to construct paths for output products) is formed up from a base directory (here set to "."), a subdirectory (defined via ``assign_based_on`` below), and an optional output suffix (defined as a recipe input below).
* assignments are re-evaluated (and thus resubstituted) at each recipe step. ``{info.suffix}``, for example, refers to the suffix of the current step's label. Thus, the recipe can contain steps labelled ``image-1`` and ``image-2``, and at each step the ``recipe.image-prefix`` variable will be updated accordingly. Note also how this refers to ``recipe.dir-out``.
* ``{config.run.datetime}`` fetches the timestamp of the Stimela run from the :ref:`configuration namespace <options>`. The assignment to ``log.dir`` results in :ref:`logfiles <logfiles>` being placed into a custom subdirectory (which is unique for each run, by virtue of having the timestamp included in its name). We're also telling Stimela that we want to keep logfiles inside ``recipe.dir-out``. 
* ``config.run.env`` contains all the shell environment variables, here we use ``HOME`` to get at the user's home directory.

The, we include a few more variable assignments using the :ref:`trick explained here <assign>`::

  assign_based_on:
    _include: rrat-observation-sets.yml

Recipe inputs
-------------

Next, it's time to define the recipe's inputs::

  inputs:
    obs:
      info: "Selects observation, see rrat-observation-sets.yml"
      required: true
    output-suffix:
      dtype: str
      default: ''
    dir-out: 
      dtype: str
    ms:
      dtype: MS

The only required input is ``obs``, which selects the observation to be processed. The ``assign_based_on`` section above relies on this input to set up a slew of other variables.

The optional ``output-suffix`` we already saw being employed in the assignments above. Next, we have inputs called ``dir-out`` and ``ms``. These may look familiar -- you may have noticed them being assigned to above as well. What is going on and why are we assigning to the recipe's inputs? Recall that :ref:`inputs can be assigned to<assign>`; this is effectively a roundabout way of setting up a default value for them. Here, the intended "normal" usage of the recipe is to specify an ``obs`` value and have ``ms`` and ``dir-out`` set up automatically via ``assign_based_on``, however they remain as documented inputs that the user may override explicitly.

Recipe steps
------------



