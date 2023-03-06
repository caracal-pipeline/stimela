
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

Next, we might want to set some stimela options. This is done by tweaking the ``opts`` section (see :doc:`reference/options` for details)::

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

...and, in fact, any recipe section defined at the top level will be implicitly inserted into `lib.recipes`. Defining things at top level is simply a shortcut that saves on identation.

Then, we have an optional `name` attribute (if missing, the section name will be used as the recipe name), and an `info` string describing the recipe.



Recipe variables
****************

Variables::

        assign:
            dir-out: '{recipe.dirs.base}/{recipe.dirs.sub}{recipe.output-suffix}'                     # output products go here
            image-prefix: '{recipe.dir-out}/im{info.suffix}{recipe.variant}/im{info.suffix}{recipe.variant}'  # prefix for image names at each step
            log.dir: '{recipe.dir-out}/logs/log-{config.run.datetime}'          # put logs into output dir
            # some more directory assignments
            dirs:
                ms: ../msdir       # MSs live here
                temp: "{config.run.env.HOME}/tmp"   # temp files go here
                base: .            # base project directory -- directory of recipe by default, but see below
