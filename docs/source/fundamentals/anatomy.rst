.. highlight: yml
.. _anatomy:

Anatomy of a complex recipe
###########################

In this section, we'll take a look at a fairly elaborate real-life recipe and explain its moving parts. This is the PARROT processing recipe used to process the data for the `RATT PARROT paper <https://academic.oup.com/mnras/article/528/4/6517/7598236>`_.

The full recipe and all supporting files are available at https://github.com/ratt-ru/parrot-stew-recipes/tree/parrot1. The top-level recipe is called ``image-parrot.yml``.


Preliminaries
*************

First, we have a shebang::

    #!/usr/bin/env -S stimela run

this is not necessary, but it's handy, as it allows the recipe file to be executed directly from the shell (if you chmod the recipe file to be executable), implicitly invoking ``stimela run``. Next, we have an include block::

    _include:
        - parrot-cabs.yml

This pulls in a `separate file <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/parrot-cabs.yml>`_ containing other standard includes and cab definitions::

  _include:
    (cultcargo):
      - wsclean.yml
      - casa-flag.yml
      - breizorro.yml
      ...

    omstimelation:
      - oms-cabs-cc.yml
      - oms-ddf-cabs.yml

  cabs:
    wget:
      command: wget
  ...

Option settings
****************

.. _log_options_example:

Next, we set set some Stimela configuration options. This is done by tweaking the ``opts`` section (see :ref:`options` for details)::

  ## this augments the standard 'opts' config section to tweak logging settings
  opts:
    log:
      dir: logs/log-{config.run.datetime}
      name: log-{info.fqname}
      nest: 2
      symlink: log
    backend:
      select: singularity
      singularity:
        auto_update: true
      rlimits:
        NOFILE: 100000  # set high limit on number of open files

The first of these have to do with :ref:`logfile handling <logfiles>` for details). Here, we're telling Stimela that:

1. We want the logs from every run placed into a subdirectory called ``./logs/log-DATETIME``. This way, logs from each run are kept separate.

2. We want logfiles to be split up by step, and named in a specific way (``log-recipe.step.txt``)...

3. ...but only to a nesting level of 2 -- that is, if a top-level step happens to be a sub-recipe with steps of its own, these nested steps will not have their own logfiles -- rather, all their output will be logged into the logfile of the outer step. If we wanted nested recipe steps to be logged separately, we could increase the nesting level.

4. We want a symlink named ``logs/log`` to be updated to point to the latest log subdirectory -- this allows for quick examination of logs from the latest run.

The ``backend`` options select the SIngularty backend, and set so process resource limits (see :ref:`backend_reference`). 

Body of the recipe
******************

Next, we have the body of the recipe proper::

    rrat:
        name: ratt-rrat
        info: imaging of RRAT follow-up

This starts with a section name. Because the section name is not one of the standard ones (i.e. `cabs`, `opts`), stimela will treat
it as a recipe definition. Note that it is also possible to define recipes directly in the library, like this::

  ratt-parrot:
    name: ratt-parrot
    info: "imaging of RRAT PARROT follow-up observations"

...and, in fact, any recipe section defined at the top level will be implicitly inserted into ``lib.recipes``. Defining things at top level is simply a :ref:`shortcut <config_loading_recipes>` that saves on indentation.

Then, we have an optional ``name`` attribute (if missing, the section name will be used as the recipe name), and an ``info`` string describing the recipe.


Recipe variables
****************

.. _log_options_dirout_example:

Next, we define some :ref:`variable assignments <assign>`. Note that all these variables are completely free-form and user-defined, with no particular meaning to Stimela itself. The point is to set up some consistent naming conventions for output files and dirctories, which we then apply throughout the body of the recipe::

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

* the above makes heavy use of :ref:`{}-substitutions <subst>` to define a set of naming conventions. For example, the ``recipe.dir-out`` variable (which this recipe uses consistently throughout to construct paths for output products) is formed up from a base directory (here set to "."), a subdirectory (defined via ``assign_based_on`` below), and an optional output suffix (defined as a recipe input below).

* assignments are re-evaluated (and thus resubstituted) at each recipe step. ``{info.suffix}``, for example, refers to the suffix of the current step's label. The recipe contains steps labeled ``image-1``, ``image-2``, etc. -- at each step, the ``recipe.image-prefix`` variable will be updated accordingly. Note also how this includes ``recipe.dir-out``.

* ``{config.run.datetime}`` fetches the timestamp of the Stimela run from the :ref:`configuration namespace <options>`. The assignment to ``log.dir`` results in :ref:`logfiles <logfiles>` being placed into a custom subdirectory (which is unique for each run, by virtue of having the timestamp included in its name). We're also telling Stimela that we want to keep logfiles inside ``recipe.dir-out``. 

* ``config.run.env`` contains all the shell environment variables, here we use ``HOME`` to get at the user's home directory.

Then, we include a few more variable assignments via the :ref:`assign-based-on trick <assign_based_on>`::

  assign_based_on:
    _include: parrot-observation-sets.yml

Inside `that file <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/parrot-observation-sets.yml>`you'll see a bunch of variable assignments based on the value of ``obs`` (which has the meaning of "observation ID", see below), including an assignment to ``band`` ("L" o "UHF"), which itself triggers a bunch of other assignments. This is a useful pattern for grouping observation-specific settings together and managing them in one place.

Recipe inputs
*************

Next, it's time to define the recipe's inputs::

  inputs:
    obs:
      choices: [L1, L2, L3, L4, U0, U1, U2, U3, U3b, U3c]
      info: "Selects observation, see parrot-observation-sets.yml for list of observations"
      default: L1
    output-suffix:
      dtype: str
      default: '-qc2'
    variant:
      dtype: str
      default: ''
    dir.out: 
      dtype: str
    ...

The only required input is ``obs``, which selects the observation to be processed. The ``assign_based_on`` section above relies on this input to set up a slew of other variables (including the MS name). The ``output-suffix`` and ``variant`` inputs are used to form up filename paths, as we saw above. Then, we have an inputs called ``dir-out``. This may look familiar -- you may have it being assigned to above. What is going on, and why are we assigning to a recipe's inputs? Recall that :ref:`inputs can be assigned to<assign>`; this is effectively a roundabout way of setting up a default value for them. Here, the intended routine usage of the recipe is to specify ``obs`` and have ``dir-out`` set up automatically via the ``assign_based_on`` section. However, ``dir-out`` remains a legitimate input, so the user may also specify it explicitly from the command line.

Aliases
*******

The aliases section links certain recipe inputs to inputs of particular steps::

  aliases:
    ms:
      - (wsclean).ms
      - (quartical).input_ms.path
    weight: 
      - (wsclean).weight
    minuv-l:
      - (wsclean).minuv-l
    taper-inner-tukey:
      - (wsclean).taper-inner-tukey

Since WSClean and QuartiCal steps recur throughout the recipe, this is a clean way to link some of their parameters to recipe inputs up front. Note how the ``(wsclean)`` syntax refers to "all steps using the ``wsclean`` cab". Throughout the rest of the recipe, you will often see parameter assignments such as ``ms: =recipe.ms`` for other cabs. This achieves the same effect as an alias, with the difference that aliases allow for a bit more up-front prevalidation. (The PARROT recipe could be overhauled to use more aliases in these cases, modulo better being the enemy of good.)

Note also that as a result of this aliases declaration, ``ms``, ``weight``, etc. become recipe-level inputs (see :ref:`aliases`). Recall that ``ms`` was assigned to based on the value of ``obs``,  similar to ``dir-out``. The user can still specify it manually from the command line.

Recipe steps
************

We now get to the business end of the recipe. We won't go through all of its many steps here, but rather highlight some of the more interesting ones that illustrate various Stimela features.

The `flag-save step <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/image-parrot.yml#L122>`_ is marked as always skipped (``skip: true``). Why so? This step captures the initial state of flags in the MS, and should only ever be run once per MS. The next step, ``flag-reset`` (not skipped!), resets the flags to the saved initial state. The idea here is, the very first time a user processes a particular MS, they should do an explicit ``stimela run -s flag-save`` to save the initial state. Subsequent re-runs of the same workflow can then start from a known set of flags.

The `image-1 step reuses <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/image-parrot.yml#L139>`_ a "template" step definition defined `elsewhere <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/parrot-cabs.yml#L454>`_, and augments it with some specific parameter settings::

    image-1:
      info: "auto-masked deep I clean"
      _use: lib.steps.wsclean.rrat
      params:
        column: DATA
        niter: 150000
        fits-mask: =IF(recipe.automask, UNSET, recipe.deep-mask-1)
        auto-threshold: 2

Given many repeated steps with lengthy yet similar parameter settings, this "template" pattern can reduce the recipe's complexity. You will see it recur in many of the subsequent steps.

The `mask-1 step <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/image-parrot.yml#L146>`_ invokes a sub-recipe::

   mask-1:
      recipe: make_masks
      params:
        restored-image: "{previous.restored.mfs}"
        prefix: "{previous.prefix}"

The sub-recipe is defined `here <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/parrot-cabs.yml#L357>`_.

A few steps down, we come to ``predict-copycol-3``. This illustrates a `conditional skip 
<https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/image-parrot.yml#L219>`_ based on a recipe input.

Another few steps down, we see conditional skips based on the :ref:`state of a step's outputs <skips>`::

   download-power-beam:
      cab: wget
      params:
        url: =recipe.mdv-beams-url
        dest: =recipe.mdv-beams
      skip_if_outputs: exist

    compute-power-beam:
      cab:  mdv-beams-to-power-beam
      params:
        mdv_beams: =recipe.mdv-beams
        power_beam: =recipe.power-beam
      skip_if_outputs: fresh

    derive-obs-specific-power-beam:
      cab: derive-power-beam
      params:
        cube: =steps.cube-3.cube
        images: =steps.image-3.restored.per-band
        outcube: =STRIPEXT(current.cube) + ".pbcorr.fits"
        power_beam: =recipe.power-beam
        beaminfo: "{steps.image-3.prefix}-powerbeam.p"
        nband: 128
      skip_if_outputs: fresh

This pattern comes in handy for relatively expensive steps that should only be re-executed if some of their inputs change. The ``skip_if_outputs: fresh`` directive makes Stimela behave in a way that is reminiscent of Unix `Makefiles <https://opensource.com/article/18/8/what-how-makefile>`_.

In passing, noe the ``=STRIPEXT(current.cube) + ".pbcorr.fits"`` pattern. This take the value of the ``cube`` input (a filename), removed the extension, and appends another extension to form up a value for the ``outcube`` output.

A few more steps down, we come onto an `example of the use of tags <https://github.com/ratt-ru/parrot-stew-recipes/blob/d012edc41096a1f143c216c424e9ecf896a9a171/image-parrot.yml#L436>`_::

    make-master-catalog:
      tags: [master-catalog, never]
      ...

    augment-master-catalog:
      tags: [master-catalog, never]
      ...

Tags are related to :ref:`skips <skips>`. They can be used to group related steps together, and invoke or skip them as a whole. In this case, we see the special ``never`` tag. This tells Stimela that these two steps are to be skipped *unless* explicitly invoked from the command line. The invoication can be done by specifything their other tag with ``-t``::

  stimela run image-parrot.yml -t master-catalog

or, more cumbesomely, using ``-s`` with the step labels::

  stimela run image-parrot.yml -s make-master-catalog -s augment-master-catalog

A bit further down we see another example of step tags::

      tags: [lightcurves]

Running::

  stimela run image-parrot.yml -t lightcurves

will skip the bulk of the recipe, only invoking the steps tagged with ``lightcurves``.

Looping recipes
***************

On a different subject, let's leave the PARROT and examine a `Jupiter imaging recipe <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/jove-pol.yml>`_ in the same repository, ``jove-pol.yml``. The structure of this recipe is broadly similar to the PARROT recipe above. It takes one MS and, after multiple rounds of selfcal, produces images. Note how its ``ms`` input is defined::

  inputs:
    ms:
      dtype: MS
      default: '{recipe.ms-base}-scan{recipe.scan:02d}.ms'
      aliases: ['*.ms']

This means that the user can specify an ``ms`` explicitly, or, alternatively, via two other inputs -- a base name and a scan number -- from which the corresponding MS name is constructed by default. (In passing, note also the use of ``aliases`` to link this input to all steps with an ``ms`` input.)

The interesting trick comes when we want to apply this recipe `to a series of MSs <https://github.com/ratt-ru/parrot-stew-recipes/blob/parrot1/jove-pol-loop.yml>`_. This is done by ``jove-pol-loop.yml``::

  jove-pol-loop:
    name: "Jove IQUV scan loop"
    info: "makes images with 1GC/DDCal for a series of scans, in full Stokes"

    for_loop:
      var: scan
      over: scan-list
      display_status: "{var}={value} {index1}/{total}"

    inputs:
      _include: jove-defaults.yml
      scan-list:
        dtype: List[int]
        default: [ 4, 6, 8, 11, 13, 15, 18, 20, 22, 24, 27, 29, 31, 34, 
                  36, 38, 41, 43, 45, 48, 50, 52, 55, 57, 59, 61, 64, 65 ] 

This tells Stimela that the recipe is a loop: the ``scan`` variable is to be iterated over values in ``scan-list``, which by itself is an input, with a default. (The ``display_status`` attribute tells Stimela how to format information for its status bar.) For each scan in the list, it involves two steps, passing the scan number (and base filenames) as inputs to the sub-recipe::

  steps:
    jove-prepare:
      ...

    jove-pol:
      recipe: jove-pol4
      params:
        ms-base: =recipe.ms-base
        dir-out-base: =recipe.dir-out-base
        scan: =recipe.scan

If the :ref:`Slurm backend is enabled <backend_reference>`, once could also add ``scatter: -1`` to the ``for_loop`` section so as to process all the iterations in parallel.

The above pattern represents a common scenario where the same workflow needs to be applied to a series of observations. Note how this structure allows for a straightforward invocation of the whole workflow or its individual parts::

  $ stimela run jove-pol.yml scan=11                  # process one scan (MS name formed up automatically)
  $ stimela run jove-pol.yml ms=my.ms                 # process one particular MS
  $ stimela run jove-pol-loop.yml                     # process all scans
  $ stimela run jove-pol-loop.yml scan-list=[4,6,8]   # process three particular scans

This would be an appropriate place to mention that Stimela also supports step selection within sub-recipes. If you want to run a speficic sequence of steps within ``jove-pol``, but over multiple scans, it could be done as e.g.::

  $ stimela run jove-pol-loop.yml scan-list=[4,6,8] -s jove-pol.mask-1:image-2

The ``STEP.FROM:TO`` syntax here selects a sequence of substeps (``mask-1`` through ``image-2``) from the nested recipe given by ``jove-pol`` step of the outer recipe.

In conclusion
*************

This concludes our brief tour of some real-life recipes. Hopefully, this has illustrated some good practices for recipe construction, as well as some advanced Stimela trickery and the right ways of employing it. We hope this material has been stimelating!




