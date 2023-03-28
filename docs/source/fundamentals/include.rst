.. highlight: yml
.. _include:

Modularity: use and include
###########################

Stimela implements a couple of extensions to the YaML parser to support modularity. For example, the ``cult-cargo`` package (a companion to stimela) contains standard, curated cab definitions for common radio astronomy software packages. On top of that, you may want to provide your own collections of cabs, or use your colleagues's cab collection, or just split your recipe into a set of standard reusable modules.

Includes
--------

The lynchpin to modularity is the special ``_include`` section. This gives a list of YaML files which are read in and merged at the point of invocation. For example, you may have your own set of custom cabs defined in ``my-cabs.yml``::

    cabs:
        foo:
            command: foo
            inputs:
                ...
        bar:
            command: bar
                ...

If you want to use these cabs in your recipe, you'll need to add this at the top of the recipe::

    _include: my-cabs.yml

This will cause stimela to read in ``my-cabs.yml``, and paste in the content as if it was part of the recipe in the first place. Of course, you might want to use some ``cult_cargo`` cabs alongside your custom ones, in which case your include section will look something like::

    _include: 
        - (cult_cargo)wsclean.yml
        - my-cabs.yml

The ()-form tells stimela to look for ``wsclean.yml`` inside the Python package named ``cult_cargo``, wherever that happens to reside. You will have presumably installed it via ``pip`` in the usual way, and you don't actually need to know where exactly it is installed, as stimela will take care of finding it for you using the standard Python machinery. In the second case, stimela will look for ``my-cabs.yml`` in the current directory, then in a few standard locations such as ``~/lib/stimela``. You can set the ``STIMELA_INCLUDE`` environment variable to specify a custom set of paths to look in.

Includes are recursive
^^^^^^^^^^^^^^^^^^^^^^

Includes can be recursive. In real life, your ``my-cabs.yml`` might actually look like::

    _include:
        - (cult_cargo)wsclean.yml

    cabs:
        foo:
            command: foo
            inputs:
                ...

...and your top-level recipe then only needs to include ``my-cabs.yml`` -- the ``wsclean`` cab will come along for the ride.  

Includes are a "merge"
^^^^^^^^^^^^^^^^^^^^^^

Include works in the sense of a "merge" (or a "union") with prior content. Consider that in the example above, both ``my-cabs.yml`` and ``wsclean.yml`` will contain a ``cabs`` section with different content. The resulting ``cabs`` section available to stimela will contain subsections from both of the included files -- the two ``cabs`` sections will have been merged. What happens when the contents clash? The answer is that everything is merged. 

More strictly, subsequent content *augments* previously included content: section content is merged, while any "leaf" items are overwritten. This is a very powerful feature. For example, imagine that you want to try an unreleased version of WSClean, which you've built from the latest source under ``~/src/wsclean/build``. Furthermore, the ``cult_cargo`` definition of the ``wsclean`` cab doesn't know about a new parameter that is available in the new build, but it is something that you need to use. Also, you'd like to modify the default value of a standard parameter. You can simply augment the cult ``wsclean`` definition by putting this into ``my-cabs.yml``::

    
    _include:
        - (cult_cargo)wsclean.yml

    cabs:
        wsclean:
            command: ~/src/wsclean/build/wsclean  # my own build
            inputs:
                previously-defined-paramater:
                    default: "my own default value"
                new-parameter:
                    dtype: int 
                    info: "I added this parameter, since it was missing in cult_cargo"
        foo:
            command: foo
            inputs:
                ...

Equivalently, you can do the augmentation by providing a ``cabs: wsclean`` section directly in the recipe file.

Includes can also be nested
^^^^^^^^^^^^^^^^^^^^^^^^^^^

As mentioned above, includes are processed at point of invocation. This means that subsections can contain their own ``_include`` that is processed at that subsection's level. You could choose to rewrite ``my-cabs.yml`` without an enclosing ``cabs`` section, like so::

    foo:
        command: foo
        inputs:
            ...
    bar:
        command: bar
            ...

...and then include it in your recipe like so::

    cabs:
        _include: my-cabs.yml

We don't necessarily advocate doing this for cab definitions, as this can make them confusing and less reusable. There are, however, other instances where breaking out a subsection into an include can make things neater (see :ref:`anatomy` for an example.)


Use: reusing content
--------------------

The special ``_use`` section is closely related to ``_include``, but instead of pulling in YaML files, it copies in previously defined sections. A typical use case for ``_use`` (excusing the pun) is "library" content. You'll want to use ``_use`` (excusing the pun) if you find yourself often repeating identical bits of YaML. For example, if your recipe contains multiple imaging steps where you invoke the imager with a largely the same set of parameters, you can avoid repetition like so::

    calibration-recipe:
        info: "a notional recipe for calibration & imaging"
        ...
        steps:
            image-1:
                info: "make initial image and model from DATA column"
                cab: imager-tool
                params:
                    ms: =recipe.ms
                    mode: image
                    size: =recipe.image-size * 2
                    column: DATA
                    output.image: '{recipe.image-name}.image-{info.suffix}-{current.size:05d}.fits'
                    output.model: '{recipe.image-name}.model-{info.suffix}.fits'
            ...
            image-2:
                _use: calibration-recipe.steps.image-1
                info: "make image from calibrated data column"
                params:
                    column: =steps.calibrate.output.column
                    output.image: '{recipe.image-name}.image-{info.suffix}.fits'

Here, the definition of the ``image-1`` step is copied over into ``image-2``, then tweaked. Note how the merge-and-augment semantics are exactly the same as for ``_include``. That is, subsections are merged, and "leaf" values are modified. 

In passing, we should note that both ``_include`` and ``_use`` will accept either a single string (interpreted as a filename, or a section name), or a sequence of strings. In the latter case, the sequence is treated as multiple filenames (section names), which are all merged together in order.

An alternative way to modularize the above is to use the standard ``lib`` namespace of stimela. In particular, ``lib.steps`` is meant to contain reusable step definitions. You could recast the above in terms of a "standard" imager invocation, by incuding something like this in ``my-cabs.yml``::

    lib:
        steps:
            standard-imaging:
                cab: imager-tool
                params:
                    ms: =recipe.ms
                    mode: image
                    output.image: '{recipe.image-name}.image-{info.suffix}-{current.size:05d}.fits'
                    output.model: '{recipe.image-name}.model-{info.suffix}.fits'

Your recipe file could then reuse this step definition like so::

    calibration-recipe:
        info: "a notional recipe for calibration & imaging"
        ...
        steps:
            image-1:
                _use: lib.steps.standard-imaging
                info: "make initial image and model from DATA column"
                params:
                    size: =recipe.image-size * 2
                    column: DATA
            ...
            image-2:
                _use: lib.steps.standard-imaging
                info: "make image from calibrated data column"
                params:
                    column: =steps.calibrate.output.column


Scrubs
------

The sharp-eyed reader will have spotted one limitation to the merge-and-augment semantics of ``_include`` and ``_use``. Any subsections brought in by these statements can be added to, and leaf items can be overwitten, but it's one-way traffic -- nothing can be removed.

The ``_scrub`` keyword is provided to overcome this restriction. Any section listed in ``_scrub`` will be removed from anything brought in by ``_include`` or ``_use``. A (rather futile) example would be::

    _include:
        - (cult_cargo)wsclean.yml
    _scrub:
        - cabs.wsclean

This will pull in the (presumed) WSClean definition from ``cult-cargo``, them proceed to remove it (presumably remove it, as the cult definiton could contain more than than ``cabs.wsclean``). A somewhat more useful example would be if one wanted to completely redefine a WSClean input (as opposed to tweaking the standard definition, as above)::

    _include:
        - (cult_cargo)wsclean.yml
    _scrub:
        - cabs.wsclean.inputs.redefined-parameter

    cabs:
        wsclean:
            inputs:
                redefined-paramater:
                    dtype: int
                    default: 0
                    info: "this input is redefined from scratch"

Scrubbing tends to be even more useful in step definitions. If one wanted to define a step based on a previous step (or a template from ``lib.steps``), *minus* some parameters, *plus* some parameters, ``_scrub`` is the way to do it.


Best practices?
---------------

The ``_use`` and ``_include`` features offer one a lot of rope, and even as the developers, we are still figuring out the best ways of deploying them. Modularity is more often a matter of custom and taste. We can only offer general advise:

* Repetition is annoying, and tends to lead to cut-and-paste, which often leads to errors. Use the ``lib`` namespace with ``_use`` and ``_include``! 

* "Make everything as simple as possible, but not simpler." *(Albert Einstein.)* Which is a fine ethos, but then "Radio interferometry is death by a million papercuts." *(Jan Noordam.)* And finally, "Code is read more often than it is written." *(Guido van Rossum.)* So, considering these pearls of wisdom from a German and two Dutchmen:

  * A good top-level recipe should convey the essense of what is being done, without going into unnecessary detail.

  * Detail should be hidden in ``lib`` and brought in via ``_use`` and ``_include``.

  * ...but without overuse (pardon the pun). One should not need to dive through multiple levels of include files to figure out where a particular step's parameter is coming from. A single level is OK, two levels or more need to be considered carefully.

  * Tweaking things like cab definitions at the recipe level is simple and powerful, and can be necessary, but again, don't overuse it. 

Deployed sensibly, ``_use`` and ``_include`` provide ways of specifying common settings in a single place. Following the "plurality of means to peel a feline" ethos, stimela provides other ways, such as :ref:`assign` and :ref:`aliases`. Again, we do no mean to 
suggest a single one, but rather leave it to experience to come up with :ref:`best_practices`. 

As far as basic modularity goes, a sensible workflow that works well for one of the developers runs as follows:

  * Project-specific recipes live in their own repository, along with a few project-specific cab and step definitions (e.g. ``rrat-cabs.yml``).
  * More generic cab definitions live in a separate repository (https://github.com/o-smirnov/omstimelation). The project-specific cabs include this as ``_include: omstimelation/oms-cabs.yml``.
  * ``oms-cabs.yml`` includes ``cult_cargo``.
  * Cab definitions can be promoted "upstream". That is, some project-specific tools initially wrapped in ``rrat-cabs.yml`` eventually get generic enough to graduate to ``omstimelation``. If they are even more generic, they can be considered for submission to ``cult-cargo``.


