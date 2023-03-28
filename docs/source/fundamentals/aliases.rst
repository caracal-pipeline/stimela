.. highlight: yml
.. _aliases:

Aliased inputs/outputs
######################

An *alias* is a recipe input or output (IO) that directly maps onto a step's input or output. Consider this abbreviated version of our notional calibration recipe::

  calibration-recipe:
      inputs:
          ms:
              dtype: MS
              required: true
              info: "measurement set to use"
          image-size:
              dtype: int 
              default: 4096
              info: "image size, in pixels"
      steps:
          image:
              cab: imager-tool
              params:
                  ms: =recipe.ms
                  size: =recipe.image-size
          predict:
              cab: imager-tool
              params:
                  ms: =recipe.ms
                  mode: predict
                  model: =previous.output.model
                  column: MODEL_DATA
          calibrate:
              cab: calibration-tool
              params:
                  ms: =recipe.ms
                  model.column: =steps.predict.column
                  output.column: CORRECTED_DATA

Note how the ``ms`` parameter of each step is set to ``recipe.ms`` using :ref:`formula evaluation<subst>`. Remembering the :ref:`Tim Toady<timtoady>` philosophy, Stimela provides other ways of accomplishing the same result. One obvious alternative is a {}-substitution, i.e. ``"{recipe.ms}"``. This works in a somewhat less direct manner, although the difference is subtle: the formula ``=recipe.ms`` directly refers to the ``ms`` input of the recipe, while the {}-substitution evaluates to a string which just happens to be the value of the ``ms`` input. Either way, Stimela can't prevalidate the steps' ``ms`` setting up front, since evaluations and substitutions only happen at runtime, directly before a step is executed.

A more rigorous way (in the sense that user errors can be caught earlier) to achieve the same result is to specify aliases for the inputs::

    calibration-recipe:
        inputs:
            ms:
                dtype: MS
                required: true
                info: "measurement set to use"
                aliases: [image.ms, predict.ms, calibrate.ms]
            image-size:
                dtype: int 
                default: 4096
                info: "image size, in pixels"
                aliases: [image.size]
        steps:
            image:
                info: "make initial image and model from DATA column"
                cab: imager-tool
            predict:
                info: "predict model into MODEL_DATA"
                cab: imager-tool
                params:
                    mode: predict
                    model: =previous.output.model
                    column: MODEL_DATA
            calibrate:
                info: "calibrate model against data"
                cab: calibration-tool
                params:
                    model.column: =steps.predict.column
                    output.column: CORRECTED_DATA

An alternative syntax for this is a separate ``aliases`` section::

    calibration-recipe:
        inputs:
            ms:
                dtype: MS
                required: true
                info: "measurement set to use"
            image-size:
                dtype: int 
                default: 4096
                info: "image size, in pixels"
        aliases:
            ms: [image.ms, predict.ms, calibrate.ms]
            image-size: [image.size]

In both cases, we're telling Stimela that the ``ms`` input of the recipe must be passed to the ``ms`` inputs of the three given steps, and that the ``image-size`` input must be passed to the ``size`` input of the ``image`` step. This is a more rigorous way of linking IOs because Stimela can now check that the schema (i.e. types) of the IOs all match each other, and it can do this up front, during recipe prevalidation. You can think of the aliasing way as a "hard link", and the formula or substitution way above as a "soft link".

(NB: when using the latter syntax, the recipe need not even declare the aliased IOs at all -- Stimela will copy the schema for the IO from the schema of the (first) step IO being aliased. Declaring the schema at recipe level, however, allows you to override the info string and the default value.)

Wildcard aliases
----------------

The step label of the alias target may contain "*" and "?" wildcards that will be matched against step labels::

    calibration-recipe:
        aliases:
            ms: [*.ms]
            imaging-weight: [image-*.weight]

This tells Stimela that the recipe's ``ms`` input is an alias for the ``ms`` input of all steps (all steps that have an ``ms`` input, to be more precise), and that the ``imaging-weight`` input is an alias for the ``weight`` input of all steps whose label matches ``image-*``. Another option, the ()-form, matches all steps that invoke a particular cab::

        aliases:
            imaging-weight: [(wsclean).weight]

Auto-aliases
------------

Consider the following notional recipe::

    cabs:
        make-image:
            outputs:
                name:
                    dtype: File
                    required: true
        threshold-image:
            inputs:
                input-image:
                    dtype: File
                    required: true
                threshold:
                    dtype: float
                    required: true
                option-foo:
                    required: false
                option-bar:
                    default: x
            outputs:
                output-image:
                    dtype: File
                    required: true

    image-recipe:
        steps:
            make:
                cab: make-image
            threshold:
                cab: threshold-image
                params:
                    input-image: =previous.name
                    output-image: =STRIPEXT(current.input-image) + ".threshold.fits"

The first step is missing a required ``name`` parameter. The second step is missing a required ``threshold`` parameter, while its other two required parameters are specified (a.k.a. *bound*) within the recipe definition. This recipe is nonetheless perfectly valid -- Stimela will implicitly create automatic aliases for the missing parameters. These aliases will be named using the step label and the parameter name, i.e. the recipe will automatically acquire a ``make.name`` output and a ``threshold.threshold`` input, with schemas copied over from the appropriate cab schemas. In fact, **all** unbound step parameters become recipe-level inputs or outputs via auto-aliasing. That is, the recipe above will also have a ``threshold.option-foo`` input and a ``threshold.option-bar`` input. There are three categories of such auto-aliases:

* **Required:** unbound step parameters that are marked as required by the schema. These must be supplied by the user in order to run the recipe.
* **Obscure:** unbound step parameters that are not marked as required. 
* **Hidden:** unbound step parameters that have a default value defined.

These categories only matter for documentation purposes, that is, ``stimela doc`` will not display obscure or hidden parameters unless the ``-O`` or ``-A`` option is given.






