.. highlight: yml
.. _basics:

Basics
######

Stimela is *workflow management* framework. The two basic elements of Stimela are *cabs* and *recipes*.

* **Cabs**: a cab is an atomic task that can be invoked in a workflow. Cabs come in a few different :ref:`flavours <cabdefs>`: 

  * an executable command
  
  * a Python function
   
  * a snippet of Python code  
   
  * a CASA task
     
  A *cab definition* (see :ref:`cabdefs`) is a YAML document that tells Stimela how to invoke the task, and what its inputs and outputs are (this is collectively known as a *schema*). You can write your own cab definitions, and mix-and-match them with standard cabs shipped in Stimela's companion ``cult-cargo`` package.

  
* **Recipes**: a recipe is a YAML document describing a workflow, in terms of a sequence of **steps**. Each step invokes a cab, or another recipe (a.k.a. nested recipe). Steps have *parameters* which are matched (*validated*) against the schema of the underlying cab. Stimela provides a number of powerful mechanisms to pass parameters between steps.
  
  Recipes also have inputs and outputs, described by the recipe's *schema* (which is what allows them to be nested.)

Cabs and recipes cab be executed natively (i.e. directly on the host OS), inside a Python virtual environment, inside a container, on a Kubernetes cluster, and/or as a Slurm job. See :ref:`backends <backends>` for more detail.


Stimela and cult-cargo
======================

Stimela by itself does not predefine any cabs. Instead, the idea is that separate packages such as ``cult-cargo`` (just use pip install) provide cab collections that the user can employ. ``cult-cargo`` is a curated set of cabs for radio interferometry software, maintained by the Stimela developers. Users are also free to roll their own cab definitions, and provide their own cab collections as installable packages.


Anatomy of a simple recipe
==========================

Here is a (rather notional and idealized) recipe::

  #!/usr/bin/env -S stimela run
  ### (the above is just a handy trick that lets us execute the recipe file directly)

  # include some of my cab definitions
  _include:
      - mycabs.yml

  # set some stimela options
  opts:
      log:
          dir: .logs  # changes the logfiles directory

  # this recipe is named thus
  calibration-recipe:
      info: "a notional recipe for calibration & imaging"
      
      # this recipe has some input parameters
      inputs:
          ms:
              dtype: MS
              required: true
              info: "measurement set to use"
          image-name:
              dtype: str
              required: true
              info: "base name for output images"
          image-size:
              dtype: int 
              default: 4096
              info: "image size, in pixels"

      # this recipe consists of three steps, "image", "predict" and "calibrate"
      steps:
          image:
              info: "make initial image and model from DATA column"
              # this is the unerlying tool that the step invokes (defined in mycabs.yml, presumably)
              cab: imager-tool
              # and these are the parameters of the step...
              params:
                  ms: =recipe.ms
                  mode: image
                  size: =recipe.image-size * 2
                  column: DATA
                  output.image: '{recipe.image-name}.image-{info.suffix}-{current.size:05d}.fits'
                  output.model: '{recipe.image-name}.model-{info.suffix}.fits'
          predict:
              info: "predict model into MODEL_DATA"
              cab: imager-tool
              params:
                  ms: =recipe.ms
                  mode: predict
                  model: =previous.output.model
                  column: MODEL_DATA
          calibrate:
              info: "calibrate model against data"
              cab: calibration-tool
              params:
                  ms: =recipe.ms
                  model.column: =steps.predict.column
                  output.column: CORRECTED_DATA
  
The following sections will explain what's going on in more detail.

