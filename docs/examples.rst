********
Examples
********

Simulate observation and image PSF
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In this example, we will create simulated MeerKAT observations at declinations -90, -30, 0 and 20 degrees. We will then image these observations using three Briggs robust values. 


First, lets setup some global parameters for the recipe. Things set in `opts` are appplied across the board, unless explicitly changed within in a sub-section. For example, by setting `backend` to "native"

.. code:: yaml
  opts:
    backend: native
    log:
      dir: logs
    dist:
      max_workers: 4



.. code:: yaml
  vars:
    prefix: meerkat-psf-sim
    indir: input
    msdir: msdir
    outdir: output
    decs: [-90, -30, 0, 20]



