********
Examples
********

Simulate observation and image PSF
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In this example, we will create simulated MeerKAT observations at declinations -90, -30, 0 and 20 degrees. We will then image these observations using three Briggs robust values. 


First, lets setup some global parameters for the recipe. Things set in `opts` are appplied across the board, unless explicitly changed within in a sub-section. For example, setting `log.dir` to "logs" means that all log files will be saved in directory `./logs`, and setting `backend` to native means that cabs will be executed using applications installed on the host system (not in a container).

.. code:: yaml
  opts:
    backend: native
    log:
      dir: logs
    dist:
      max_workers: 4

We can also set variables that can be used by recipes. These can be accessed through `$(vars.<variable name>)`

.. code:: yaml
  vars:
    prefix: meerkat-psf-sim
    indir: input
    msdir: msdir
    outdir: output
    decs: [-90, -30, 0, 20]

Now, lets start the recipe

.. code:: yaml
  simvis_and_image_psf:
    inputs:
      dec:
        dtype: float
        default: -30
      ra:
        dtype: str
        default: "0deg"
      prefix:
        dtype: str
        default: "{vars.prefix}"
      obstime: # in hours
        dtype: float
        default: 2 # in hours
      dtime:
        dtype: int
        default: 2 # in seconds 
      msdir:
        dtype: Directory
        default: "{vars.msdir}"
      outdir:
        dtype: Directory
        default: "{vars.outdir}"
      indir:
        dtype: Directory
        default: "{vars.indir}"

    steps:
      simvis:
        cab: simms
        params:
          telescope: meerkat
          ms: "{recipe.msdir}/{recipe.prefix}_dec{recipe.dec}.ms"
          direction: "J2000,{recipe.ra},{recipe.dec}d"
          synthesis: "{recipe.obstime}"
          dfreq: 1MHz
          nchan: 5
          freq0: 580MHz
      image_psf:
        cab: wsclean
        params:
          ms: "{recipe.simvis.ms.path}"
          name: "{recipe.outdir}/{recipe.ms.basename}"
          scale: 1asec
          size: 2048
          weight: briggs 0.3
          psf-only: true
          


