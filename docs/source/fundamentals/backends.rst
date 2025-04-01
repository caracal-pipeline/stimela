.. highlight: yml
.. _backends:

Backends and Images
===================

Stimela recipes may be executed on a variety of backends:

* The **native** backend directly executes software installed on the native host OS (optionally, in a Python virtual environment). Of course, this requires that the software package in question be already installed, which can be a hassle. However, it does provide  flexibility in a development environment where things need to be tested quickly.

* The **Singularity** backend uses Singularity/Apptainer to run the step in a container (using an application image specified by the cab definition). The ``cult-cargo`` package provides an image registry for all the cabs that it bundles, which means Stimela can download the images on demand, without user intervention. This provides a zero-install experience for the user (only Stimela itself, and Singularity, needs to be available on the system), and ensures maximum reproducibility.

* The **Kubernetes** backend runs each step as a *pod* on a Kubernetes (k8s) cluster. This also uses images from the registry. This backend is a modern way to provide scalable and reproducible workflows, but typically requires more setting up on the part of the cluster administrator.

* The **Slurm** backend (strictly speaking, a wrapper around the Singlarity and/or native backends) will execute the steps as jobs on a Slurm cluster.

The current backend can be selected by setting a global Stimela config option, namely::

    opts:
        backend:
            select: singularity

in the recipe or configuration file. Note that ``select`` may also be set to a list::
                
    opts:
        backend:
            select: 
                - singularity
                - native

to select backends in order of preference (i.e. use Singularity if available, else fall back to native). Alternatively, the ``-N/--native``, ``-S/--singularity``, and ``-K/--kube`` options of the ``stimela run`` command have the same effect. The Slurm wrapper can be enabled via::

    opts:
        backend:
            slurm:
                enable: true 

or by passing ``--slurm`` to ``stimela run``. Note that the Slurm wrapper only works with the native or Singularity backends.

Building images
---------------

The Singularity backend works with local copies of application images (in SIF format) that are built from Docker-format images downloaded from a remote Docker registry (such as ``quay.io``, which hosts the ``cult-cargo`` images). 

By default, images are built on-demand (i.e. just before a step is executed, if a local SIF image for that step's cab doesn't already exist). Images are versioned, and version information is included in the cab definition. Thus, if you install an updated version of e.g. ``cult-cargo``, Stimela will usually know when it needs to download and build new versions of images.

It is also possible to pre-build all the images that a given recipe needs. This is as simple as running::

    $ stimela build recipe.yml

Stimela will then load the recipe, scan through all the steps, and issue build commands for the required images. It will skip any images that already exist (with the appropriate version). Use the ``-r`` option to do a fresh rebuild of all images.

Running different images
------------------------

If alternative versions of an image are provided (as is the case for many packages shipped via cultcargo), one can tweak the cab definition to use a different image. The :ref:`include-and-merge semantics<include_merge>` of the ``_include`` statement make this easy::

    _include:
        - (cultcargo)wsclean.yml

    cabs:
        wsclean:
            image:
                version: 3.0.1-cc0.2.0

To find available alternative versions, you may consult the package's `image manifest <https://github.com/caracal-pipeline/cult-cargo/blob/master/bundle-manifest.md>`_. Cultcargo uses a specific naming convention for image versions -- an underlying package version, followed by a bundle version suffix (``-cc0.2.0`` for cultcargo 0.2.0). The default image version is usually just the bundle version itself (e.g. ``cc0.2.0``); this usually corresponds to the most recent/stable version of the underlying package at the time of that specific cultcargo release.

The full image section consists of ``repository``, ``name`` and ``version`` components, and these may be modified individually as needed. If you want to run an image from an alternative source, you may specify e.g::

    cabs:
        wsclean:
            image:
                registry: quay.io/myorg
                name: myimage
                version: latest

This will use ``quay.io/myorg/myimage:latest`` to download the image.

Finally, if you have built e.g. a local Apptainer/Singularity image, and want to run that instead, you may specify a path to it as follows::

    cabs:
        wsclean:
            image:
                path: /path/to/image.sif 

The ``path`` setting then overrides all other ``image`` attributes.



Tweaking backend settings, all the way down
-------------------------------------------

Backends have different settings that can be set under the corresponding ``opts.backend`` section. These are described in detail under :ref:`backend settings<backend_reference>`. 

Stimela allows for mixing-and-matching of backends within a recipe. This is not something that would be useful (or indeed encouraged) in a stable production workflow, but it can be a valuable development and experimentation tool.

Crucially, backend settings can be tweaked on a per-recipe, per-cab and per-step basis, respetively. Recipes, cabs and steps all recognize an optional ``backend`` section for this purpose. Before a step is executed, Stimela will take the global backend settings (``opts.backend``), merge in the current recipe backend settings, merge in the cab backend settings, and then merge in the step backend settings, if any. The resulting merged setting are then applied to the step being executed.

A typical use case (as well as a suggested best practice) for this would be as follows. Let's say you have a recipe defined in ``recipe.yml``. Ideally, this should not specify any backend settings at all -- a recipe file should define only the logical sequence of steps in the workflow, not the specific means of executing them. You are free to switch backends by simply specifying a command-line option to ``stimela run``. A typical user would run everything via the Singularity backend -- the most hassle-free route.

Now, let's say you have a long workflow you've been running via Singularity, but you want to test it with a new experimental version of ``breizorro``, for which no Singularity image is yet provided. You've installed this version in a local virtual environment, and you want to switch this into your workflow. Stimela makes this pretty straightforward. Recall that ``stimela run`` can take multiple YAML files as arguments, merging them together in order. You can therefore create a file named, say, ``tweaks.yml``, containing::

    opts:
        backend:
            select: 
                singularity
    
    cabs:
        breizorro:
            backend:
                select: 
                    native:
                native:
                    virtual_env: ~/venv/breizorro

Now, when you ``stimela run recipe.yml tweaks.yml``, your entire workflow will execute via Singularity, except for any steps invoking the ``breizorro`` cab, which will use the native backend instead.

.. _backends_slurm_tweaks:

Another use case for this is tweaking Slurm settings. Different steps in the workflow will have different CPU and RAM requirements, and it can be helpful (or sometimes even vital) to make the Slurm scheduler aware of this. Let's say you want run your WSClean jobs with a 128G memory requirement and 32 cores, one particularly expensive step with a 256G memory requirement and 64 cores, and everything else with 32G of memory and 1 core. Your ``tweaks.yml`` would then contain something like::

    opts:
        backend:
            slurm:
                enable: true
                srun_opts:
                    mem: 32G
                    cpus-per-task: 1
    
    cabs:
        wsclean:
            backend:
                slurm:
                    srun_opts:
                        mem: 128G
                        cpus-per-task: 32
    
    my-recipe:
        steps:
            memory-expensive-step:  # well, whatever the step is labelled   
                backend:
                    slurm:
                        srun_opts:
                            mem: 256G
                            cpus-per-task: 64











