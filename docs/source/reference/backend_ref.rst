.. highlight: yml
.. _backend_reference:

Backend settings
================

See :ref:`backends` for a top-level overview. Backend settings are controlled via the ``opts.backend`` section of the configuration namespace, and can be augmented on a per-recipe, per-cab and per-step basis, by defining a separate ``backend`` section therein with the required subset of settings.

The ``backend`` section defined separate sub-sections per each backend, described below, and a few top-level options:

* ``select``: a string or a list of strings specifying the backend(s) to use. The first available backend listed will be used (note that the native backend is always available, while Singularity and Kubernetes are contingent on the respective packages being installed). 

* ``default_registry`` to use if no registry is specified in a cab's image definition.

* ``override_registries`` can be used to replace one image registry with another. This is useful is you have a local registry that functions as a pull-through caches. For example::

    backend:
        override_registries:
            quay.io/stimela2: 800133935729.dkr.ecr.af-south-1.amazonaws.com/quay/stimela2

* ``verbose``: increasing this number produces more log messages from the backends -- useful for debugging.

* ``rlimits`` can be used to set various resource limits during the run. E.g. to increase the max number of open files, use::

        backend:
            rlimits:
                NOFILE: 10000
    
  See https://docs.python.org/3/library/resource.html for details: all of the symbols starting with ``RLIMIT_`` are recognized and applied. Note that rlimits only apply to the native and Singularity backends running locally -- Kubernetes and Slurm have their own resource management options. 

See also `comments in the source code <https://github.com/caracal-pipeline/stimela/blob/4344313b23cfca119e117fdf5d734334cc254bcf/stimela/backends/__init__.py#L44>`_ for more information.


Native backend settings
-----------------------
.. _native_backend_reference:

The native backend has only a couple of settings::

    backend:
        native:
            enable: true
            virtual_env: ~/venvs/my_venv

It is enabled by default. The optional ``virtual_env`` setting activates a Python virtual environment before running commands. This can be useful to tweak on a per-cab basis, when playing with experimental cabs.


Singularity backend settings
----------------------------
.. _singularity_backend_reference:

The Singularity backend has the following settings::

    backend:
        singularity:
            enable: true
            image_dir: ~/.singularity
            auto_build: true
            rebuild: false
            executable: PATH
            remote_only: false
            contain: true
            contain_all: false
            bind_tmp: true
            env:
                VAR: VALUE
            bind_dirs:
                arbitrary_label:
                    host: PATH
                    target: PATH
                    mode: rw
                    mkdir: false
                    conditional: ''

The backend is enabled by default, if the ``singularity`` executable (or whatever is specified by the ``executable`` setting) is found in the path. Set ``enable`` to false to disable.

Singularity works with local copies of application images (in SIF format) that can be built from Docker-format images served by a remote Docker registry. The ``image_dir`` setting determines where these SIF images are cached. If ``auto_build`` is set, Stimela will attempt to build any missing Singularity images on-demand. If ``rebuild`` is set, it will rebuild images anew even if they are already present. (Note that in a cluster environment, it may be useful to disable auto-build, and work with prebuilt images only. The ``stimela build`` command can be used to pre-build images.)

``remote_only`` tells Stimela to not bother checking for a local install of Singularity. This can be useful in combination with Slurm, if the login node (or whatever node Stimela is executed on) does not support Singularity, but the compute nodes on which jobs are scheduled do.

Containers are normally run with the ``--contain`` flag (see Singularity documentation: this isolates the container from the host filesystem). This is the recommended setting. You may choose for more strict isolation by setting ``containall: true`` (which runs with the ``--containall`` flag), or disable isolation altogether via ``contain: false``. (The latter is not recommended, for the sake of repeatable workflows.) 

The optional ``env`` subsection can be used to setup additional environment variables inside the container.

Binding container directories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``contain`` or ``containall`` are in effect, the container does not see the host filesystem, except for directories that are explicitly bound by Stimela. By default, these are derived from the file- and directory-type inputs and outputs of the cab. If additional directories need to be mounted, this can be specified by adding entries to the ``bind_dirs`` subsection.

Each entry in ``bind_dirs`` is a subsection with an arbitrary label, containing, as a minimum, a ``host`` path, and a ``target`` path (i.e. inside the container) if this needs to be different. A special case of ``host: empty`` refers to an empty temporary directory (in which case ``target`` is required). ``mode`` can be set to ``ro`` for a read-only bind. With ``mkdir: true``, Stimela will create a host directory if it doesn't exist. Finally, the bind can be made ``conditional`` using a :ref:`substitution or formula expression <subst>` that is evaluated when each step is run.

With ``bind_tmp: true``, an empty temporary directory on the host is bound to ``/tmp`` inside the container. This is normally a sensible thing to do, so this is the default setting.


Slurm wrapper settings
----------------------------
.. _slurm_backend_reference:

Slurm is a wrapper, not a backend per se. It can be used in combination with the native and Singularity backends to schedule steps as Slurm jobs (using ``srun``). Enabling it can be as simple as setting ``enable`` to true::

    backend:
        slurm:
            enable: false
            srun_path:              # optional path to srun executable
            srun_opts: {}           # extra srun options
            srun_opts_build: {}     # extra srun options for build commands
            build_local: true


provided you're running in a cluster environment where Slurm is configured. Instead of running a step locally, Stimela then invokes `srun <https://slurm.schedmd.com/srun.html>`_ to pass the job off to Slurm, and waits for ``srun`` to finish. 

A typical usage scenario is running Stimela on the cluster login (head) node, in a persistent console session (using ``tmux`` or ``screen``). The Stimela process itself is pretty lightweight and can be executed on the login node, while every step of the workflow is passed off to Slurm.

The `srun command <https://slurm.schedmd.com/srun.html>`_ has a veritable cornucopia of options controlling all aspects of job and resource management. Any of these can be configured here: Stimela will blindly pass through the contents of the ``srun_opts`` mapping (prepending a double-dash to each mapping key). An example of using this feature to tweak CPU and RAM allocation is discussed :ref:`here <backends_slurm_tweaks>`.

If Singularity images need to be built, Stimela will schedule the ``singularity build`` command via ``srun`` as well, unless ``build_local`` is set to true, in which case ``singularity build`` will execute on the same node that Stimela is running on. If builds are being done via ``srun``, then you can control its options via the ``srun_opts_build`` mapping. If this is not provided, ``srun_opts`` are used instead.


Kubernetes backend settings
----------------------------
.. _kube_backend_reference:

The Kubernetes backend can be pretty arcane to configure, and is still under active development at time of writing. The best reference for its options are the `comments in the source code <https://github.com/caracal-pipeline/stimela/blob/4344313b23cfca119e117fdf5d734334cc254bcf/stimela/backends/kube/__init__.py#L68>`_. Here are some settings from a working example::

    opts:
        backend:
            kube:
                context: osmirnov-rarg-test-eks-cluster         # k8s context to run in, this determines which cluster to connect to etc.
                
                debug:  # options useful during debugging
                    verbose: 0       
                    log_events: 1                               # logs all k8s events to Stimela
                    save_spec: "kube.{info.fqname}.spec.yml"    # saved pod manifests for inspection
                
                dir: /mnt/data/stimela-test                     # directory in which the workflow runs

                volumes:   # this defines filesystem volumes of each pod
                    rarg-test-compute-efs-pvc:                  # this is a k8s PersistentVolumeClaim
                        mount: /mnt/data                        # ...which is mounted here in the pod
                        at_start: must_exist

                provisioning_timeout: 0                         # timeout (secs) to start a pod before giving up, 0 waits forever
                connection_timeout: 5                           # timeout (secs) to restore lost connection
                
                # this is the UID/GID that the pod will run as
                user:
                    uid: 1000
                    gid: 1000

                # RAM limit -- should be tweaked per-cab and per-step, really
                memory:
                    limit: 16Gi

                # some predefined pod specs. Keys are labels -- content is determined by the k8s cluster administrator
                predefined_pod_specs:
                    admin:
                        nodeSelector:
                            rarg/node-class: admin
                    thin:
                        nodeSelector:
                            rarg/node-class: compute
                            rarg/instance-type: m5.large
                    medium:
                        nodeSelector:
                            rarg/node-class: compute
                            rarg/instance-type: m5.4xlarge
                    fat:
                        nodeSelector:
                            rarg/node-class: compute
                            rarg/instance-type: m6i.4xlarge

                # default pod type to use -- must be in predefined_pod_types
                job_pod:
                    type: admin 

                # start a dask cluster along with the pod, if enabled
                dask_cluster:
                    enable: false
                    num_workers: 4
                    name: qc-test-cluster
                    threads_per_worker: 4
                    worker_pod:
                        type: thin
                    scheduler_pod:
                        type: admin


    ## some cab-specific backend tweaks
    cabs:
        breizorro:    
            backend:
                kube:
                    job_pod:               # don't need a big pod for breizorro
                        type: thin
                    memory:
                        limit: 3Gi
        wsclean:    
            backend:
                kube:
                    job_pod:               # wsclean could do with a big pod
                        type: fat
                    memory:
                        limit: 64Gi
        quartical:
            backend:
                kube:
                    dask_cluster:           # enable Dask cluster for QuartiCal
                        enable: true



Bat country! Backend settings and substitutions
-----------------------------------------------

Backend settings are amenable to :ref:`substitutions and formula evaluations <subst>`, in a somewhat limited way. Only string-type settings support substitutions and formulas. (Note also that at image build time, only the ``self`` namespace is available.) 

Like everything else in the Stimela config namespace, the global backend settings may be manipulated via :ref:`assign-sections <assign>`. For example::

    my-recipe:
        inputs:
            ncpu: int = 16
        assign:
            config.opts.backend.slurm.srun_opts.cpus-per-task: =recipe.ncpu

We can only recommend this feature to ninja-level users hacking on some kind of development or experimental workflows. Use with great caution, as great confusion may ensue! Also, this hardly promotes reproducable and portable recipes.







        



