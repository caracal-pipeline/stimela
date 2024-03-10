.. highlight: yml
.. _for_loops:

Recipe for-loops
################

A for-loop is a mechanism for repeating a workflow over a list of parameter values. For example, you may want to run the same sequence of operations on a list of images, or a list of measurement sets. 

This can be specified in one of two ways::

    my-recipe:
        for_loop:
            var: foo
            over: [a, b, c]
        steps:
            step1:
                cab: my-cab
                params:
                    bar: =recipe.foo

This tells Stimela to run the recipe three times, setting the variable ``foo`` to "a", "b" and "c" respectively. :ref:`subst`can then be employed inside the step definitions to alter the workflow based on the value of ``foo``.

In the second form, the list of values to be iterated over is an input to the recipe::

    my-recipe:
        inputs:
            image-list: List[File]
        for_loop:
            var: image
            over: image-list
        steps:
            step1:
                cab: my-cab
                params:
                    img: =recipe.image

Looping sub-recipes
-------------------

Note that loops are all-or-nothing, i.e. the entire recipe is treated as a loop. If you only need to loop a particular series of steps, you can do this by defining a sub-recipe. Here's a notional example::

    loop-recipe:
        info: "this runs an operation over a list of images"
        inputs:
            image-list: List[File]
        for_loop:
            var: image
            over: image-list
        steps:
            step1:
                cab: my-cab
                params:
                    img: =recipe.image

    main-recipe:
        info: "this is the top-level recipe"
        steps:
            a:
                info:
                    this step gets a list of images from somewhere.
                    The get-image-list cab has an output called "image_list"
                cab: get-image-list
            b:
                info:
                    this step runs "loop-recipe" over the list of images 
                    returned by the previous step
                recipe: loop-recipe
                params:
                    image-list: =steps.a.image_list            

Scattering and concurrency
--------------------------

You can also tell Stimela to scatter a loop over a number of worker processes, and execute them in parallel::

    my-recipe:
        for_loop:
            var: image
            over: image-list
            scatter: 16

This will run up to 16 iterations of the loop concurrently. Use ``scatter: -1`` to run all iterations concurrently. Single-node users beware, this is an easy way to overload a node! However, with the Kubernetes backend or the Slurm backend wrapper, this can very effectively leverage a cluster.


