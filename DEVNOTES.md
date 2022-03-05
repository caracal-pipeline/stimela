


Problem with globs:
-------------------

* prevalidate() calls validation of inputs and outputs. This is the only chance for skipped recipes to get aliases propagated down. I.e. if a sub-recipe has an output that is an alias of a sub-step, and the sub-recipe is skipped, this is the only chance to evaluate the glob (presumably, to existing outputs on disk).

* validate_inputs() called before running a step. Currently this does not evaluate globs.

* validate_outputs() called after running. Here we must re-expand the globs, since running the step may have changed the content.

The current scheme where the glob is expanded and substituted as ``params[name] = [files]`` creates a problem. Expansion needs to happen at prevalidation. Then it needs to happen again at the output stage. So we can't replace the glob with a filelist. Somehow we must retain knowledge that this is a glob, otherwise we won't know to re-evaluate it.

I tried creating a Glob class, but pydantic won't allow that, it expects a list of strings. So we need to retain this information externally (in Cargo, perhaps?)

So: keep a copy of the original params dict, and re-evaluate all globs when asked to.

Consider adding an explicit "glob:" prefix to glob values, so that we know not to re-evaluate explicitly specified files?




Problem with aliases:
---------------------

Let's clean up the logic. First, things to check during finalization:

* an input alias may refer to multiple steps' inputs, but then their schema.dtype must be consistent

* an output alias may refer to only one step's output, aliasing multiple outputs is nonsensical

* an alias's schema is copied from the step, so implicit outputs are implicit for the recipe as well (but mark implicit=True, 
since we don't want to copy the value literally until the step has validated it in its own {}-substitution context)

During prevalidation (when parameter values are available), they must be propagated up or down

* before prevalidating recipe parameters

    * go over all aliases -- if a recipe's parameter is not set:
        
        * check if a substep has a default or implicit -- propagate that value down to the recipe


* before prevalidating steps -- if a recipe has a parameter (or default)

  * propagate that value up to the step aliases, overriding their values

  * implicit output values can't be set, so this will be caught at this point

* after prevalidating the steps -- if the recipe does not have a parameter value

  * check if a substep has a parameter value or default or implicit -- propagate that value down to the recipe

  * if multiple substeps are aliased, check that they're not set to conflicting values, throw an error if so. Otherwise propagate up.

  * make a list of parameters that have been propagated down

After running the step

* outputs may have changed compared to prevalidation. Propagate their value down to the recipe again (using the list compiled in prevalidation)
