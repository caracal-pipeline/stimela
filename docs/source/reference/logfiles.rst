.. highlight: yml
.. _logfiles:


Log files and logging
#####################

Stimela has an extensive logging mechanism. All console output from the steps is intercepted and logged, and written to log file(s) (unless directed otherwise). The log files can be separated by recipe, step, etc.

Logging settings can be changed via the global ``opts.log`` setting::

    opts:
        log:
            dir: logs/log-{config.run.datetime}
            name: log-{info.fqname}
            nest: 2
            symlink: log

The meaning of the log options above is :ref:`discussed here <log_options_example>`. This is generally a useful default setup for long workflows, as it logs each Stimela run in a separate directory with a timestamp in the name, and also keeps the logs from each step separate. 

Here is a complete list of available log settings::

    opts:
        log:
            enable: true                          
            name: log-{info.fqname}
            ext: .txt
            dir: . 
            symlink: none
            nest: 999                             
            level: INFO

Logfiles may be completely disabled by setting ``enable: false``. The ``name`` field determines the naming of logfiles, and may include :ref:`substitutions <subst>`. In this case, the name will include the fully-qualified name of the step (i.e. "recipe.step_label"). A useful alternative would be ``log-{info.taskname}``, since the task name includes a loop counter, in the cae of for-loops. The ``ext`` field is just the extension given to log files. 

The ``dir`` field determines which directory log files go into. Since this can also include substitutions, the pattern above (``dir: logs/log-{config.run.datetime}``) is useful for neatly separating logs into separate subdirectories (``run.datetime`` giving a date/time stamp for the start of the current Stimela session). The ``symlink`` field, if given, then tells Stimela to create a symlink (named according to the content of the field) pointing to the latest log directory. The ``nest`` field determines the granularity at which logfiles are split up by recipe and step (values above 2 are only useful for splitting up logs from deeply nested subrecipes). Finally, ``level`` determines the level at which log messages are issued.

Tweaking log settings
---------------------

Log settings may be tweaked in the :ref:`assign section <assign>` of a recipe or step. This can be useful if you want to redirect the logs of a recipe (or step) to a non-standard location. For example, you can name your logfiles based on some input of the recipe, as given by the :ref:`example here <log_options_dirout_example>`::

    my-recipe:
        inputs:
            output_directory: Directory = "output"
        assign:
            log.dir: "{recipe.output_directory}/logs"

Note that this is subtly different from::

        assign:
            config.opts.log.dir: "{recipe.output_directory}/logs"

The latter form would tweak the global log settings -- this is not advisable, as such an assignment would persist after a recipe has finished running (which makes a difference if it is invoked as a sub-recipe). Assignments to ``log.*`` only persist for the duration of the recipe (or step).






