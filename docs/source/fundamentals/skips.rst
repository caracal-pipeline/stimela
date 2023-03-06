.. highlight: yml
.. _skips:

Cherry-picking steps, skips and tags
####################################


Cherry-picking steps from the command line
------------------------------------

The ``-s/--step`` option to ``stimela run`` allows one to selectively run part of the recipe. This option expects one or more step labels (comma-separated, or alternatively multiple ``--step`` options can be given), or a step *range* specified as ``start:end`` (or ``:end``, or ``start:``. Note that ``end`` is inclusive.) Only the specified step(s) are then run. 

The skip attribute
------------------

Recipe steps can have an optional ``skip`` attribute. This can be set to ``true`` (a.k.a. a hard-skip), or can employ :ref:`subst` for a *conditional skip* that is evaluated at runtime. Skipped steps are, for lack of a better word, skipped.

Hard-skips are mainly useful for steps that are intended to be invoked manually (think of dev-workflows with expensive or experimental one-off steps). A hard-skip step can only ever be invoked via the ``-s/--step`` option.

The ``skip_if_outputs`` attribute provides a way to skip steps based on the state of their (file-type) outputs. Setting ``skip_if_outputs: exist`` will cause a step to be skipped if all its file-type outputs exist. Setting ``skip_if_outputs: fresh`` will cause a step to be skipped if all its file-type outputs exist, and are not older than its file-type inputs (think old-school Makefiles). 


Tags
-----

Tags provide a way of grouping related steps together, and running or skipping them as a group. Tags are inspired by Ansible (https://docs.ansible.com/ansible/latest/playbook_guide/playbooks_tags.html). 

A step can have an optional ``tags`` attribute, giving a list of one or more arbitrary text labels. Two tags with special meaning are  ``never`` and ``always``. The following rules then apply:

* Invoking the recipe as ``stimela run --tags foo`` tells stimela to run only the steps that have a ``foo`` or ``always`` tag.   
* Conversely, ``--skip-tags foo`` tells stimela to run all steps, except those with a ``foo`` or ``never`` tag. 
* If no ``--tags`` or ``--skip-tags`` option is given, steps tagged with ``never`` are skipped.

Note that if a **single** step (as opposed to a step range) is given to the ``-s/--step`` option, that step is explicitly enabled, regardless of its tags or skip attributes.







