.. highlight: yml
.. _assign:

Recipe variable assignments
###########################

In addition to inputs and outputs (IOs), recipes can define internal *variables* (a close analogy would be the local variables of a Python function, as opposed to its parameters). 

Variables are defined by providing an optional ``assign`` section within the recipe::

    my-recipe:
        assign:
            foo: x
            bar: 
                baz: '5'
                qux: 5
                quux: =recipe.bar.qux * 2
            bar.corge: y

Things to note from the above:

* Variables do not have a fixed schema. Their types are set dynamically at time of assignment, following normal YaML syntax (in the case of constants). In the example above, ``foo``, ``bar.baz`` and ``bar.corge`` would be strings, while ``bar.qux`` is an integer. 
* Variables can be nested into subsections, such as ``bar`` above. Using dot-syntax (``bar.corge``) is equivalent to putting a variable inside a subsection.
* Variables are subject to :ref:`subst`, and can be used inside formulas and substitutions themselves, via the ``recipe`` namespace (this is, in fact, the primary *raison d'etre* of variables.)

Step assignments
================

Assign-based-on
===============






