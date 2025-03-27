import pytest
import traceback
from scabha.exceptions import SubstitutionError
from scabha.substitutions import *
from omegaconf import OmegaConf 
from scabha.evaluator import UNSET, Evaluator
from scabha.validate import Unresolved

def test_subst():

    x = OmegaConf.create()
    x.a = 1
    x.b = "{foo.a} not meant to be substituted here since x marked as not mutable"
    x.c = 3

    ns = SubstitutionNS(foo={})

    bar = SubstitutionNS()
    ns._add_("x", x, nosubst=True)
    ns._add_("bar", bar)
    
    ns.foo.zero = 0

    ns.foo.a = "{x.a}-{x.c}"
    ns.foo.b = "{foo.a}{{}}"
    ns.foo.c = "{bar.a}-{bar.x}-{bar.b}"
#    ns.foo['d/e'] = "x"

    ns.bar.a = 1
    ns.bar.b = "{foo.b}"
    ns.bar.c = "{foo.x} deliberately unresolved"
    ns.bar.c1 = "{foo.x.y.z} deliberately unresolved"
    ns.bar.b1 = "{bar.b}"

    # some deliberate cyclics
    ns.bar.d = "{bar.d}"
    ns.bar.e = "{bar.f}"
    ns.bar.f = "{bar.e}"


    with substitutions_from(ns, raise_errors=True) as context:
        assert context.evaluate("{bar.a}") == "1"
        assert context.evaluate("{bar.b}") == "1-3{}"
        assert context.evaluate("{bar.b1}") == "1-3{}"
        assert context.evaluate(["{x.a}-{x.c}", "{foo.a}{{}}"]) == ["1-3", "1-3{}"]
        assert context.evaluate(["{x.a}-{x.c}", {'y': "{foo.a}{{}}"}]) == ["1-3", {'y': "1-3{}"}]\
        
#        print(context.evaluate("{foo.d/e}"))

    with substitutions_from(ns, raise_errors=False) as context:
        val = context.evaluate("{bar.c}")
        # expect 1 error
        assert val == ''
        assert len(context.errors) == 1
        print(f"bar.c evaluates to type{type(val)}: '{val}'")
        print(f"error is (expected): {context.errors[0]}")

    with forgiving_substitutions_from(ns) as context:
        val = context.evaluate("{nothing}")
        assert val == ''   # '{nothing}' evaluates to '' in forgiving mode

    with forgiving_substitutions_from(ns, True) as context:
        val1 = context.evaluate("{nothing}")
        val2 = context.evaluate("{nothing.more}")
        print(f"errors (none expected): {context.errors}")
        assert not context.errors
        assert val1 == "(KeyError: 'nothing')" # '{nothing}' evaluates to '(error message)' in forgiving=True mode
        assert val2 == "(KeyError: 'nothing')" # '{nothing}' evaluates to '(error message)' in forgiving=True mode

    with forgiving_substitutions_from(ns, "XX") as context: # unknown substitutions evaluate to "XX"
        val = context.evaluate("{nothing}")
        assert val == 'XX'                            
        val = context.evaluate("{bar.c}")
        assert val == 'XX deliberately unresolved'    
        val = context.evaluate("{bar.c1}")
        assert val == 'XX deliberately unresolved'    
        val = context.evaluate("{bug.x} {bug.y}")
        assert val == 'XX XX'    

    with substitutions_from(ns) as context:
        val = context.evaluate("{bar.d}")
        val = context.evaluate("{bar.e}")
        val = context.evaluate("{foo.a:02d}")
        # expect 1 error
        assert len(context.errors) == 3
        for err in context.errors:
            print(f"expected error: {err}")

    with substitutions_from(ns, raise_errors=True) as context:
        try:
            val = context.evaluate("{bar.d}")
            assert val == "not allowed"
            print("{bar.d} is ", val)
        except CyclicSubstitutionError as exc:
            traceback.print_exc()

    try:
        context.evaluate("xxx")
        raise RuntimeError("exception should have been raised due to invalid substitution")
    except SubstitutionError as exc:
        print(f"Error as expected ({exc})")


            
        
def test_formulas():

    ns = SubstitutionNS(previous={}, previous2={})

    ns.previous.x = 1
    ns.previous.x0 = 0
    ns.previous.y = 'y'
    ns.previous.z = 'z'
    ns.previous2.z = 'zz'

    current = OrderedDict(
        a = 'a{previous.x}',
        b = '==escaped',
        c = '=previous.x',
        d = '=IFSET(previous.x)',
        e = '=IFSET(previous.x,"z",2)',
        e1 = '=IFSET(previous.x,SELF,2)',
        f = '=IFSET(previous.xx)',
        g = '=IFSET(previous.xx,SELF,2)',
        h = "=IF(previous.x, True, 'False')", 
        i = "=IF(previous.x0, True, 'False')",
        j = "=IF(previous.xx, True, 'False', UNSET)",
        k = "=current.j",
        l = "=IFSET(current.f)",
        m = "=IF((previous.x+1)*previous.x == 2, previous.x == 1, previous.y == 0)",
        n = "=IF((-previous.x+1)*(previous.x) == 0, previous.x == 1, previous.y < 0)",
        o = '=previous.z',
        p = 'x{previous.zz}',
        q = '=LIST(current.a, current.b, current.c + 1, 0)',
        r = '=not IFSET(current.a)',
        s = '=current.c + current.c + current.c',
        t = '=previous*.z',
        u = [3, 1, 2],
        u1 = '=SORT(current.u)',
        u2 = '=RSORT(current.u)',
        u3 = '=GETITEM(current.u, 1)',
        u4 = '=current.u[previous.x]',
        v1 = '=CASES(previous.x == 0, deliberately.unset.thats.ok, previous.x == 1, 1)',
        v2 = '=CASES(previous.x == 1, 2, previous.x == 2, 2)',
        v3 = '=CASES(previous.x == 0, 0, previous.x == 2, 2, 3)',
        v4 = '=CASES(previous.x == 0, 0, previous.x == 2, 2)',
        v5 = '=ERROR(boom!)',
        v6 = '=ERROR(boom!)',
    )
    ns._add_("current", current)
    
    with substitutions_from(ns, raise_errors=True) as context:
        evaltor = Evaluator(ns, context, location=['top'])

        results = evaltor.evaluate_dict(current, corresponding_ns=ns.current, raise_substitution_errors=False, verbose=True)

        r = results
        print(r.keys())

        assert r['a'] == "a1"
        assert r['b'] == "=escaped"
        assert r['c'] == 1
        assert r['d'] == 1 
        assert r['e'] == "z"
        assert r['e1'] == 1
        assert 'f' not in r 
        assert r['g'] == 2
        assert r['h'] == True
        assert r['i'] == 'False'
        assert 'j' not in r
        assert type(r['k']) is UNSET
        assert 'l' not in r
        assert r['m'] == True
        assert r['n'] == True
        assert r['o'] == 'z'
        assert type(r['p']) is Unresolved
        assert r['q'] == ["a1", "=escaped", 2, 0]
        assert r['r'] == False
        assert r['t'] == 'zz'
        assert r['v1'] == 1
        assert r['v2'] == 2
        assert r['v3'] == 3
        assert type(r['v4']) is UNSET

if __name__ == "__main__":
    test_subst()
    test_formulas()
