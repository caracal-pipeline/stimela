from pyparsing import *
from pyparsing import common
# Forward, Group, Word, Optional, alphas, alphanums, nums, ZeroOrMore, Literal, sglQuotedString, dblQuotedString
from rich import print

def test_parser():
    from scabha.evaluator import construct_parser

    expr = construct_parser()

    for string in [
            "a.b + b.c - c.d",
            "a.b + b.c * c.d",
            "a.b + -b.c",
            "a.b <= 0",
            "a.b", 
            "IFSET(a.b)",
            "a.b[c.d]",
                ]:
        print(f"\n\n\n=====================\nExpression: {string}\n")
        a = expr.parseString(string, parse_all=True)
        print(f"\n\n\n{a.getName()}")
        print(a.dump())

    # a = expr.parseString("IFSET(a.b)+IFSET(a.b)", parse_all=True)
    # print(a.dump())

    # a = expr.parseString("a.x==b.x", parse_all=True)
    # print(a.dump())


    # a = expr.parseString("(a.x==0)==IF(a.x==0,1,2,3)", parse_all=True)
    # print(a.dump())

    # a = expr.parseString("IFSET(a.b, (a.x==0)==(a.x==0),(a.x!=b.x))", parse_all=True)
    # print(a.dump())

    # print("===")
    # a = expr.parseString("a.b OR a.b", parse_all=True)
    # print(a.dump())

    # a = expr.parse_string("IF((previous.x+1)*previous.x == 2, previous.x == 0, previous.y == 0)", parse_all=True)

    # expr.runTests("""
    #     (a==0)
    #     ((a==0)==(a==0))
    #     IFSET(a.b)
    #     IFSET(a.b, (a==0)==(a==0),(a!=b))
    #     IF((previous.x+1)*previous.x == 2, previous.x is 0, previous.y is not 0)
    #     IF((-previous.x+1)*previous.x == 0, previous.x is 0, previous.y < 0)
    #     a.b
    #     """)

#    a = expr.parse_string("((a.x))")
    



if __name__ == "__main__":
    test_parser()
