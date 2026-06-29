def callable_function(a: int, b: str):
    print(f"callable_function({a},'{b}')")
    return a * 2


def callable_function_dict(a: int, b: str):
    print(f"callable_function_dict({a},'{b}')")
    return dict(x=a * 2, y=b + b)
