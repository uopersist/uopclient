
foo = None
class Foo:
    def __init__(self):
        self._foo = 333

def set_foo(f):
    global foo
    foo = f

