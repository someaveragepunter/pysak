def compose(*decs):
    def deco(f):
        for dec in reversed(decs):
            f = dec(f)
        return f
    return deco

classproperty = compose(property, classmethod)