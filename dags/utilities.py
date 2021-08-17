def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
        except TypeError:
            return None
    return dct


def safeget_no_case(dct, *keys):
    dct = {k.lower(): v for k, v in dct.items()}
    for key in keys:
        try:
            dct = dct[key.lower()]
            if type(dct) is dict:
                dct = {k.lower(): v for k, v in dct.items()}
        except KeyError:
            return None
        except TypeError:
            return None
    return dct
