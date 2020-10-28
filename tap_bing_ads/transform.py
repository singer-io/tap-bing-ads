from datetime import datetime

import arrow
from suds.sudsobject import asdict

def sobject_to_dict(obj):
    if not hasattr(obj, '__keylist__'):
        return obj

    out = {}
    for key, value in asdict(obj).items():
        if hasattr(value, '__keylist__'):
            out[key] = sobject_to_dict(value)
        elif isinstance(value, list):
            out[key] = []
            for item in value:
                out[key].append(sobject_to_dict(item))
        elif isinstance(value, datetime):
            out[key] = arrow.get(value).isoformat()
        else:
            out[key] = value
    return out
