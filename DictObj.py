from typing import Any, Dict  # , List, Union


class DictObj:
    """
    A utility class that converts a dictionary into an object with attributes.
    Recursively handles nested dictionaries and lists.
    """

    def __init__(self, in_dict: Dict[str, Any]) -> None:
        if not isinstance(in_dict, dict):
            raise TypeError("Input must be a dictionary")
        for key, val in in_dict.items():
            if isinstance(val, (list, tuple)):
                setattr(self, key, [DictObj(x) if isinstance(x, dict) else x for x in val])
            else:
                setattr(self, key, DictObj(val) if isinstance(val, dict) else val)
