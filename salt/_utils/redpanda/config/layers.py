"""Merge configuration layers without mutating the inputs."""

import copy


def merge(a, b):
    result = copy.deepcopy(a)
    for key, value in b.items():
        result[key] = (
            merge(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else copy.deepcopy(value)
        )
    return result
