"""
Place some common APIs.
"""

import logging
import subprocess
import re
from decimal import Decimal
from decimal import getcontext
import six


def normalize_data_size(value_str, order_magnitude="M", factor=1024):
    """
    Normalize a data size in one order of magnitude to another.

    :param value_str: a string include the data default unit is 'B'
    :param order_magnitude: the magnitude order of result
    :param factor: int, the factor between two relative order of magnitude.
                   Normally could be 1024 or 1000
    :return normalized data size string
    """
    def _get_unit_index(m):
        try:
            return ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'].index(
                m.upper())
        except ValueError:
            pass
        return 0

    def _trim_tailling_zeros(num_str):
        # remove tailing zeros, convert float number str to int str
        if '.' in num_str:
            num_str = num_str.rstrip('0').rstrip('.')
        return num_str

    regex = r"(\d+\.?\d*)\s*(\w?)"
    match = re.search(regex, value_str)
    try:
        value = match.group(1)
        unit = match.group(2)
        if not unit:
            unit = 'B'
    except TypeError as e:
        raise ValueError(f"Invalid data size format 'value_str={value_str}'") from e

    getcontext().prec = 20
    from_index = _get_unit_index(unit)
    to_index = _get_unit_index(order_magnitude)
    if from_index - to_index >= 0:
        data = Decimal(value) * Decimal(factor ** (from_index - to_index))
    else:
        data = Decimal(value) / Decimal(factor ** (to_index - from_index))
    return _trim_tailling_zeros(f"{data:f}")


def format_result(result, base="12", fbase="2"):
    """
    Format the result to a fixed length string.

    :param result: result need to convert
    :param base: the length of converted string
    :param fbase: the decimal digit for float
    """

    if isinstance(result, six.string_types):
        value = "{0:>" + base + "s}"
    elif isinstance(result, int):
        value = "{0:>" + base + "d}"
    elif isinstance(result, float):
        value = "{0:>" + base + "." + fbase + "f}"
    return value.format(result)


def drop_cache():
    """
    Drop system cache
    """
    try:
        subprocess.run("sync && echo 3 > /proc/sys/vm/drop_caches",
                        shell=True, check=True)
    except Exception as e:
        logging.debug("Failed to drop cache %s", str(e))
