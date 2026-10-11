import traceback
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from dataclasses import is_dataclass, asdict
from uuid import UUID
from .models import ModelProtocol


def exception_to_dict(ex: Exception) -> dict:
    """Convert an exception into a dictionary containing type, message, and traceback.

    Args:
        ex (Exception): The exception to convert.

    Returns:
        dict: A dictionary with keys:
            - "type": Exception class name.
            - "message": Exception message string.
            - "traceback": Formatted traceback string.
    """
    return {
        "type": ex.__class__.__name__,
        "message": str(ex),
        "traceback": traceback.format_exc(),
    }


def error_to_dict(error_message: str) -> dict:
    """Wrap an error message string into a dictionary.

    Args:
        error_message (str): The error message to wrap.

    Returns:
        dict: A dictionary with key "message" containing the error message.
    """
    return {"message": error_message}


def normalize_data(data: Any) -> dict:
    """Normalize input data to a dictionary for storage or serialization.

    Supports `dict`, `ModelProtocol` objects, and dataclasses.

    Args:
        data (Any): The input data to normalize.

    Returns:
        dict: A dictionary representation of the data.

    Raises:
        TypeError: If `data` is not a `dict`, `ModelProtocol`, or dataclass.
    """
    # Dict
    if isinstance(data, dict):
        return data

    # ModelProtocol
    if isinstance(data, ModelProtocol):
        return data.as_dict()

    # Dataclass
    if is_dataclass(data):
        return asdict(data)

    raise TypeError(
        f"Unsupported type: {type(data).__name__}. Expected ModelProtocol, dataclass, or dict."
    )


def to_json(data: Any) -> Any:
    """Convert the return value of a task function to data that can be stored as JSON.

    - `None`, `bool`, `int`, `float` and `str` stay as they are.
    - A `dict`, a `list` and a `tuple` are converted value by value. A tuple
      becomes a list.
    - A `ModelProtocol` object and a dataclass become a dictionary.
    - A `datetime`, a `date` and a `time` become text in ISO 8601 format.
    - A `UUID` and a `Decimal` become text.

    Args:
        data (Any): The return value to convert.

    Returns:
        Any: Data that only consists of JSON values.

    Raises:
        TypeError: If `data` is or contains a value of any other type.
    """
    if data is None or isinstance(data, (bool, int, float, str)):
        return data

    if isinstance(data, dict):
        return {key: to_json(value) for key, value in data.items()}

    if isinstance(data, (list, tuple)):
        return [to_json(value) for value in data]

    # ModelProtocol
    if isinstance(data, ModelProtocol):
        return to_json(data.as_dict())

    # Dataclass. The class itself is not a value.
    if is_dataclass(data) and not isinstance(data, type):
        return to_json(asdict(data))

    if isinstance(data, (datetime, date, time)):
        return data.isoformat()

    if isinstance(data, (UUID, Decimal)):
        return str(data)

    raise TypeError(
        f"Unsupported type: {type(data).__name__}. Expected a JSON value, a "
        "ModelProtocol, a dataclass, a datetime, a date, a time, a UUID or a Decimal."
    )
