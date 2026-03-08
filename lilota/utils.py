import traceback
from typing import Any
from dataclasses import is_dataclass, asdict
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
  return {
    "message": error_message
  }


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
