import traceback
from typing import Any
from dataclasses import is_dataclass, asdict
from .models import ModelProtocol


def exception_to_dict(ex: Exception) -> dict:
  return {
    "type": ex.__class__.__name__,
    "message": str(ex),
    "traceback": traceback.format_exc(),
  }


def normalize_data(data: Any) -> dict:
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
