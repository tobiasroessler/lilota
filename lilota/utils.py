import traceback
from typing import Any
from dataclasses import is_dataclass, asdict
from pydantic import BaseModel


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
  
  # Pydantic v2
  if isinstance(data, BaseModel):
    return data.model_dump()

  # Dataclass
  if is_dataclass(data):
    return asdict(data)

  raise TypeError(
    f"Unsupported type: {type(data).__name__}. Expected BaseModel, dataclass, or dict."
  )
