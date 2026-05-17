"""
Custom serialization for scientific arrays (NumPy, XArray, Zarr).

This module provides optional support for serializing/deserializing these types
to/from JSON-compatible format using base64 encoding.
"""

import base64
import json
import pickle
from typing import Any

# Optional imports
try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False

try:
    import zarr

    HAS_ZARR = True
except ImportError:
    HAS_ZARR = False


class ScientificArrayEncoder(json.JSONEncoder):
    """JSON encoder that handles NumPy arrays, XArray objects, Zarr groups/arrays."""

    def default(self, obj: Any) -> Any:
        """Encode scientific objects into JSON-serializable structures."""
        if HAS_NUMPY and isinstance(obj, np.ndarray):
            if obj.flags.writeable:
                obj = obj.copy()
            data = base64.b64encode(obj.tobytes()).decode("ascii")
            return {
                "__scientific_array__": True,
                "type": "numpy",
                "dtype": str(obj.dtype),
                "shape": obj.shape,
                "data": data,
            }
        if HAS_XARRAY and isinstance(obj, (xr.Dataset, xr.DataArray)):
            # Serialize XArray to dict representation (no netcdf dependency)
            data_dict = obj.to_dict()
            return {
                "__scientific_array__": True,
                "type": "xarray_dict",
                "data": data_dict,
            }
        if HAS_ZARR and isinstance(obj, (zarr.Array, zarr.Group)):
            # Serialize Zarr objects using pickle as fallback
            data = base64.b64encode(pickle.dumps(obj)).decode("ascii")
            return {
                "__scientific_array__": True,
                "type": "zarr",
                "data": data,
            }
        # Fallback: raise TypeError by delegating to base class
        return super().default(obj)


def scientific_array_hook(dct: Any) -> Any:
    """Object hook for JSON decoding to reconstruct scientific arrays."""
    if not isinstance(dct, dict):
        return dct
    if dct.get("__scientific_array__"):
        arr_type = dct.get("type")
        if arr_type == "numpy" and HAS_NUMPY:
            data = base64.b64decode(dct["data"])
            dtype = np.dtype(dct["dtype"])
            shape = dct["shape"]
            return np.frombuffer(data, dtype=dtype).reshape(shape)
        if arr_type == "xarray_dict" and HAS_XARRAY:
            # Reconstruct XArray from dict representation
            data_dict = dct["data"]
            if "data_vars" in data_dict:
                return xr.Dataset.from_dict(data_dict)
            return xr.DataArray.from_dict(data_dict)
        if arr_type == "xarray_pickle" and HAS_XARRAY:
            # Deserialize XArray from pickle bytes
            data = base64.b64decode(dct["data"])
            return pickle.loads(data)  # noqa: S301
        if arr_type == "zarr" and HAS_ZARR:
            # Deserialize Zarr objects using pickle (fallback)
            data = base64.b64decode(dct["data"])
            return pickle.loads(data)  # noqa: S301
    return dct


def dumps_scientific(obj: Any) -> str:
    """Serialize object to JSON string with scientific array support."""
    return json.dumps(obj, cls=ScientificArrayEncoder)


def loads_scientific(s: str) -> Any:
    """Deserialize JSON string with scientific array support."""
    return json.loads(s, object_hook=scientific_array_hook)
