"""
Custom serialization for scientific arrays (NumPy, XArray, Zarr).

This module provides optional support for serializing/deserializing these types
to/from JSON-compatible format using base64 encoding.
"""

import base64
import json
import pickle
from io import BytesIO
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
            buffer = BytesIO()
            obj.to_netcdf(buffer)  # type: ignore[call-overload]
            data = base64.b64encode(buffer.getvalue()).decode("ascii")
            return {
                "__scientific_array__": True,
                "type": "xarray",
                "data": data,
            }
        if HAS_ZARR and isinstance(obj, (zarr.Array, zarr.Group)):
            store = zarr.MemoryStore()  # type: ignore[attr-defined]
            zarr.store(store, obj, path="zarr_data")  # type: ignore[attr-defined]
            if hasattr(obj, "to_bytes"):
                data = base64.b64encode(obj.to_bytes()).decode("ascii")
            else:
                # Fallback: pickle (not ideal but works)

                data = base64.b64encode(pickle.dumps(obj)).decode("ascii")
            return {
                "__scientific_array__": True,
                "type": "zarr",
                "data": data,
            }
        # Fallback: return string representation for any other object
        return str(obj)


def scientific_array_hook(dct: Any) -> Any:
    """Object hook for JSON decoding to reconstruct scientific arrays."""
    if not isinstance(dct, dict):
        return dct
    if dct.get("__scientific_array__"):
        arr_type = dct.get("type")
        data = base64.b64decode(dct["data"])
        if arr_type == "numpy" and HAS_NUMPY:
            dtype = np.dtype(dct["dtype"])
            shape = dct["shape"]
            return np.frombuffer(data, dtype=dtype).reshape(shape)
        if arr_type == "xarray" and HAS_XARRAY:
            return xr.open_dataset(BytesIO(data), engine="netcdf4")
        if arr_type == "zarr" and HAS_ZARR:
            try:
                if hasattr(zarr, "from_bytes"):
                    return zarr.from_bytes(data)
                # pickle is safe here for trusted data only; may be unsafe
                return pickle.loads(data)  # noqa: S301
            except Exception:
                # Last resort: return raw data
                return data
    return dct


def dumps_scientific(obj: Any) -> str:
    """Serialize object to JSON string with scientific array support."""
    return json.dumps(obj, cls=ScientificArrayEncoder)


def loads_scientific(s: str) -> Any:
    """Deserialize JSON string with scientific array support."""
    return json.loads(s, object_hook=scientific_array_hook)
