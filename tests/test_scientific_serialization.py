"""Tests for scientific array serialization support."""

import pytest

from taskiq_flow.serialization import dumps_scientific, loads_scientific

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


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
class TestNumPySerialization:
    """Tests for NumPy array serialization."""

    def test_simple_array_roundtrip(self) -> None:
        """Test that a simple 1D array survives serialization."""
        arr = np.array([1, 2, 3, 4, 5])
        serialized = dumps_scientific(arr)
        deserialized = loads_scientific(serialized)
        assert np.array_equal(arr, deserialized)

    def test_2d_array_roundtrip(self) -> None:
        """Test that a 2D array survives serialization."""
        arr = np.array([[1, 2], [3, 4]])
        serialized = dumps_scientific(arr)
        deserialized = loads_scientific(serialized)
        assert np.array_equal(arr, deserialized)

    def test_array_with_dtype(self) -> None:
        """Test that dtype is preserved during serialization."""
        arr = np.array([1.1, 2.2, 3.3], dtype=np.float64)
        serialized = dumps_scientific(arr)
        deserialized = loads_scientific(serialized)
        assert arr.dtype == deserialized.dtype
        assert np.array_equal(arr, deserialized)


@pytest.mark.skipif(not HAS_XARRAY, reason="xarray not installed")
class TestXArraySerialization:
    """Tests for XArray serialization."""

    def test_dataset_roundtrip(self) -> None:
        """Test that a Dataset survives serialization."""
        ds = xr.Dataset({"var": (["x", "y"], [[1, 2], [3, 4]])})
        serialized = dumps_scientific(ds)
        deserialized = loads_scientific(serialized)
        # Compare variables
        assert "var" in deserialized
        assert np.array_equal(ds["var"].values, deserialized["var"].values)

    def test_dataarray_roundtrip(self) -> None:
        """Test that a DataArray survives serialization."""
        da = xr.DataArray([1, 2, 3], dims=["x"])
        serialized = dumps_scientific(da)
        deserialized = loads_scientific(serialized)
        assert np.array_equal(da.values, deserialized.values)


@pytest.mark.skipif(not HAS_ZARR, reason="zarr not installed")
class TestZarrSerialization:
    """Tests for Zarr storage serialization."""

    def test_array_roundtrip(self) -> None:
        """Test that a Zarr array survives serialization."""
        z = zarr.zeros((10,), chunks=(5,))
        z[:5] = [1, 2, 3, 4, 5]
        serialized = dumps_scientific(z)
        deserialized = loads_scientific(serialized)
        # Zarr arrays may not have a simple equality; check stored data
        assert np.array_equal(z[:], deserialized[:])
