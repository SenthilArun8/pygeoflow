"""Provenance tracking for reproducible geospatial workflows"""

import datetime
import hashlib
import json
import platform
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)


class ProvenanceRecord:
    """
    Records provenance metadata for a single operation in a pipeline.

    Captures:
    - Operation name and parameters
    - Input/output data signatures
    - Timestamp and execution time
    - System environment details
    """

    def __init__(
        self,
        operation_name: str,
        operation_type: str = "task",
        parameters: Optional[Dict[str, Any]] = None
    ):
        self.operation_name = operation_name
        self.operation_type = operation_type
        self.parameters = parameters or {}
        self.timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.inputs: List[Dict[str, Any]] = []
        self.outputs: List[Dict[str, Any]] = []
        self.execution_time: Optional[float] = None
        self.error: Optional[str] = None

    def add_input(self, name: str, value: Any) -> None:
        """Record an input to this operation"""
        input_record = {
            'name': name,
            'type': type(value).__name__,
            'signature': self._compute_signature(value)
        }

        if isinstance(value, gpd.GeoDataFrame):
            input_record.update({
                'shape': value.shape,
                'crs': str(value.crs) if value.crs else None,
                'bounds': value.total_bounds.tolist() if len(value) > 0 else None,
                'geometry_types': value.geometry.geom_type.unique().tolist()
            })
        elif isinstance(value, (str, Path)):
            input_record['value'] = str(value)
        elif isinstance(value, (int, float, bool)):
            input_record['value'] = value

        self.inputs.append(input_record)

    def add_output(self, name: str, value: Any) -> None:
        """Record an output from this operation"""
        output_record = {
            'name': name,
            'type': type(value).__name__,
            'signature': self._compute_signature(value)
        }

        if isinstance(value, gpd.GeoDataFrame):
            output_record.update({
                'shape': value.shape,
                'crs': str(value.crs) if value.crs else None,
                'bounds': value.total_bounds.tolist() if len(value) > 0 else None,
                'geometry_types': value.geometry.geom_type.unique().tolist()
            })

        self.outputs.append(output_record)

    def _compute_signature(self, value: Any) -> str:
        """Compute a unique signature for the data"""
        if isinstance(value, gpd.GeoDataFrame):
            # Hash based on geometry bounds and row count
            sig_str = f"{value.shape}_{value.total_bounds.tobytes() if len(value) > 0 else b''}"
            return hashlib.md5(sig_str.encode()).hexdigest()[:16]
        elif isinstance(value, (str, Path)):
            return hashlib.md5(str(value).encode()).hexdigest()[:16]
        else:
            return hashlib.md5(str(value).encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'operation_name': self.operation_name,
            'operation_type': self.operation_type,
            'parameters': self.parameters,
            'timestamp': self.timestamp.isoformat(),
            'inputs': self.inputs,
            'outputs': self.outputs,
            'execution_time': self.execution_time,
            'error': self.error
        }


class ProvenanceTracker:
    """
    Tracks provenance for an entire pipeline execution.

    Features:
    - Records all operations and their metadata
    - Captures system environment details
    - Exports provenance to JSON
    - Validates reproducibility
    """

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.records: List[ProvenanceRecord] = []
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.end_time: Optional[datetime.datetime] = None
        self.environment = self._capture_environment()

    # internal method
    def _capture_environment(self) -> Dict[str, Any]:
        """Capture system environment details"""
        import geopandas
        import shapely
        import pyproj

        return {
            'python_version': platform.python_version(),
            'platform': platform.platform(),
            'geopandas_version': geopandas.__version__,
            'shapely_version': shapely.__version__,
            'pyproj_version': pyproj.__version__,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    # public methods
    def start_operation(
        self,
        operation_name: str,
        operation_type: str = "task",
        parameters: Optional[Dict[str, Any]] = None
    ) -> ProvenanceRecord:
        """Start tracking a new operation"""
        record = ProvenanceRecord(operation_name, operation_type, parameters)
        self.records.append(record)
        logger.debug(f"Started tracking operation: {operation_name}")
        return record

    def complete_operation(self, record: ProvenanceRecord, execution_time: float) -> None:
        """Mark operation as complete"""
        record.execution_time = execution_time
        logger.debug(f"Completed operation: {record.operation_name} ({execution_time:.3f}s)")

    def record_error(self, record: ProvenanceRecord, error: Exception) -> None:
        """Record an error during operation"""
        record.error = str(error)
        logger.error(f"Error in operation {record.operation_name}: {error}")

    def finalize(self) -> None:
        """Finalize the provenance tracking"""
        self.end_time = datetime.datetime.now(datetime.timezone.utc)
        total_time = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Pipeline '{self.pipeline_name}' completed in {total_time:.2f}s")

    def to_dict(self) -> Dict[str, Any]:
        """Convert entire provenance to dictionary"""
        total_time = None
        if self.end_time:
            total_time = (self.end_time - self.start_time).total_seconds()

        return {
            'pipeline_name': self.pipeline_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_execution_time': total_time,
            'environment': self.environment,
            'operations': [record.to_dict() for record in self.records]
        }

    def save(self, filepath: Union[str, Path]) -> None:
        """Save provenance to JSON file"""
        filepath = Path(filepath)

        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

        logger.info(f"Saved provenance to {filepath}")

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> 'ProvenanceTracker':
        """Load provenance from JSON file"""
        filepath = Path(filepath)

        with open(filepath, 'r') as f:
            data = json.load(f)

        # Reconstruct tracker (simplified - doesn't restore full state)
        tracker = cls(data['pipeline_name'])
        tracker.start_time = datetime.datetime.fromisoformat(data['start_time'])
        if data['end_time']:
            tracker.end_time = datetime.datetime.fromisoformat(data['end_time'])
        tracker.environment = data['environment']

        logger.info(f"Loaded provenance from {filepath}")
        return tracker

    def get_summary(self) -> Dict[str, Any]:
        """Get a human-readable summary of the provenance"""
        total_ops = len(self.records)
        failed_ops = sum(1 for r in self.records if r.error)
        total_time = None
        if self.end_time:
            total_time = (self.end_time - self.start_time).total_seconds()

        return {
            'pipeline_name': self.pipeline_name,
            'total_operations': total_ops,
            'failed_operations': failed_ops,
            'total_execution_time': total_time,
            'operations': [
                {
                    'name': r.operation_name,
                    'type': r.operation_type,
                    'time': r.execution_time,
                    'status': 'failed' if r.error else 'success'
                }
                for r in self.records
            ]
        }
