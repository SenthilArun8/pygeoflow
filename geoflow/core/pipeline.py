"""
Pipeline orchestration with @geo_pipeline decorator
"""

import functools
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union
import logging

from geoflow.core.provenance import ProvenanceTracker

logger = logging.getLogger(__name__)


class PipelineResult:
    """
    Produces a pipeline execution with provenance metadata.

    Contract:
        result: The actual return value from the pipeline
        provenance: ProvenanceTracker with execution details
        pipeline_name: Name of the pipeline
    """

    def __init__(self, result: Any, provenance: ProvenanceTracker, pipeline_name: str):
        self.result = result
        self.provenance = provenance
        self.pipeline_name = pipeline_name

    def save_provenance(self, filepath: Union[str, Path]) -> None:
        '''
        Save provenance metadata to JSON file containing complete operation history.

        save_provenance: filepath: Union[str, Path] -> None

        Examples:
            result.save_provenance("analysis_provenance.json") -> Saves provenance to JSON file
        '''
        self.provenance.save(filepath)
        logger.info(f"Saved provenance for '{self.pipeline_name}' to {filepath}")

    def get_summary(self) -> Dict[str, Any]:
        '''
        Get human-readable execution summary with operation counts and timing.

        get_summary: -> Dict[str, Any]

        Examples:
            result.get_summary() -> {'pipeline_name': 'buffer_analysis', 'total_operations': 5, 'total_execution_time': 1.23}
        '''
        return self.provenance.get_summary()

    def __repr__(self) -> str:
        summary = self.provenance.get_summary()
        return (
            f"PipelineResult(pipeline='{self.pipeline_name}', "
            f"operations={summary['total_operations']}, "
            f"time={summary['total_execution_time']:.2f}s)"
        )


class GeoPipeline:
    """
    Wrapper for a pipeline function with provenance tracking.

    Created by @geo_pipeline decorator. Provides:
    - Automatic provenance tracking
    - Execution metadata capture
    - Error handling and logging
    """

    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        description: Optional[str] = None,
        track_provenance: bool = True,
        auto_save_provenance: bool = False,
        provenance_dir: Optional[Union[str, Path]] = None
    ):
        self.func = func
        self.name = name or func.__name__
        self.description = description or func.__doc__
        self.track_provenance = track_provenance
        self.auto_save_provenance = auto_save_provenance
        if provenance_dir:
            self.provenance_dir = Path(provenance_dir)
        else:
            self.provenance_dir = Path("provenance")
        # Once initialized, the pipeline is ready to be executed
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs) -> Any:
        """
        Execute the pipeline WITHOUT provenance tracking.

        Use .run() method for provenance tracking.
        """
        logger.info(f"Executing pipeline '{self.name}' (no provenance tracking)")
        return self.func(*args, **kwargs)

    def run(self, *args, **kwargs) -> PipelineResult:
        """
        Execute the pipeline WITH provenance tracking.

        Returns:
            PipelineResult containing both the result and provenance metadata
        """
        logger.info(f"Starting pipeline '{self.name}' with provenance tracking")

        # Initialize provenance tracker
        tracker = ProvenanceTracker(self.name)

        # Create operation record for the entire pipeline
        pipeline_record = tracker.start_operation(
            self.name,
            operation_type="pipeline",
            parameters={
                'args': [str(arg) for arg in args],
                'kwargs': {k: str(v) for k, v in kwargs.items()}
            }
        )

        # Record inputs
        for i, arg in enumerate(args):
            pipeline_record.add_input(f"arg_{i}", arg)
        for key, value in kwargs.items():
            pipeline_record.add_input(key, value)

        # Execute pipeline
        start_time = time.time()
        try:
            result = self.func(*args, **kwargs)
            execution_time = time.time() - start_time

            # Record output
            pipeline_record.add_output("result", result)

            # Complete tracking
            tracker.complete_operation(pipeline_record, execution_time)
            tracker.finalize()

            logger.info(f"Pipeline '{self.name}' completed successfully in {execution_time:.2f}s")

        except Exception as e:
            execution_time = time.time() - start_time
            tracker.record_error(pipeline_record, e)
            tracker.complete_operation(pipeline_record, execution_time)
            tracker.finalize()

            logger.error(f"Pipeline '{self.name}' failed after {execution_time:.2f}s: {e}")
            raise

        # Auto-save provenance if enabled
        if self.auto_save_provenance:
            self.provenance_dir.mkdir(parents=True, exist_ok=True)
            timestamp = tracker.start_time.strftime("%Y%m%d_%H%M%S")
            provenance_file = self.provenance_dir / f"{self.name}_{timestamp}_provenance.json"
            tracker.save(provenance_file)

        return PipelineResult(result, tracker, self.name)


def geo_pipeline(
    name: Optional[str] = None,
    description: Optional[str] = None,
    track_provenance: bool = True,
    auto_save_provenance: bool = False,
    provenance_dir: Optional[Union[str, Path]] = None
):
    '''
    Decorator that transforms ordinary functions into reproducible geospatial pipelines
    with automatic provenance tracking. Captures all operations, parameters, inputs, outputs,
    and execution metadata for full reproducibility. Can auto-save provenance to JSON files.
    Use .run() method for provenance tracking, or call directly for fast mode without tracking.

    geo_pipeline: name: Optional[str] = None, description: Optional[str] = None,
                  track_provenance: bool = True, auto_save_provenance: bool = False,
                  provenance_dir: Optional[Union[str, Path]] = None -> Callable[[Callable], GeoPipeline]

    Examples:
        @geo_pipeline(name="buffer_analysis")
        def analyze_buffers(roads_path):
            return buffer(load(roads_path).to_crs('EPSG:32610'), distance=100)
        result = analyze_buffers.run("roads.gpkg") -> PipelineResult with provenance metadata
        result.save_provenance("analysis.json") -> Saves provenance to JSON file
    '''

    def decorator(func: Callable) -> GeoPipeline:
        return GeoPipeline(
            func,
            name=name,
            description=description,
            track_provenance=track_provenance,
            auto_save_provenance=auto_save_provenance,
            provenance_dir=provenance_dir
        )

    return decorator
