from pathlib import Path

from latch.types.directory import LatchDir
from latch.types.metadata import (
    LatchAuthor,
    NextflowMetadata,
    NextflowRuntimeResources,
)

from .parameters import flow, generated_parameters

NextflowMetadata(
    display_name="nf-core/ampliseq",
    author=LatchAuthor(
        name="nf-core",
    ),
    parameters=generated_parameters,
    runtime_resources=NextflowRuntimeResources(
        cpus=4,
        memory=8,
        storage_gib=100,
    ),
    flow=flow,
    about_page_path=Path("docs/latch.md"),
    log_dir=LatchDir("latch:///nfcore_ampliseq_logs"),
)
