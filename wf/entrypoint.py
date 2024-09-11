import csv
import os
import shutil
import subprocess
import sys
import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Annotated, List, Optional

import requests
import typing_extensions
from flytekit.core.annotation import FlyteAnnotation
from latch.executions import rename_current_execution, report_nextflow_used_storage
from latch.ldata.path import LPath
from latch.resources.tasks import custom_task, nextflow_runtime_task
from latch.resources.workflow import workflow
from latch.types import metadata
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.nextflow.workflow import get_flag
from latch_cli.services.register.utils import import_module_by_path
from latch_cli.utils import urljoins

from wf.enums import DADATaxonomy, Kraken2RefTaxonomy, QiimeRefTaxonomy, SidleRefTaxonomy, SintaxRefTaxonomy

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

sys.stdout.reconfigure(line_buffering=True)


@dataclass(frozen=True)
class SampleSheet:
    sampleid: str
    forwardreads: LatchFile
    reversereads: Optional[LatchFile]
    run: Optional[str]


@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        # "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage-ofs",
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_expiration_hours": 0,
            "version": 2,
        },
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


def custom_samplesheet_constructor(samples: List[SampleSheet], shared_dir: Path) -> Path:
    samplesheet = Path(shared_dir / "samplesheet.csv")
    columns = ["sampleID", "forwardReads", "reverseReads", "run"]

    with open(samplesheet, "w") as f:
        writer = csv.DictWriter(f, columns, delimiter=",")
        writer.writeheader()

        for sample in samples:
            forward_reads_local_path = Path(sample.forwardreads.local_path)
            forward_reads_shared_path = shared_dir / forward_reads_local_path.name
            shutil.move(str(forward_reads_local_path), str(forward_reads_shared_path))

            reverse_reads_shared_path = ""
            if sample.reversereads:
                reverse_reads_local_path = Path(sample.reversereads.local_path)
                reverse_reads_shared_path = shared_dir / reverse_reads_local_path.name
                shutil.move(str(reverse_reads_local_path), str(reverse_reads_shared_path))

            row_data = {
                "sampleID": sample.sampleid,
                "forwardReads": str(forward_reads_shared_path),
                "reverseReads": str(reverse_reads_shared_path) if reverse_reads_shared_path else "",
                "run": sample.run if sample.run else "",
            }
            writer.writerow(row_data)

    return samplesheet


@nextflow_runtime_task(cpu=8, memory=32, storage_gib=100)
def nextflow_runtime(
    input_source: str,
    pvc_name: str,
    run_name: Annotated[
        str,
        FlyteAnnotation(
            {
                "rules": [
                    {
                        "regex": r"^[a-zA-Z0-9_-]+$",
                        "message": "ID name must contain only letters, digits, underscores, and dashes. No spaces are allowed.",
                    }
                ],
            }
        ),
    ],
    input: List[SampleSheet],
    input_fasta: Optional[LatchFile],
    input_folder: Optional[LatchDir],
    FW_primer: Optional[str],
    RV_primer: Optional[str],
    metadata: Optional[LatchFile],
    multiregion: Optional[LatchFile],
    save_intermediates: bool,
    email: Optional[str],
    illumina_novaseq: bool,
    pacbio: bool,
    iontorrent: bool,
    single_end: bool,
    illumina_pe_its: bool,
    multiple_sequencing_runs: bool,
    ignore_empty_input_files: bool,
    retain_untrimmed: bool,
    double_primer: bool,
    ignore_failed_trimming: bool,
    trunclenf: Optional[int],
    trunclenr: Optional[int],
    max_len: Optional[int],
    ignore_failed_filtering: bool,
    concatenate_reads: bool,
    vsearch_cluster: bool,
    filter_ssu: Optional[str],
    min_len_asv: Optional[int],
    max_len_asv: Optional[int],
    filter_codons: bool,
    orf_end: Optional[int],
    dada_ref_tax_custom: Optional[LatchFile],
    dada_ref_tax_custom_sp: Optional[LatchFile],
    dada_assign_taxlevels: Optional[str],
    cut_dada_ref_taxonomy: bool,
    dada_addspecies_allowmultiple: bool,
    dada_taxonomy_rc: bool,
    pplace_tree: Optional[LatchFile],
    pplace_aln: Optional[LatchFile],
    pplace_model: Optional[str],
    pplace_taxonomy: Optional[LatchFile],
    qiime_ref_taxonomy: Optional[QiimeRefTaxonomy],
    qiime_ref_tax_custom: Optional[LatchFile],
    classifier: Optional[LatchFile],
    kraken2_ref_taxonomy: Optional[Kraken2RefTaxonomy],
    kraken2_ref_tax_custom: Optional[LatchFile],
    kraken2_assign_taxlevels: Optional[str],
    sintax_ref_taxonomy: Optional[SintaxRefTaxonomy],
    addsh: bool,
    sidle_ref_taxonomy: Optional[SidleRefTaxonomy],
    sidle_ref_tax_custom: Optional[List[LatchFile]],
    sidle_ref_tree_custom: Optional[LatchFile],
    metadata_category: Optional[str],
    metadata_category_barplot: Optional[str],
    qiime_adonis_formula: Optional[str],
    picrust: bool,
    sbdiexport: bool,
    ancom: bool,
    ancombc: bool,
    ancombc_formula: Optional[str],
    ancombc_formula_reflvl: Optional[str],
    report_abstract: Optional[str],
    skip_fastqc: bool,
    skip_cutadapt: bool,
    skip_dada_quality: bool,
    skip_barrnap: bool,
    skip_qiime: bool,
    skip_qiime_downstream: bool,
    skip_taxonomy: bool,
    skip_dada_taxonomy: bool,
    skip_dada_addspecies: bool,
    skip_barplot: bool,
    skip_abundance_tables: bool,
    skip_alpha_rarefaction: bool,
    skip_diversity_indices: bool,
    skip_multiqc: bool,
    skip_report: bool,
    multiqc_methods_description: Optional[str],
    report_template: Optional[str],
    report_css: Optional[str],
    report_logo: Optional[str],
    extension: Optional[str] = "/*_R{1,2}_001.fastq.gz",
    min_read_counts: Optional[int] = 1,
    cutadapt_min_overlap: Optional[int] = 3,
    cutadapt_max_error_rate: Optional[float] = 0.1,
    trunc_qmin: Optional[int] = 25,
    trunc_rmin: Optional[float] = 0.75,
    max_ee: Optional[int] = 2,
    min_len: Optional[int] = 50,
    sample_inference: Optional[str] = "independent",
    vsearch_cluster_id: Optional[float] = 0.97,
    orf_start: Optional[int] = 1,
    stop_codons: Optional[str] = "TAA,TAG",
    dada_ref_taxonomy: Optional[DADATaxonomy] = DADATaxonomy.silva_138,
    pplace_alnmethod: Optional[str] = "hmmer",
    kraken2_confidence: Optional[float] = 0.0,
    cut_its: Optional[str] = "none",
    its_partial: Optional[int] = 0,
    exclude_taxa: Optional[str] = "mitochondria,chloroplast",
    min_frequency: Optional[int] = 1,
    min_samples: Optional[int] = 1,
    diversity_rarefaction_depth: Optional[int] = 500,
    tax_agglom_min: Optional[int] = 2,
    tax_agglom_max: Optional[int] = 6,
    ancom_sample_min_count: Optional[int] = 1,
    ancombc_effect_size: Optional[float] = 1.0,
    ancombc_significance: Optional[float] = 0.05,
    report_title: Optional[str] = "Summary of analysis results",
    seed: Optional[int] = 100,
    max_cpus: Optional[int] = 16,
    max_memory: Optional[str] = "128.GB",
    max_time: Optional[str] = "240.h",
    outdir: LatchOutputDir = LatchOutputDir("latch:///Ampliseq"),
) -> None:
    shared_dir = Path("/nf-workdir")
    rename_current_execution(str(run_name))

    ignore_list = [
        "latch",
        ".latch",
        ".git",
        "nextflow",
        ".nextflow",
        "work",
        "results",
        "miniconda",
        "anaconda3",
        "mambaforge",
    ]

    shutil.copytree(
        Path("/root"),
        shared_dir,
        ignore=lambda src, names: ignore_list,
        ignore_dangling_symlinks=True,
        dirs_exist_ok=True,
    )

    input_samplesheet = custom_samplesheet_constructor(input, shared_dir)

    cmd = [
        "/root/nextflow",
        "run",
        str(shared_dir / "main.nf"),
        "-work-dir",
        str(shared_dir),
        "-profile",
        "docker",
        "-c",
        "latch.config",
        "-resume",
        *get_flag("input", input_samplesheet),
        *get_flag("input_fasta", input_fasta),
        *get_flag("input_folder", input_folder),
        *get_flag("FW_primer", FW_primer),
        *get_flag("RV_primer", RV_primer),
        *get_flag("metadata", metadata),
        *get_flag("multiregion", multiregion),
        *get_flag("outdir", LatchOutputDir(f"{outdir.remote_path}/{run_name}")),
        *get_flag("save_intermediates", save_intermediates),
        *get_flag("email", email),
        *get_flag("illumina_novaseq", illumina_novaseq),
        *get_flag("pacbio", pacbio),
        *get_flag("iontorrent", iontorrent),
        *get_flag("single_end", single_end),
        *get_flag("illumina_pe_its", illumina_pe_its),
        *get_flag("multiple_sequencing_runs", multiple_sequencing_runs),
        *get_flag("extension", extension),
        *get_flag("min_read_counts", min_read_counts),
        *get_flag("ignore_empty_input_files", ignore_empty_input_files),
        *get_flag("retain_untrimmed", retain_untrimmed),
        *get_flag("cutadapt_min_overlap", cutadapt_min_overlap),
        *get_flag("cutadapt_max_error_rate", cutadapt_max_error_rate),
        *get_flag("double_primer", double_primer),
        *get_flag("ignore_failed_trimming", ignore_failed_trimming),
        *get_flag("trunclenf", trunclenf),
        *get_flag("trunclenr", trunclenr),
        *get_flag("trunc_qmin", trunc_qmin),
        *get_flag("trunc_rmin", trunc_rmin),
        *get_flag("max_ee", max_ee),
        *get_flag("min_len", min_len),
        *get_flag("max_len", max_len),
        *get_flag("ignore_failed_filtering", ignore_failed_filtering),
        *get_flag("sample_inference", sample_inference),
        *get_flag("concatenate_reads", concatenate_reads),
        *get_flag("vsearch_cluster", vsearch_cluster),
        *get_flag("vsearch_cluster_id", vsearch_cluster_id),
        *get_flag("filter_ssu", filter_ssu),
        *get_flag("min_len_asv", min_len_asv),
        *get_flag("max_len_asv", max_len_asv),
        *get_flag("filter_codons", filter_codons),
        *get_flag("orf_start", orf_start),
        *get_flag("orf_end", orf_end),
        *get_flag("stop_codons", stop_codons),
        *get_flag("dada_ref_taxonomy", dada_ref_taxonomy),
        *get_flag("dada_ref_tax_custom", dada_ref_tax_custom),
        *get_flag("dada_ref_tax_custom_sp", dada_ref_tax_custom_sp),
        *get_flag("dada_assign_taxlevels", dada_assign_taxlevels),
        *get_flag("cut_dada_ref_taxonomy", cut_dada_ref_taxonomy),
        *get_flag("dada_addspecies_allowmultiple", dada_addspecies_allowmultiple),
        *get_flag("dada_taxonomy_rc", dada_taxonomy_rc),
        *get_flag("pplace_tree", pplace_tree),
        *get_flag("pplace_aln", pplace_aln),
        *get_flag("pplace_model", pplace_model),
        *get_flag("pplace_alnmethod", pplace_alnmethod),
        *get_flag("pplace_taxonomy", pplace_taxonomy),
        *get_flag("qiime_ref_taxonomy", qiime_ref_taxonomy),
        *get_flag("qiime_ref_tax_custom", qiime_ref_tax_custom),
        *get_flag("classifier", classifier),
        *get_flag("kraken2_ref_taxonomy", kraken2_ref_taxonomy),
        *get_flag("kraken2_ref_tax_custom", kraken2_ref_tax_custom),
        *get_flag("kraken2_assign_taxlevels", kraken2_assign_taxlevels),
        *get_flag("kraken2_confidence", kraken2_confidence),
        *get_flag("sintax_ref_taxonomy", sintax_ref_taxonomy),
        *get_flag("addsh", addsh),
        *get_flag("cut_its", cut_its),
        *get_flag("its_partial", its_partial),
        *get_flag("sidle_ref_taxonomy", sidle_ref_taxonomy),
        *get_flag("sidle_ref_tax_custom", sidle_ref_tax_custom),
        *get_flag("sidle_ref_tree_custom", sidle_ref_tree_custom),
        *get_flag("exclude_taxa", exclude_taxa),
        *get_flag("min_frequency", min_frequency),
        *get_flag("min_samples", min_samples),
        *get_flag("metadata_category", metadata_category),
        *get_flag("metadata_category_barplot", metadata_category_barplot),
        *get_flag("qiime_adonis_formula", qiime_adonis_formula),
        *get_flag("picrust", picrust),
        *get_flag("sbdiexport", sbdiexport),
        *get_flag("diversity_rarefaction_depth", diversity_rarefaction_depth),
        *get_flag("tax_agglom_min", tax_agglom_min),
        *get_flag("tax_agglom_max", tax_agglom_max),
        *get_flag("ancom_sample_min_count", ancom_sample_min_count),
        *get_flag("ancom", ancom),
        *get_flag("ancombc", ancombc),
        *get_flag("ancombc_formula", ancombc_formula),
        *get_flag("ancombc_formula_reflvl", ancombc_formula_reflvl),
        *get_flag("ancombc_effect_size", ancombc_effect_size),
        *get_flag("ancombc_significance", f"{ancombc_significance:.6f}" if ancombc_significance is not None else None),
        *get_flag("report_template", report_template),
        *get_flag("report_css", report_css),
        *get_flag("report_logo", report_logo),
        *get_flag("report_title", report_title),
        *get_flag("report_abstract", report_abstract),
        *get_flag("skip_fastqc", skip_fastqc),
        *get_flag("skip_cutadapt", skip_cutadapt),
        *get_flag("skip_dada_quality", skip_dada_quality),
        *get_flag("skip_barrnap", skip_barrnap),
        *get_flag("skip_qiime", skip_qiime),
        *get_flag("skip_qiime_downstream", skip_qiime_downstream),
        *get_flag("skip_taxonomy", skip_taxonomy),
        *get_flag("skip_dada_taxonomy", skip_dada_taxonomy),
        *get_flag("skip_dada_addspecies", skip_dada_addspecies),
        *get_flag("skip_barplot", skip_barplot),
        *get_flag("skip_abundance_tables", skip_abundance_tables),
        *get_flag("skip_alpha_rarefaction", skip_alpha_rarefaction),
        *get_flag("skip_diversity_indices", skip_diversity_indices),
        *get_flag("skip_multiqc", skip_multiqc),
        *get_flag("skip_report", skip_report),
        *get_flag("seed", seed),
        *get_flag("multiqc_methods_description", multiqc_methods_description),
        *get_flag("max_cpus", max_cpus),
        *get_flag("max_memory", max_memory),
        *get_flag("max_time", max_time),
    ]

    print("Launching Nextflow Runtime")
    print(" ".join(cmd))
    print(flush=True)

    failed = False
    try:
        env = {
            **os.environ,
            "NXF_ANSI_LOG": "false",
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms5g -Xmx30g -XX:ActiveProcessorCount=8",
            "NXF_DISABLE_CHECK_LATEST": "true",
            "NXF_ENABLE_VIRTUAL_THREADS": "false",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    except subprocess.CalledProcessError:
        failed = True
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_ampliseq", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)

        print("Computing size of workdir... ", end="")
        try:
            result = subprocess.run(
                ["du", "-sb", str(shared_dir)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5 * 60,
            )

            size = int(result.stdout.split()[0])
            report_nextflow_used_storage(size)
            print(f"Done. Workdir size: {size / 1024 / 1024 / 1024: .2f} GiB")
        except subprocess.TimeoutExpired:
            print("Failed to compute storage size: Operation timed out after 5 minutes.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to compute storage size: {e.stderr}")
        except Exception as e:
            print(f"Failed to compute storage size: {e}")

    if failed:
        sys.exit(1)
