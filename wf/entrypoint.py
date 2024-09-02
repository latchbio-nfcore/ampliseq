import os
import shutil
import subprocess
import sys
import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

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

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata


@dataclass(frozen=True)
class SampleSheet:
    sampleid: str
    forwardreads: LatchFile
    reversereads: typing.Optional[LatchFile]
    run: typing.Optional[str]


@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_expiration_hours": 0,
        },
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(
    input_source: str,
    pvc_name: str,
    run_name: str,
    input: typing.List[SampleSheet],
    input_fasta: typing.Optional[LatchFile],
    input_folder: typing.Optional[LatchDir],
    FW_primer: typing.Optional[str],
    RV_primer: typing.Optional[str],
    metadata: typing.Optional[LatchFile],
    multiregion: typing.Optional[LatchFile],
    outdir: LatchOutputDir,
    save_intermediates: bool,
    email: typing.Optional[str],
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
    trunclenf: typing.Optional[int],
    trunclenr: typing.Optional[int],
    max_len: typing.Optional[int],
    ignore_failed_filtering: bool,
    concatenate_reads: bool,
    vsearch_cluster: bool,
    filter_ssu: typing.Optional[str],
    min_len_asv: typing.Optional[int],
    max_len_asv: typing.Optional[int],
    filter_codons: bool,
    orf_end: typing.Optional[int],
    dada_ref_tax_custom: typing.Optional[str],
    dada_ref_tax_custom_sp: typing.Optional[str],
    dada_assign_taxlevels: typing.Optional[str],
    cut_dada_ref_taxonomy: bool,
    dada_addspecies_allowmultiple: bool,
    dada_taxonomy_rc: bool,
    pplace_tree: typing.Optional[str],
    pplace_aln: typing.Optional[str],
    pplace_model: typing.Optional[str],
    pplace_taxonomy: typing.Optional[str],
    qiime_ref_taxonomy: typing.Optional[str],
    qiime_ref_tax_custom: typing.Optional[str],
    classifier: typing.Optional[str],
    kraken2_ref_taxonomy: typing.Optional[str],
    kraken2_ref_tax_custom: typing.Optional[str],
    kraken2_assign_taxlevels: typing.Optional[str],
    sintax_ref_taxonomy: typing.Optional[str],
    addsh: bool,
    sidle_ref_taxonomy: typing.Optional[str],
    sidle_ref_tax_custom: typing.Optional[str],
    sidle_ref_tree_custom: typing.Optional[str],
    metadata_category: typing.Optional[str],
    metadata_category_barplot: typing.Optional[str],
    qiime_adonis_formula: typing.Optional[str],
    picrust: bool,
    sbdiexport: bool,
    ancom: bool,
    ancombc: bool,
    ancombc_formula: typing.Optional[str],
    ancombc_formula_reflvl: typing.Optional[str],
    report_abstract: typing.Optional[str],
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
    multiqc_methods_description: typing.Optional[str],
    extension: typing.Optional[str],
    min_read_counts: typing.Optional[int],
    cutadapt_min_overlap: typing.Optional[int],
    cutadapt_max_error_rate: typing.Optional[float],
    trunc_qmin: typing.Optional[int],
    trunc_rmin: typing.Optional[float],
    max_ee: typing.Optional[int],
    min_len: typing.Optional[int],
    sample_inference: typing.Optional[str],
    vsearch_cluster_id: typing.Optional[float],
    orf_start: typing.Optional[int],
    stop_codons: typing.Optional[str],
    dada_ref_taxonomy: typing.Optional[str],
    pplace_alnmethod: typing.Optional[str],
    kraken2_confidence: typing.Optional[float],
    cut_its: typing.Optional[str],
    its_partial: typing.Optional[int],
    exclude_taxa: typing.Optional[str],
    min_frequency: typing.Optional[int],
    min_samples: typing.Optional[int],
    diversity_rarefaction_depth: typing.Optional[int],
    tax_agglom_min: typing.Optional[int],
    tax_agglom_max: typing.Optional[int],
    ancom_sample_min_count: typing.Optional[int],
    ancombc_effect_size: typing.Optional[float],
    ancombc_significance: typing.Optional[float],
    report_template: typing.Optional[str],
    report_css: typing.Optional[str],
    report_logo: typing.Optional[str],
    report_title: typing.Optional[str],
    seed: typing.Optional[int],
    max_cpus: typing.Optional[int],
    max_memory: typing.Optional[str],
    max_time: typing.Optional[str],
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

    profile_list = ["docker", "test"]

    if len(profile_list) == 0:
        profile_list.append("standard")

    profiles = ",".join(profile_list)

    cmd = [
        "/root/nextflow",
        "run",
        str(shared_dir / "main.nf"),
        "-work-dir",
        str(shared_dir),
        "-profile",
        profiles,
        "-c",
        "latch.config",
        "-resume",
        *get_flag("input", input),
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
        *get_flag("ancombc_significance", ancombc_significance),
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
            "NXF_OPTS": "-Xms1536M -Xmx6144M -XX:ActiveProcessorCount=4",
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


@workflow(metadata._nextflow_metadata)
def nf_nf_core_ampliseq(
    input_source: str,
    run_name: str,
    input: typing.List[SampleSheet],
    input_fasta: typing.Optional[LatchFile],
    input_folder: typing.Optional[LatchDir],
    FW_primer: typing.Optional[str],
    RV_primer: typing.Optional[str],
    metadata: typing.Optional[LatchFile],
    multiregion: typing.Optional[LatchFile],
    outdir: LatchOutputDir,
    save_intermediates: bool,
    email: typing.Optional[str],
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
    trunclenf: typing.Optional[int],
    trunclenr: typing.Optional[int],
    max_len: typing.Optional[int],
    ignore_failed_filtering: bool,
    concatenate_reads: bool,
    vsearch_cluster: bool,
    filter_ssu: typing.Optional[str],
    min_len_asv: typing.Optional[int],
    max_len_asv: typing.Optional[int],
    filter_codons: bool,
    orf_end: typing.Optional[int],
    dada_ref_tax_custom: typing.Optional[str],
    dada_ref_tax_custom_sp: typing.Optional[str],
    dada_assign_taxlevels: typing.Optional[str],
    cut_dada_ref_taxonomy: bool,
    dada_addspecies_allowmultiple: bool,
    dada_taxonomy_rc: bool,
    pplace_tree: typing.Optional[str],
    pplace_aln: typing.Optional[str],
    pplace_model: typing.Optional[str],
    pplace_taxonomy: typing.Optional[str],
    qiime_ref_taxonomy: typing.Optional[str],
    qiime_ref_tax_custom: typing.Optional[str],
    classifier: typing.Optional[str],
    kraken2_ref_taxonomy: typing.Optional[str],
    kraken2_ref_tax_custom: typing.Optional[str],
    kraken2_assign_taxlevels: typing.Optional[str],
    sintax_ref_taxonomy: typing.Optional[str],
    addsh: bool,
    sidle_ref_taxonomy: typing.Optional[str],
    sidle_ref_tax_custom: typing.Optional[str],
    sidle_ref_tree_custom: typing.Optional[str],
    metadata_category: typing.Optional[str],
    metadata_category_barplot: typing.Optional[str],
    qiime_adonis_formula: typing.Optional[str],
    picrust: bool,
    sbdiexport: bool,
    ancom: bool,
    ancombc: bool,
    ancombc_formula: typing.Optional[str],
    ancombc_formula_reflvl: typing.Optional[str],
    report_abstract: typing.Optional[str],
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
    multiqc_methods_description: typing.Optional[str],
    extension: typing.Optional[str] = "/*_R{1,2}_001.fastq.gz",
    min_read_counts: typing.Optional[int] = 1,
    cutadapt_min_overlap: typing.Optional[int] = 3,
    cutadapt_max_error_rate: typing.Optional[float] = 0.1,
    trunc_qmin: typing.Optional[int] = 25,
    trunc_rmin: typing.Optional[float] = 0.75,
    max_ee: typing.Optional[int] = 2,
    min_len: typing.Optional[int] = 50,
    sample_inference: typing.Optional[str] = "independent",
    vsearch_cluster_id: typing.Optional[float] = 0.97,
    orf_start: typing.Optional[int] = 1,
    stop_codons: typing.Optional[str] = "TAA,TAG",
    dada_ref_taxonomy: typing.Optional[str] = "silva=138",
    pplace_alnmethod: typing.Optional[str] = "hmmer",
    kraken2_confidence: typing.Optional[float] = 0.0,
    cut_its: typing.Optional[str] = "none",
    its_partial: typing.Optional[int] = 0,
    exclude_taxa: typing.Optional[str] = "mitochondria,chloroplast",
    min_frequency: typing.Optional[int] = 1,
    min_samples: typing.Optional[int] = 1,
    diversity_rarefaction_depth: typing.Optional[int] = 500,
    tax_agglom_min: typing.Optional[int] = 2,
    tax_agglom_max: typing.Optional[int] = 6,
    ancom_sample_min_count: typing.Optional[int] = 1,
    ancombc_effect_size: typing.Optional[float] = 1.0,
    ancombc_significance: typing.Optional[float] = 0.05,
    report_template: typing.Optional[str] = "${projectDir}/assets/report_template.Rmd",
    report_css: typing.Optional[str] = "${projectDir}/assets/nf-core_style.css",
    report_logo: typing.Optional[str] = "${projectDir}/assets/nf-core-ampliseq_logo_light_long.png",
    report_title: typing.Optional[str] = "Summary of analysis results",
    seed: typing.Optional[int] = 100,
    max_cpus: typing.Optional[int] = 16,
    max_memory: typing.Optional[str] = "128.GB",
    max_time: typing.Optional[str] = "240.h",
) -> None:
    pvc_name: str = initialize()
    nextflow_runtime(
        pvc_name=pvc_name,
        input_source=input_source,
        run_name=run_name,
        input=input,
        input_fasta=input_fasta,
        input_folder=input_folder,
        FW_primer=FW_primer,
        RV_primer=RV_primer,
        metadata=metadata,
        multiregion=multiregion,
        outdir=outdir,
        save_intermediates=save_intermediates,
        email=email,
        illumina_novaseq=illumina_novaseq,
        pacbio=pacbio,
        iontorrent=iontorrent,
        single_end=single_end,
        illumina_pe_its=illumina_pe_its,
        multiple_sequencing_runs=multiple_sequencing_runs,
        extension=extension,
        min_read_counts=min_read_counts,
        ignore_empty_input_files=ignore_empty_input_files,
        retain_untrimmed=retain_untrimmed,
        cutadapt_min_overlap=cutadapt_min_overlap,
        cutadapt_max_error_rate=cutadapt_max_error_rate,
        double_primer=double_primer,
        ignore_failed_trimming=ignore_failed_trimming,
        trunclenf=trunclenf,
        trunclenr=trunclenr,
        trunc_qmin=trunc_qmin,
        trunc_rmin=trunc_rmin,
        max_ee=max_ee,
        min_len=min_len,
        max_len=max_len,
        ignore_failed_filtering=ignore_failed_filtering,
        sample_inference=sample_inference,
        concatenate_reads=concatenate_reads,
        vsearch_cluster=vsearch_cluster,
        vsearch_cluster_id=vsearch_cluster_id,
        filter_ssu=filter_ssu,
        min_len_asv=min_len_asv,
        max_len_asv=max_len_asv,
        filter_codons=filter_codons,
        orf_start=orf_start,
        orf_end=orf_end,
        stop_codons=stop_codons,
        dada_ref_taxonomy=dada_ref_taxonomy,
        dada_ref_tax_custom=dada_ref_tax_custom,
        dada_ref_tax_custom_sp=dada_ref_tax_custom_sp,
        dada_assign_taxlevels=dada_assign_taxlevels,
        cut_dada_ref_taxonomy=cut_dada_ref_taxonomy,
        dada_addspecies_allowmultiple=dada_addspecies_allowmultiple,
        dada_taxonomy_rc=dada_taxonomy_rc,
        pplace_tree=pplace_tree,
        pplace_aln=pplace_aln,
        pplace_model=pplace_model,
        pplace_alnmethod=pplace_alnmethod,
        pplace_taxonomy=pplace_taxonomy,
        qiime_ref_taxonomy=qiime_ref_taxonomy,
        qiime_ref_tax_custom=qiime_ref_tax_custom,
        classifier=classifier,
        kraken2_ref_taxonomy=kraken2_ref_taxonomy,
        kraken2_ref_tax_custom=kraken2_ref_tax_custom,
        kraken2_assign_taxlevels=kraken2_assign_taxlevels,
        kraken2_confidence=kraken2_confidence,
        sintax_ref_taxonomy=sintax_ref_taxonomy,
        addsh=addsh,
        cut_its=cut_its,
        its_partial=its_partial,
        sidle_ref_taxonomy=sidle_ref_taxonomy,
        sidle_ref_tax_custom=sidle_ref_tax_custom,
        sidle_ref_tree_custom=sidle_ref_tree_custom,
        exclude_taxa=exclude_taxa,
        min_frequency=min_frequency,
        min_samples=min_samples,
        metadata_category=metadata_category,
        metadata_category_barplot=metadata_category_barplot,
        qiime_adonis_formula=qiime_adonis_formula,
        picrust=picrust,
        sbdiexport=sbdiexport,
        diversity_rarefaction_depth=diversity_rarefaction_depth,
        tax_agglom_min=tax_agglom_min,
        tax_agglom_max=tax_agglom_max,
        ancom_sample_min_count=ancom_sample_min_count,
        ancom=ancom,
        ancombc=ancombc,
        ancombc_formula=ancombc_formula,
        ancombc_formula_reflvl=ancombc_formula_reflvl,
        ancombc_effect_size=ancombc_effect_size,
        ancombc_significance=ancombc_significance,
        report_template=report_template,
        report_css=report_css,
        report_logo=report_logo,
        report_title=report_title,
        report_abstract=report_abstract,
        skip_fastqc=skip_fastqc,
        skip_cutadapt=skip_cutadapt,
        skip_dada_quality=skip_dada_quality,
        skip_barrnap=skip_barrnap,
        skip_qiime=skip_qiime,
        skip_qiime_downstream=skip_qiime_downstream,
        skip_taxonomy=skip_taxonomy,
        skip_dada_taxonomy=skip_dada_taxonomy,
        skip_dada_addspecies=skip_dada_addspecies,
        skip_barplot=skip_barplot,
        skip_abundance_tables=skip_abundance_tables,
        skip_alpha_rarefaction=skip_alpha_rarefaction,
        skip_diversity_indices=skip_diversity_indices,
        skip_multiqc=skip_multiqc,
        skip_report=skip_report,
        seed=seed,
        multiqc_methods_description=multiqc_methods_description,
        max_cpus=max_cpus,
        max_memory=max_memory,
        max_time=max_time,
    )
