from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

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
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: typing.Optional[LatchFile], input_fasta: typing.Optional[LatchFile], input_folder: typing.Optional[LatchDir], FW_primer: typing.Optional[str], RV_primer: typing.Optional[str], metadata: typing.Optional[LatchFile], multiregion: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], save_intermediates: typing.Optional[bool], email: typing.Optional[str], illumina_novaseq: typing.Optional[bool], pacbio: typing.Optional[bool], iontorrent: typing.Optional[bool], single_end: typing.Optional[bool], illumina_pe_its: typing.Optional[bool], multiple_sequencing_runs: typing.Optional[bool], ignore_empty_input_files: typing.Optional[bool], retain_untrimmed: typing.Optional[bool], double_primer: typing.Optional[bool], ignore_failed_trimming: typing.Optional[bool], trunclenf: typing.Optional[int], trunclenr: typing.Optional[int], max_len: typing.Optional[int], ignore_failed_filtering: typing.Optional[bool], concatenate_reads: typing.Optional[bool], vsearch_cluster: typing.Optional[bool], filter_ssu: typing.Optional[str], min_len_asv: typing.Optional[int], max_len_asv: typing.Optional[int], filter_codons: typing.Optional[bool], orf_end: typing.Optional[int], dada_ref_tax_custom: typing.Optional[str], dada_ref_tax_custom_sp: typing.Optional[str], dada_assign_taxlevels: typing.Optional[str], cut_dada_ref_taxonomy: typing.Optional[bool], dada_addspecies_allowmultiple: typing.Optional[bool], dada_taxonomy_rc: typing.Optional[bool], pplace_tree: typing.Optional[str], pplace_aln: typing.Optional[str], pplace_model: typing.Optional[str], pplace_taxonomy: typing.Optional[str], qiime_ref_taxonomy: typing.Optional[str], qiime_ref_tax_custom: typing.Optional[str], classifier: typing.Optional[str], kraken2_ref_taxonomy: typing.Optional[str], kraken2_ref_tax_custom: typing.Optional[str], kraken2_assign_taxlevels: typing.Optional[str], sintax_ref_taxonomy: typing.Optional[str], addsh: typing.Optional[bool], sidle_ref_taxonomy: typing.Optional[str], sidle_ref_tax_custom: typing.Optional[str], sidle_ref_tree_custom: typing.Optional[str], metadata_category: typing.Optional[str], metadata_category_barplot: typing.Optional[str], qiime_adonis_formula: typing.Optional[str], picrust: typing.Optional[bool], sbdiexport: typing.Optional[bool], report_abstract: typing.Optional[str], skip_fastqc: typing.Optional[bool], skip_cutadapt: typing.Optional[bool], skip_dada_quality: typing.Optional[bool], skip_barrnap: typing.Optional[bool], skip_qiime: typing.Optional[bool], skip_qiime_downstream: typing.Optional[bool], skip_taxonomy: typing.Optional[bool], skip_dada_taxonomy: typing.Optional[bool], skip_dada_addspecies: typing.Optional[bool], skip_barplot: typing.Optional[bool], skip_abundance_tables: typing.Optional[bool], skip_alpha_rarefaction: typing.Optional[bool], skip_diversity_indices: typing.Optional[bool], skip_ancom: typing.Optional[bool], skip_multiqc: typing.Optional[bool], skip_report: typing.Optional[bool], multiqc_methods_description: typing.Optional[str], extension: typing.Optional[str], min_read_counts: typing.Optional[int], cutadapt_min_overlap: typing.Optional[int], cutadapt_max_error_rate: typing.Optional[float], trunc_qmin: typing.Optional[int], trunc_rmin: typing.Optional[float], max_ee: typing.Optional[int], min_len: typing.Optional[int], sample_inference: typing.Optional[str], vsearch_cluster_id: typing.Optional[float], orf_start: typing.Optional[int], stop_codons: typing.Optional[str], dada_ref_taxonomy: typing.Optional[str], pplace_alnmethod: typing.Optional[str], kraken2_confidence: typing.Optional[float], cut_its: typing.Optional[str], its_partial: typing.Optional[int], exclude_taxa: typing.Optional[str], min_frequency: typing.Optional[int], min_samples: typing.Optional[int], diversity_rarefaction_depth: typing.Optional[int], ancom_sample_min_count: typing.Optional[int], tax_agglom_min: typing.Optional[int], tax_agglom_max: typing.Optional[int], report_template: typing.Optional[str], report_css: typing.Optional[str], report_logo: typing.Optional[str], report_title: typing.Optional[str], seed: typing.Optional[int], max_cpus: typing.Optional[int], max_memory: typing.Optional[str], max_time: typing.Optional[str]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
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
                *get_flag('input', input),
                *get_flag('input_fasta', input_fasta),
                *get_flag('input_folder', input_folder),
                *get_flag('FW_primer', FW_primer),
                *get_flag('RV_primer', RV_primer),
                *get_flag('metadata', metadata),
                *get_flag('multiregion', multiregion),
                *get_flag('outdir', outdir),
                *get_flag('save_intermediates', save_intermediates),
                *get_flag('email', email),
                *get_flag('illumina_novaseq', illumina_novaseq),
                *get_flag('pacbio', pacbio),
                *get_flag('iontorrent', iontorrent),
                *get_flag('single_end', single_end),
                *get_flag('illumina_pe_its', illumina_pe_its),
                *get_flag('multiple_sequencing_runs', multiple_sequencing_runs),
                *get_flag('extension', extension),
                *get_flag('min_read_counts', min_read_counts),
                *get_flag('ignore_empty_input_files', ignore_empty_input_files),
                *get_flag('retain_untrimmed', retain_untrimmed),
                *get_flag('cutadapt_min_overlap', cutadapt_min_overlap),
                *get_flag('cutadapt_max_error_rate', cutadapt_max_error_rate),
                *get_flag('double_primer', double_primer),
                *get_flag('ignore_failed_trimming', ignore_failed_trimming),
                *get_flag('trunclenf', trunclenf),
                *get_flag('trunclenr', trunclenr),
                *get_flag('trunc_qmin', trunc_qmin),
                *get_flag('trunc_rmin', trunc_rmin),
                *get_flag('max_ee', max_ee),
                *get_flag('min_len', min_len),
                *get_flag('max_len', max_len),
                *get_flag('ignore_failed_filtering', ignore_failed_filtering),
                *get_flag('sample_inference', sample_inference),
                *get_flag('concatenate_reads', concatenate_reads),
                *get_flag('vsearch_cluster', vsearch_cluster),
                *get_flag('vsearch_cluster_id', vsearch_cluster_id),
                *get_flag('filter_ssu', filter_ssu),
                *get_flag('min_len_asv', min_len_asv),
                *get_flag('max_len_asv', max_len_asv),
                *get_flag('filter_codons', filter_codons),
                *get_flag('orf_start', orf_start),
                *get_flag('orf_end', orf_end),
                *get_flag('stop_codons', stop_codons),
                *get_flag('dada_ref_taxonomy', dada_ref_taxonomy),
                *get_flag('dada_ref_tax_custom', dada_ref_tax_custom),
                *get_flag('dada_ref_tax_custom_sp', dada_ref_tax_custom_sp),
                *get_flag('dada_assign_taxlevels', dada_assign_taxlevels),
                *get_flag('cut_dada_ref_taxonomy', cut_dada_ref_taxonomy),
                *get_flag('dada_addspecies_allowmultiple', dada_addspecies_allowmultiple),
                *get_flag('dada_taxonomy_rc', dada_taxonomy_rc),
                *get_flag('pplace_tree', pplace_tree),
                *get_flag('pplace_aln', pplace_aln),
                *get_flag('pplace_model', pplace_model),
                *get_flag('pplace_alnmethod', pplace_alnmethod),
                *get_flag('pplace_taxonomy', pplace_taxonomy),
                *get_flag('qiime_ref_taxonomy', qiime_ref_taxonomy),
                *get_flag('qiime_ref_tax_custom', qiime_ref_tax_custom),
                *get_flag('classifier', classifier),
                *get_flag('kraken2_ref_taxonomy', kraken2_ref_taxonomy),
                *get_flag('kraken2_ref_tax_custom', kraken2_ref_tax_custom),
                *get_flag('kraken2_assign_taxlevels', kraken2_assign_taxlevels),
                *get_flag('kraken2_confidence', kraken2_confidence),
                *get_flag('sintax_ref_taxonomy', sintax_ref_taxonomy),
                *get_flag('addsh', addsh),
                *get_flag('cut_its', cut_its),
                *get_flag('its_partial', its_partial),
                *get_flag('sidle_ref_taxonomy', sidle_ref_taxonomy),
                *get_flag('sidle_ref_tax_custom', sidle_ref_tax_custom),
                *get_flag('sidle_ref_tree_custom', sidle_ref_tree_custom),
                *get_flag('exclude_taxa', exclude_taxa),
                *get_flag('min_frequency', min_frequency),
                *get_flag('min_samples', min_samples),
                *get_flag('metadata_category', metadata_category),
                *get_flag('metadata_category_barplot', metadata_category_barplot),
                *get_flag('qiime_adonis_formula', qiime_adonis_formula),
                *get_flag('picrust', picrust),
                *get_flag('sbdiexport', sbdiexport),
                *get_flag('diversity_rarefaction_depth', diversity_rarefaction_depth),
                *get_flag('ancom_sample_min_count', ancom_sample_min_count),
                *get_flag('tax_agglom_min', tax_agglom_min),
                *get_flag('tax_agglom_max', tax_agglom_max),
                *get_flag('report_template', report_template),
                *get_flag('report_css', report_css),
                *get_flag('report_logo', report_logo),
                *get_flag('report_title', report_title),
                *get_flag('report_abstract', report_abstract),
                *get_flag('skip_fastqc', skip_fastqc),
                *get_flag('skip_cutadapt', skip_cutadapt),
                *get_flag('skip_dada_quality', skip_dada_quality),
                *get_flag('skip_barrnap', skip_barrnap),
                *get_flag('skip_qiime', skip_qiime),
                *get_flag('skip_qiime_downstream', skip_qiime_downstream),
                *get_flag('skip_taxonomy', skip_taxonomy),
                *get_flag('skip_dada_taxonomy', skip_dada_taxonomy),
                *get_flag('skip_dada_addspecies', skip_dada_addspecies),
                *get_flag('skip_barplot', skip_barplot),
                *get_flag('skip_abundance_tables', skip_abundance_tables),
                *get_flag('skip_alpha_rarefaction', skip_alpha_rarefaction),
                *get_flag('skip_diversity_indices', skip_diversity_indices),
                *get_flag('skip_ancom', skip_ancom),
                *get_flag('skip_multiqc', skip_multiqc),
                *get_flag('skip_report', skip_report),
                *get_flag('seed', seed),
                *get_flag('multiqc_methods_description', multiqc_methods_description),
                *get_flag('max_cpus', max_cpus),
                *get_flag('max_memory', max_memory),
                *get_flag('max_time', max_time)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
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



@workflow(metadata._nextflow_metadata)
def nf_nf_core_ampliseq(input: typing.Optional[LatchFile], input_fasta: typing.Optional[LatchFile], input_folder: typing.Optional[LatchDir], FW_primer: typing.Optional[str], RV_primer: typing.Optional[str], metadata: typing.Optional[LatchFile], multiregion: typing.Optional[LatchFile], outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], save_intermediates: typing.Optional[bool], email: typing.Optional[str], illumina_novaseq: typing.Optional[bool], pacbio: typing.Optional[bool], iontorrent: typing.Optional[bool], single_end: typing.Optional[bool], illumina_pe_its: typing.Optional[bool], multiple_sequencing_runs: typing.Optional[bool], ignore_empty_input_files: typing.Optional[bool], retain_untrimmed: typing.Optional[bool], double_primer: typing.Optional[bool], ignore_failed_trimming: typing.Optional[bool], trunclenf: typing.Optional[int], trunclenr: typing.Optional[int], max_len: typing.Optional[int], ignore_failed_filtering: typing.Optional[bool], concatenate_reads: typing.Optional[bool], vsearch_cluster: typing.Optional[bool], filter_ssu: typing.Optional[str], min_len_asv: typing.Optional[int], max_len_asv: typing.Optional[int], filter_codons: typing.Optional[bool], orf_end: typing.Optional[int], dada_ref_tax_custom: typing.Optional[str], dada_ref_tax_custom_sp: typing.Optional[str], dada_assign_taxlevels: typing.Optional[str], cut_dada_ref_taxonomy: typing.Optional[bool], dada_addspecies_allowmultiple: typing.Optional[bool], dada_taxonomy_rc: typing.Optional[bool], pplace_tree: typing.Optional[str], pplace_aln: typing.Optional[str], pplace_model: typing.Optional[str], pplace_taxonomy: typing.Optional[str], qiime_ref_taxonomy: typing.Optional[str], qiime_ref_tax_custom: typing.Optional[str], classifier: typing.Optional[str], kraken2_ref_taxonomy: typing.Optional[str], kraken2_ref_tax_custom: typing.Optional[str], kraken2_assign_taxlevels: typing.Optional[str], sintax_ref_taxonomy: typing.Optional[str], addsh: typing.Optional[bool], sidle_ref_taxonomy: typing.Optional[str], sidle_ref_tax_custom: typing.Optional[str], sidle_ref_tree_custom: typing.Optional[str], metadata_category: typing.Optional[str], metadata_category_barplot: typing.Optional[str], qiime_adonis_formula: typing.Optional[str], picrust: typing.Optional[bool], sbdiexport: typing.Optional[bool], report_abstract: typing.Optional[str], skip_fastqc: typing.Optional[bool], skip_cutadapt: typing.Optional[bool], skip_dada_quality: typing.Optional[bool], skip_barrnap: typing.Optional[bool], skip_qiime: typing.Optional[bool], skip_qiime_downstream: typing.Optional[bool], skip_taxonomy: typing.Optional[bool], skip_dada_taxonomy: typing.Optional[bool], skip_dada_addspecies: typing.Optional[bool], skip_barplot: typing.Optional[bool], skip_abundance_tables: typing.Optional[bool], skip_alpha_rarefaction: typing.Optional[bool], skip_diversity_indices: typing.Optional[bool], skip_ancom: typing.Optional[bool], skip_multiqc: typing.Optional[bool], skip_report: typing.Optional[bool], multiqc_methods_description: typing.Optional[str], extension: typing.Optional[str] = '/*_R{1,2}_001.fastq.gz', min_read_counts: typing.Optional[int] = 1, cutadapt_min_overlap: typing.Optional[int] = 3, cutadapt_max_error_rate: typing.Optional[float] = 0.1, trunc_qmin: typing.Optional[int] = 25, trunc_rmin: typing.Optional[float] = 0.75, max_ee: typing.Optional[int] = 2, min_len: typing.Optional[int] = 50, sample_inference: typing.Optional[str] = 'independent', vsearch_cluster_id: typing.Optional[float] = 0.97, orf_start: typing.Optional[int] = 1, stop_codons: typing.Optional[str] = 'TAA,TAG', dada_ref_taxonomy: typing.Optional[str] = 'silva=138', pplace_alnmethod: typing.Optional[str] = 'hmmer', kraken2_confidence: typing.Optional[float] = 0.0, cut_its: typing.Optional[str] = 'none', its_partial: typing.Optional[int] = 0, exclude_taxa: typing.Optional[str] = 'mitochondria,chloroplast', min_frequency: typing.Optional[int] = 1, min_samples: typing.Optional[int] = 1, diversity_rarefaction_depth: typing.Optional[int] = 500, ancom_sample_min_count: typing.Optional[int] = 1, tax_agglom_min: typing.Optional[int] = 2, tax_agglom_max: typing.Optional[int] = 6, report_template: typing.Optional[str] = '${projectDir}/assets/report_template.Rmd', report_css: typing.Optional[str] = '${projectDir}/assets/nf-core_style.css', report_logo: typing.Optional[str] = '${projectDir}/assets/nf-core-ampliseq_logo_light_long.png', report_title: typing.Optional[str] = 'Summary of analysis results', seed: typing.Optional[int] = 100, max_cpus: typing.Optional[int] = 16, max_memory: typing.Optional[str] = '128.GB', max_time: typing.Optional[str] = '240.h') -> None:
    """
    nf-core/ampliseq

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, input_fasta=input_fasta, input_folder=input_folder, FW_primer=FW_primer, RV_primer=RV_primer, metadata=metadata, multiregion=multiregion, outdir=outdir, save_intermediates=save_intermediates, email=email, illumina_novaseq=illumina_novaseq, pacbio=pacbio, iontorrent=iontorrent, single_end=single_end, illumina_pe_its=illumina_pe_its, multiple_sequencing_runs=multiple_sequencing_runs, extension=extension, min_read_counts=min_read_counts, ignore_empty_input_files=ignore_empty_input_files, retain_untrimmed=retain_untrimmed, cutadapt_min_overlap=cutadapt_min_overlap, cutadapt_max_error_rate=cutadapt_max_error_rate, double_primer=double_primer, ignore_failed_trimming=ignore_failed_trimming, trunclenf=trunclenf, trunclenr=trunclenr, trunc_qmin=trunc_qmin, trunc_rmin=trunc_rmin, max_ee=max_ee, min_len=min_len, max_len=max_len, ignore_failed_filtering=ignore_failed_filtering, sample_inference=sample_inference, concatenate_reads=concatenate_reads, vsearch_cluster=vsearch_cluster, vsearch_cluster_id=vsearch_cluster_id, filter_ssu=filter_ssu, min_len_asv=min_len_asv, max_len_asv=max_len_asv, filter_codons=filter_codons, orf_start=orf_start, orf_end=orf_end, stop_codons=stop_codons, dada_ref_taxonomy=dada_ref_taxonomy, dada_ref_tax_custom=dada_ref_tax_custom, dada_ref_tax_custom_sp=dada_ref_tax_custom_sp, dada_assign_taxlevels=dada_assign_taxlevels, cut_dada_ref_taxonomy=cut_dada_ref_taxonomy, dada_addspecies_allowmultiple=dada_addspecies_allowmultiple, dada_taxonomy_rc=dada_taxonomy_rc, pplace_tree=pplace_tree, pplace_aln=pplace_aln, pplace_model=pplace_model, pplace_alnmethod=pplace_alnmethod, pplace_taxonomy=pplace_taxonomy, qiime_ref_taxonomy=qiime_ref_taxonomy, qiime_ref_tax_custom=qiime_ref_tax_custom, classifier=classifier, kraken2_ref_taxonomy=kraken2_ref_taxonomy, kraken2_ref_tax_custom=kraken2_ref_tax_custom, kraken2_assign_taxlevels=kraken2_assign_taxlevels, kraken2_confidence=kraken2_confidence, sintax_ref_taxonomy=sintax_ref_taxonomy, addsh=addsh, cut_its=cut_its, its_partial=its_partial, sidle_ref_taxonomy=sidle_ref_taxonomy, sidle_ref_tax_custom=sidle_ref_tax_custom, sidle_ref_tree_custom=sidle_ref_tree_custom, exclude_taxa=exclude_taxa, min_frequency=min_frequency, min_samples=min_samples, metadata_category=metadata_category, metadata_category_barplot=metadata_category_barplot, qiime_adonis_formula=qiime_adonis_formula, picrust=picrust, sbdiexport=sbdiexport, diversity_rarefaction_depth=diversity_rarefaction_depth, ancom_sample_min_count=ancom_sample_min_count, tax_agglom_min=tax_agglom_min, tax_agglom_max=tax_agglom_max, report_template=report_template, report_css=report_css, report_logo=report_logo, report_title=report_title, report_abstract=report_abstract, skip_fastqc=skip_fastqc, skip_cutadapt=skip_cutadapt, skip_dada_quality=skip_dada_quality, skip_barrnap=skip_barrnap, skip_qiime=skip_qiime, skip_qiime_downstream=skip_qiime_downstream, skip_taxonomy=skip_taxonomy, skip_dada_taxonomy=skip_dada_taxonomy, skip_dada_addspecies=skip_dada_addspecies, skip_barplot=skip_barplot, skip_abundance_tables=skip_abundance_tables, skip_alpha_rarefaction=skip_alpha_rarefaction, skip_diversity_indices=skip_diversity_indices, skip_ancom=skip_ancom, skip_multiqc=skip_multiqc, skip_report=skip_report, seed=seed, multiqc_methods_description=multiqc_methods_description, max_cpus=max_cpus, max_memory=max_memory, max_time=max_time)

