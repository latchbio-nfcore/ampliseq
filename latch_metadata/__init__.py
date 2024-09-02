from pathlib import Path

from latch.types.directory import LatchDir
from latch.types.metadata import (
    Fork,
    ForkBranch,
    LatchAuthor,
    NextflowMetadata,
    NextflowParameter,
    NextflowRuntimeResources,
    Params,
    Section,
    Spoiler,
    Text,
)

from .parameters import generated_parameters

flow = [
    Section(
        "Input/Output",
        Params(
            "input",
            "input_fasta",
            "input_folder",
            "FW_primer",
            "RV_primer",
            "metadata",
            "multiregion",
            "outdir",
            "save_intermediates",
            "email",
        ),
    ),
    Section(
        "Sequencing Input Options",
        Params(
            "illumina_novaseq",
            "pacbio",
            "iontorrent",
            "single_end",
            "illumina_pe_its",
            "multiple_sequencing_runs",
            "extension",
            "min_read_counts",
            "ignore_empty_input_files",
        ),
    ),
    Section(
        "Primer Removal",
        Params(
            "retain_untrimmed",
            "cutadapt_min_overlap",
            "cutadapt_max_error_rate",
            "double_primer",
            "ignore_failed_trimming",
        ),
    ),
    Section(
        "Read Processing",
        Params(
            "trunclenf",
            "trunclenr",
            "trunc_qmin",
            "trunc_rmin",
            "max_ee",
            "min_len",
            "max_len",
            "ignore_failed_filtering",
        ),
    ),
    Section(
        "ASV Calculation and Post-processing",
        Params(
            "sample_inference",
            "concatenate_reads",
            "vsearch_cluster",
            "vsearch_cluster_id",
            "filter_ssu",
            "min_len_asv",
            "max_len_asv",
            "filter_codons",
            "orf_start",
            "orf_end",
            "stop_codons",
        ),
    ),
    Section(
        "Taxonomic Classification",
        Params(
            "dada_ref_taxonomy",
            "dada_ref_tax_custom",
            "dada_ref_tax_custom_sp",
            "dada_assign_taxlevels",
            "cut_dada_ref_taxonomy",
            "dada_addspecies_allowmultiple",
            "dada_taxonomy_rc",
            "pplace_tree",
            "pplace_aln",
            "pplace_model",
            "pplace_alnmethod",
            "pplace_taxonomy",
            "qiime_ref_taxonomy",
            "qiime_ref_tax_custom",
            "classifier",
            "kraken2_ref_taxonomy",
            "kraken2_ref_tax_custom",
            "kraken2_assign_taxlevels",
            "kraken2_confidence",
            "sintax_ref_taxonomy",
            "addsh",
            "cut_its",
            "its_partial",
        ),
    ),
    Section(
        "Multi-region Analysis",
        Params(
            "sidle_ref_taxonomy",
            "sidle_ref_tax_custom",
            "sidle_ref_tree_custom",
        ),
    ),
    Section(
        "ASV Filtering",
        Params(
            "exclude_taxa",
            "min_frequency",
            "min_samples",
        ),
    ),
    Section(
        "Downstream Analysis",
        Params(
            "metadata_category",
            "metadata_category_barplot",
            "qiime_adonis_formula",
            "picrust",
            "sbdiexport",
            "diversity_rarefaction_depth",
            "tax_agglom_min",
            "tax_agglom_max",
        ),
    ),
    Section(
        "Differential Abundance Analysis",
        Params(
            "ancom_sample_min_count",
            "ancom",
            "ancombc",
            "ancombc_formula",
            "ancombc_formula_reflvl",
            "ancombc_effect_size",
            "ancombc_significance",
        ),
    ),
    Section(
        "Report Customization",
        Params(
            "report_template",
            "report_css",
            "report_logo",
            "report_title",
            "report_abstract",
        ),
    ),
    Spoiler(
        "Advanced Options",
        Section(
            "Step Skipping",
            Params(
                "skip_fastqc",
                "skip_cutadapt",
                "skip_dada_quality",
                "skip_barrnap",
                "skip_qiime",
                "skip_qiime_downstream",
                "skip_taxonomy",
                "skip_dada_taxonomy",
                "skip_dada_addspecies",
                "skip_barplot",
                "skip_abundance_tables",
                "skip_alpha_rarefaction",
                "skip_diversity_indices",
                "skip_multiqc",
                "skip_report",
            ),
        ),
        Section(
            "Other Options",
            Params(
                "seed",
                "multiqc_methods_description",
            ),
        ),
        Section(
            "Resource Limits",
            Params(
                "max_cpus",
                "max_memory",
                "max_time",
            ),
        ),
    ),
]

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
