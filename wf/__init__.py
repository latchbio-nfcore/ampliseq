from typing import Annotated, List, Optional

from flytekit.core.annotation import FlyteAnnotation
from latch.resources.launch_plan import LaunchPlan
from latch.resources.workflow import workflow
from latch.types import metadata
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile

from wf.entrypoint import (
    DADATaxonomy,
    Kraken2RefTaxonomy,
    QiimeRefTaxonomy,
    SampleSheet,
    SidleRefTaxonomy,
    SintaxRefTaxonomy,
    initialize,
    nextflow_runtime,
)


@workflow(metadata._nextflow_metadata)
def nf_nf_core_ampliseq(
    input_source: str,
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
    """
    nfcore/ampliseq is a analysis pipeline used for amplicon sequencing, supporting denoising of any amplicon and supports a variety of taxonomic databases for taxonomic assignment including 16S, ITS, CO1 and 18S. Phylogenetic placement is also possible. Multiple region analysis such as 5R is implemented.

    <html>
    <p align="center">
    <img src="https://user-images.githubusercontent.com/31255434/182289305-4cc620e3-86ae-480f-9b61-6ca83283caa5.jpg" alt="Latch Verified" width="100">
    </p>

    <p align="center">
    <strong>
    Latch Verified
    </strong>
    </p>

    <p align="center">

    [![Cite with Zenodo](http://img.shields.io/badge/DOI-10.5281/zenodo.1493841-1073c8?labelColor=000000)](https://doi.org/10.5281/zenodo.1493841)[![Cite Publication](https://img.shields.io/badge/Cite%20Us!-Cite%20Publication-important?labelColor=000000)](https://doi.org/10.3389/fmicb.2020.550420)

    **nfcore/ampliseq** is a bioinformatics analysis pipeline used for amplicon sequencing, supporting denoising of any amplicon and supports a variety of taxonomic databases for taxonomic assignment including 16S, ITS, CO1 and 18S. Phylogenetic placement is also possible. Multiple region analysis such as 5R is implemented. Supported is paired-end Illumina or single-end Illumina, PacBio and IonTorrent data. Default is the analysis of 16S rRNA gene amplicons sequenced paired-end with Illumina.

    This workflow is hosted on Latch Workflows, using a native Nextflow integration, with a graphical interface for accessible analysis by scientists. There is also an integration with Latch Registry so that batched workflows can be launched from “graphical sample sheets” or tables associating raw sequencing files with metadata.

    The managed computing infrastructure scales to hundreds of samples, with clear logging and error-reporting. Data provenance links versioned and containerized workflow code to input and output files.

    </p>

    </html>

    A video about relevance, usage and output of the pipeline (version 2.1.0; 26th Oct. 2021) can also be found in [YouTube](https://youtu.be/a0VOEeAvETs) and [billibilli](https://www.bilibili.com/video/BV1B44y1e7MM), the slides are deposited at [figshare](https://doi.org/10.6084/m9.figshare.16871008.v1).

    The pipeline is built using [Nextflow](https://www.nextflow.io), a workflow tool to run tasks across multiple compute infrastructures in a very portable manner. It uses Docker/Singularity containers making installation trivial and results highly reproducible. The [Nextflow DSL2](https://www.nextflow.io/docs/latest/dsl2.html) implementation of this pipeline uses one container per process which makes it much easier to maintain and update software dependencies.

    ## Pipeline summary

    By default, the pipeline currently performs the following:

    - Sequencing quality control ([FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/))
    - Trimming of reads ([Cutadapt](https://journal.embnet.org/index.php/embnetjournal/article/view/200))
    - Infer Amplicon Sequence Variants (ASVs) ([DADA2](https://doi.org/10.1038/nmeth.3869))
    - Optional post-clustering with [VSEARCH](https://github.com/torognes/vsearch)
    - Predict whether ASVs are ribosomal RNA sequences ([Barrnap](https://github.com/tseemann/barrnap))
    - Phylogenetic placement ([EPA-NG](https://github.com/Pbdas/epa-ng))
    - Taxonomical classification using DADA2; alternatives are [SINTAX](https://doi.org/10.1101/074161), [Kraken2](https://doi.org/10.1186/s13059-019-1891-0), and [QIIME2](https://www.nature.com/articles/s41587-019-0209-9)
    - Excludes unwanted taxa, produces absolute and relative feature/taxa count tables and plots, plots alpha rarefaction curves, computes alpha and beta diversity indices and plots thereof ([QIIME2](https://www.nature.com/articles/s41587-019-0209-9))
    - Creates phyloseq R objects ([Phyloseq](https://www.bioconductor.org/packages/release/bioc/html/phyloseq.html))
    - Pipeline QC summaries ([MultiQC](https://multiqc.info/))
    - Pipeline summary report ([R Markdown](https://github.com/rstudio/rmarkdown))

    ## Pipeline output

    To see the results of an example test run with a full size dataset refer to the [results](https://nf-co.re/ampliseq/results) tab on the nf-core website pipeline page.
    For more details about the output files and reports, please refer to the
    [output documentation](https://nf-co.re/ampliseq/output).

    ## Credits

    nf-core/ampliseq was originally written by Daniel Straub ([@d4straub](https://github.com/d4straub)) and Alexander Peltzer ([@apeltzer](https://github.com/apeltzer)) for use at the [Quantitative Biology Center (QBiC)](http://www.qbic.life) and [Microbial Ecology, Center for Applied Geosciences](http://www.uni-tuebingen.de/de/104325), part of Eberhard Karls Universität Tübingen (Germany). Daniel Lundin [@erikrikarddaniel](https://github.com/erikrikarddaniel) ([Linnaeus University, Sweden](https://lnu.se/)) joined before pipeline release 2.0.0 and helped to improve the pipeline considerably.

    We thank the following people for their extensive assistance in the development of this pipeline (in alphabetical order):

    [Adam Bennett](https://github.com/a4000), [Diego Brambilla](https://github.com/DiegoBrambilla), [Emelie Nilsson](https://github.com/emnilsson), [Jeanette Tångrot](https://github.com/jtangrot), [Lokeshwaran Manoharan](https://github.com/lokeshbio), [Marissa Dubbelaar](https://github.com/marissaDubbelaar), [Sabrina Krakau](https://github.com/skrakau), [Sam Minot](https://github.com/sminot), [Till Englert](https://github.com/tillenglert)

    ## Contributions and Support

    If you would like to contribute to this pipeline, please see the [contributing guidelines](.github/CONTRIBUTING.md).

    For further information or help, don't hesitate to get in touch on the [Slack `#ampliseq` channel](https://nfcore.slack.com/channels/ampliseq) (you can join with [this invite](https://nf-co.re/join/slack)).

    ## Citations

    If you use `nf-core/ampliseq` for your analysis, please cite the `ampliseq` article as follows:

    > **Interpretations of Environmental Microbial Community Studies Are Biased by the Selected 16S rRNA (Gene) Amplicon Sequencing Pipeline**
    >
    > Daniel Straub, Nia Blackwell, Adrian Langarica-Fuentes, Alexander Peltzer, Sven Nahnsen, Sara Kleindienst
    >
    > _Frontiers in Microbiology_ 2020, 11:2652 [doi: 10.3389/fmicb.2020.550420](https://doi.org/10.3389/fmicb.2020.550420).

    You can cite the `nf-core/ampliseq` zenodo record for a specific version using the following [doi: 10.5281/zenodo.1493841](https://zenodo.org/badge/latestdoi/150448201)

    An extensive list of references for the tools used by the pipeline can be found in the [`CITATIONS.md`](CITATIONS.md) file.

    You can cite the `nf-core` publication as follows:

    > **The nf-core framework for community-curated bioinformatics pipelines.**
    >
    > Philip Ewels, Alexander Peltzer, Sven Fillinger, Harshil Patel, Johannes Alneberg, Andreas Wilm, Maxime Ulysse Garcia, Paolo Di Tommaso & Sven Nahnsen.
    >
    > _Nat Biotechnol._ 2020 Feb 13. doi: [10.1038/s41587-020-0439-x](https://dx.doi.org/10.1038/s41587-020-0439-x).

    """
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


LaunchPlan(
    nf_nf_core_ampliseq,
    "Small Test",
    {
        "input": [
            SampleSheet(
                sampleid="sampleID_1a",
                forwardreads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/1a_S103_L001_R1_001.fastq.gz"),
                reversereads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/1a_S103_L001_R2_001.fastq.gz"),
                run=None,
            ),
            SampleSheet(
                sampleid="sampleID_1",
                forwardreads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/1_S103_L001_R1_001.fastq.gz"),
                reversereads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/1_S103_L001_R2_001.fastq.gz"),
                run=None,
            ),
            SampleSheet(
                sampleid="sampleID_2a",
                forwardreads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/2a_S115_L001_R1_001.fastq.gz"),
                reversereads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/2a_S115_L001_R2_001.fastq.gz"),
                run=None,
            ),
            SampleSheet(
                sampleid="sampleID_2",
                forwardreads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/2_S115_L001_R1_001.fastq.gz"),
                reversereads=LatchFile("s3://latch-public/nf-core/ampliseq/test_data/2_S115_L001_R2_001.fastq.gz"),
                run=None,
            ),
        ],
        "metadata": LatchFile("s3://latch-public/nf-core/ampliseq/test_data/Metadata.tsv"),
        "run_name": "Test_run",
        "FW_primer": "GTGYCAGCMGCCGCGGTAA",
        "RV_primer": "GGACTACNVGGGTWTCTAAT",
        "dada_ref_taxonomy": DADATaxonomy.gtdb_R07_RS207,
        "cut_dada_ref_taxonomy": True,
        "qiime_ref_taxonomy": QiimeRefTaxonomy.greengenes85,
        "max_len_asv": 255,
        "filter_ssu": "bac",
        "min_samples": 2,
        "min_frequency": 10,
        "metadata_category_barplot": "treatment1,badpairwise10",
        "tax_agglom_max": 4,
        "sbdiexport": True,
        "qiime_adonis_formula": "treatment1,mix8",
        "diversity_rarefaction_depth": 500,
        "vsearch_cluster": True,
        "ancombc": True,
        "ancombc_formula": "treatment1",
        "ancombc_formula_reflvl": "treatment1::b",
        "ancombc_effect_size": 2.0,
        "ancombc_significance": 0.00001,
    },
)
