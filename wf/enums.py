from enum import Enum


class DADATaxonomy(Enum):
    coidb = "coidb"
    coidb_221216 = "coidb=221216"
    gtdb = "gtdb"
    gtdb_R05_RS95 = "gtdb=R05-RS95"
    gtdb_R06_RS202 = "gtdb=R06-RS202"
    gtdb_R07_RS207 = "gtdb=R07-RS207"
    gtdb_R08_RS214 = "gtdb=R08-RS214"
    gtdb_R09_RS220 = "gtdb=R09-RS220"
    midori2_co1 = "midori2-co1"
    midori2_co1_gb250 = "midori2-co1=gb250"
    pr2 = "pr2"
    pr2_4_13_0 = "pr2=4.13.0"
    pr2_4_14_0 = "pr2=4.14.0"
    pr2_5_0_0 = "pr2=5.0.0"
    rdp = "rdp"
    rdp_18 = "rdp=18"
    sbdi_gtdb = "sbdi-gtdb"
    sbdi_gtdb_R09_RS220_1 = "sbdi-gtdb=R09-RS220-1"
    sbdi_gtdb_R08_RS214_1 = "sbdi-gtdb=R08-RS214-1"
    sbdi_gtdb_R07_RS207_1 = "sbdi-gtdb=R07-RS207-1"
    sbdi_gtdb_R06_RS202_3 = "sbdi-gtdb=R06-RS202-3"
    sbdi_gtdb_R06_RS202_1 = "sbdi-gtdb=R06-RS202-1"
    silva = "silva"
    silva_132 = "silva=132"
    silva_138 = "silva=138"
    unite_alleuk = "unite-alleuk"
    unite_alleuk_9_0 = "unite-alleuk=9.0"
    unite_alleuk_8_3 = "unite-alleuk=8.3"
    unite_alleuk_8_2 = "unite-alleuk=8.2"
    unite_fungi = "unite-fungi"
    unite_fungi_9_0 = "unite-fungi=9.0"
    unite_fungi_8_3 = "unite-fungi=8.3"
    unite_fungi_8_2 = "unite-fungi=8.2"
    zehr_nifh = "zehr-nifh"
    zehr_nifh_2_5_0 = "zehr-nifh=2.5.0"


class QiimeRefTaxonomy(Enum):
    silva_138 = "silva=138"
    silva = "silva"
    greengenes85 = "greengenes85"
    greengenes2 = "greengenes2"
    greengenes2_2022_10 = "greengenes2=2022.10"


class Kraken2RefTaxonomy(Enum):
    silva = "silva"
    silva_138 = "silva=138"
    silva_132 = "silva=132"
    rdp = "rdp"
    rdp_18 = "rdp=18"
    greengenes = "greengenes"
    greengenes_13_5 = "greengenes=13.5"
    standard = "standard"
    standard_20230605 = "standard=20230605"


class SintaxRefTaxonomy(Enum):
    coidb = "coidb"
    coidb_221216 = "coidb=221216"
    unite_fungi = "unite-fungi"
    unite_fungi_10_0 = "unite-fungi=10.0"
    unite_fungi_9_0 = "unite-fungi=9.0"
    unite_fungi_8_3 = "unite-fungi=8.3"
    unite_fungi_8_2 = "unite-fungi=8.2"
    unite_alleuk = "unite-alleuk"
    unite_alleuk_10_0 = "unite-alleuk=10.0"
    unite_alleuk_9_0 = "unite-alleuk=9.0"
    unite_alleuk_8_3 = "unite-alleuk=8.3"
    unite_alleuk_8_2 = "unite-alleuk=8.2"


class SidleRefTaxonomy(Enum):
    silva = "silva"
    silva_128 = "silva=128"
    greengenes = "greengenes"
    greengenes_13_8 = "greengenes=13_8"
    greengenes88 = "greengenes88"
