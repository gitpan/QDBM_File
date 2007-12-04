#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"

#include "depot.h"
#include "curia.h"
#include "cabin.h"
#include "villa.h"
#include "vista_xs.h"
#include "odeum.h"

/*
   The DBM_setFilter & DBM_ckFilter macros are only used by
   the *DB*_File modules
   imported from XSUB.h for older XS
*/

#ifndef DBM_setFilter
#define DBM_setFilter(db_type,code)                 \
    STMT_START {                                    \
        if (db_type)                                \
            RETVAL = sv_mortalcopy(db_type) ;       \
        ST(0) = RETVAL ;                            \
        if (db_type && (code == &PL_sv_undef)) {    \
                SvREFCNT_dec(db_type) ;             \
            db_type = NULL ;                        \
        }                                           \
        else if (code) {                            \
            if (db_type)                            \
                sv_setsv(db_type, code) ;           \
            else                                    \
                db_type = newSVsv(code) ;           \
        }                                           \
    } STMT_END
#endif

#ifndef DBM_ckFilter
#define DBM_ckFilter(arg,type,name)                     \
        STMT_START {                                    \
    if (db->type) {                                     \
        if (db->filtering) {                            \
            croak("recursion detected in %s", name) ;   \
        }                                               \
        ENTER ;                                         \
        SAVETMPS ;                                      \
        SAVEINT(db->filtering) ;                        \
        db->filtering = TRUE ;                          \
        SAVESPTR(DEFSV) ;                               \
            if (name[7] == 's')                         \
                arg = newSVsv(arg);                     \
        DEFSV = arg ;                                   \
        SvTEMP_off(arg) ;                               \
        PUSHMARK(SP) ;                                  \
        PUTBACK ;                                       \
        (void) call_sv(db->type, G_DISCARD);            \
        SPAGAIN ;                                       \
        PUTBACK ;                                       \
        FREETMPS ;                                      \
        LEAVE ;                                         \
            if (name[7] == 's'){                        \
                arg = sv_2mortal(arg);                  \
            }                                           \
    } } STMT_END
#endif

typedef struct {
    void* dbp;
    SV* comparer; /* subroutine reference */
    SV* filter_fetch_key;
    SV* filter_store_key;
    SV* filter_fetch_value;
    SV* filter_store_value;
    int filtering;
} QDBM_File_type;

typedef QDBM_File_type* QDBM_File;
typedef SV* datum_key;
typedef SV* datum_value;

#define dpptr(db)  ( (DEPOT*)db->dbp )
#define crptr(db)  ( (CURIA*)db->dbp )
#define vlptr(db)  ( (VILLA*)db->dbp )
#define vstptr(db) ( (VISTA*)db->dbp )

enum {
  QD_OVER,
  QD_KEEP,
  QD_CAT,
  QD_DUP,
  QD_DUPR
};

/* define static data for btree comparer */
#define MY_CXT_KEY "QDBM_File::_guts" XS_VERSION

typedef struct {
    SV* comparer;
} my_cxt_t;

START_MY_CXT

int btree_compare(const char* key_a, int ksize_a, const char* key_b, int ksize_b) {

    int count;
    int retval;

    dSP;
    dMY_CXT;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);

    XPUSHs( sv_2mortal( newSVpvn(key_a, ksize_a) ) );
    XPUSHs( sv_2mortal( newSVpvn(key_b, ksize_b) ) );

    PUTBACK;

    count = call_sv(MY_CXT.comparer, G_SCALAR);

    SPAGAIN;

    if (1 != count) {
        croak("qdbm compare error: subroutine returned %d values, expected 1\n", count);
    }

    retval = POPi;

    PUTBACK;
    FREETMPS;
    LEAVE;

    return retval;
}

MODULE = QDBM_File    PACKAGE = QDBM_File

BOOT:
{
    HV* stash;
    MY_CXT_INIT;
    MY_CXT.comparer = &PL_sv_undef;
    stash = gv_stashpv("QDBM_File", TRUE);
    newCONSTSUB( stash, "QD_OVER", newSViv(QD_OVER) );
    newCONSTSUB( stash, "QD_KEEP", newSViv(QD_KEEP) );
    newCONSTSUB( stash, "QD_CAT",  newSViv(QD_CAT)  );
    newCONSTSUB( stash, "QD_DUP",  newSViv(QD_DUP)  );
    newCONSTSUB( stash, "QD_DUPR", newSViv(QD_DUPR) );
}

INCLUDE: dbm_filter.xsh

QDBM_File
TIEHASH(char* dbtype, char* filename, int flags = O_CREAT|O_RDWR, int mode = 0644, int buckets = -1)
PREINIT:
    DEPOT* dbp;
    int o_flags;
CODE:
    RETVAL = NULL;
    o_flags = ( (flags & O_WRONLY) || (flags & O_RDWR) ) ? DP_OWRITER : DP_OREADER;
    if (flags & O_CREAT) o_flags |= DP_OCREAT;
    if (flags & O_TRUNC) o_flags |= DP_OTRUNC;

    dbp = dpopen(filename, o_flags, buckets);

    if (NULL != dbp) {
        Newxz(RETVAL, 1, QDBM_File_type);
        RETVAL->dbp = (void*)dbp;
    }
    else {
        croak( "qdbm open error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
DESTROY(QDBM_File db)
CODE:
    if (db) {
        if ( dpclose( dpptr(db) ) ) {
            if (db->comparer)           SvREFCNT_dec(db->comparer);
            if (db->filter_fetch_key)   SvREFCNT_dec(db->filter_fetch_key);
            if (db->filter_store_key)   SvREFCNT_dec(db->filter_store_key);
            if (db->filter_fetch_value) SvREFCNT_dec(db->filter_fetch_value);
            if (db->filter_store_value) SvREFCNT_dec(db->filter_store_value);
            Safefree(db);
        }
        else {
            croak( "qdbm close error: %s\n", dperrmsg(dpecode) );
        }
    }

datum_value
FETCH(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    char* value;
CODE:
    ksize = SvCUR(key);
    value = dpget( dpptr(db), SvPVbyte(key, ksize), ksize, 0, -1, &vsize );
    if (value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else if ( !dpfatalerror( dpptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
STORE(QDBM_File db, datum_key key, datum_value value, int flags = QD_OVER)
PREINIT:
    int ksize;
    int vsize;
    int dmode;
CODE:
    ksize = SvCUR(key);
    vsize = SvCUR(value);
    if      (QD_OVER == flags) { dmode = DP_DOVER; }
    else if (QD_KEEP == flags) { dmode = DP_DKEEP; }
    else if (QD_CAT  == flags) { dmode = DP_DCAT ; }
    else { croak("qdbm store error: unknown overlap flags\n"); }
    RETVAL = (bool)dpput(
        dpptr(db),
        SvPVbyte(key, ksize),
        ksize,
        SvPVbyte(value, vsize),
        vsize,
        dmode
    );
    if (!RETVAL && dpfatalerror( dpptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
DELETE(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
CODE:
    ksize = SvCUR(key);
    RETVAL = (bool)dpout( dpptr(db), SvPVbyte(key, ksize), ksize );
    if (!RETVAL && dpfatalerror( dpptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
EXISTS(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
CODE:
    ksize = SvCUR(key);
    vsize = dpvsiz( dpptr(db), SvPVbyte(key, ksize), ksize );
    RETVAL = (bool)(-1 != vsize);
OUTPUT:
    RETVAL

datum_key
FIRSTKEY(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
CODE:
    if ( dpiterinit( dpptr(db) ) ) {
        key = dpiternext( dpptr(db), &ksize );
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else {
        croak( "qdbm iterator init error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

datum_key
NEXTKEY(QDBM_File db, datum_key prev_key)
PREINIT:
    int ksize;
    char* key;
CODE:
    key = dpiternext( dpptr(db), &ksize );
    if (NULL != key) {
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else if ( !dpfatalerror( dpptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm get key error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
CLEAR(QDBM_File db)
CODE:
    croak("qdbm clear error: method not implemented\n");

bool
set_align(QDBM_File db, int align)
CODE:
    RETVAL = (bool)dpsetalign( dpptr(db), align );
    if (!RETVAL) {
        croak( "qdbm set_align error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
set_fbp_size(QDBM_File db, int size)
CODE:
    RETVAL = (bool)dpsetfbpsiz( dpptr(db), size );
    if (!RETVAL) {
        croak( "qdbm set_fbp_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
sync(QDBM_File db)
CODE:
    RETVAL = (bool)dpsync( dpptr(db) );
    if (!RETVAL) {
        croak( "qdbm sync error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
optimize(QDBM_File db, int buckets = -1)
CODE:
    RETVAL = (bool)dpoptimize( dpptr(db), buckets );
    if (!RETVAL) {
        croak( "qdbm optimize error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_record_size(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
CODE:
    ksize = SvCUR(key);
    RETVAL = dpvsiz( dpptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

bool
iterator_init(QDBM_File db)
CODE:
    RETVAL = (bool)dpiterinit( dpptr(db) );
OUTPUT:
    RETVAL

SV*
get_name(QDBM_File db)
PREINIT:
    char* name;
CODE:
    name = dpname( dpptr(db) );
    if (NULL != name) {
        RETVAL = newSVpv(name, 0);
        cbfree(name);
    }
    else {
        croak( "qdbm get_name error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_size(QDBM_File db)
CODE:
    RETVAL = dpfsiz( dpptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_buckets(QDBM_File db)
CODE:
    RETVAL = dpbnum( dpptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_used_buckets(QDBM_File db)
CODE:
    RETVAL = dpbusenum( dpptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_used_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_records(QDBM_File db)
CODE:
    RETVAL = dprnum( dpptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_records error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
is_writable(QDBM_File db)
CODE:
    RETVAL = (bool)dpwritable( dpptr(db) );
OUTPUT:
    RETVAL

bool
is_fatal_error(QDBM_File db)
CODE:
    RETVAL = (bool)dpfatalerror( dpptr(db) );
OUTPUT:
    RETVAL

time_t
get_mtime(QDBM_File db)
CODE:
    RETVAL = dpmtime( dpptr(db) );
OUTPUT:
    RETVAL

bool
repair(SV* package, char* filename)
CODE:
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File") ) {
        croak("qdbm repair error: called via instance method\n");
    }
    else {
        RETVAL = (bool)dprepair(filename);
        if (!RETVAL) {
            croak( "qdbm repair error: %s\n", dperrmsg(dpecode) );
        }
    }
OUTPUT:
    RETVAL

bool
export_db(QDBM_File db, char* filename)
CODE:
    RETVAL = (bool)dpexportdb( dpptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm export error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
import_db(QDBM_File db, char* filename)
CODE:
    RETVAL = (bool)dpimportdb( dpptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm import error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

MODULE = QDBM_File    PACKAGE = QDBM_File::Multiple

INCLUDE: dbm_filter.xsh

QDBM_File
TIEHASH(char* dbtype, char* filename, int flags = O_CREAT|O_RDWR, int mode = 0644, int buckets = -1, int directories = -1)
PREINIT:
    CURIA* dbp;
    int o_flags;
CODE:
    RETVAL = NULL;
    o_flags = ( (flags & O_WRONLY) || (flags & O_RDWR) ) ? CR_OWRITER : CR_OREADER;
    if (flags & O_CREAT) o_flags |= CR_OCREAT;
    if (flags & O_TRUNC) o_flags |= CR_OTRUNC;
    dbp = cropen(filename, o_flags, buckets, directories);
    if (NULL != dbp) {
        Newxz(RETVAL, 1, QDBM_File_type);
        RETVAL->dbp = (void*)dbp;
    }
    else {
        croak( "qdbm open error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
DESTROY(QDBM_File db)
CODE:
    if (db) {
        if ( crclose( crptr(db) ) ) {
            if (db->comparer)           SvREFCNT_dec(db->comparer);
            if (db->filter_fetch_key)   SvREFCNT_dec(db->filter_fetch_key);
            if (db->filter_store_key)   SvREFCNT_dec(db->filter_store_key);
            if (db->filter_fetch_value) SvREFCNT_dec(db->filter_fetch_value);
            if (db->filter_store_value) SvREFCNT_dec(db->filter_store_value);
            Safefree(db);
        }
        else {
            croak( "qdbm close error: %s\n", dperrmsg(dpecode) );
        }
    }

datum_value
FETCH(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    char* value;
CODE:
    ksize = SvCUR(key);
    value = crget( crptr(db), SvPVbyte(key, ksize), ksize, 0, -1, &vsize );
    if (value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else if ( !crfatalerror( crptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
STORE(QDBM_File db, datum_key key, datum_value value, int flags = QD_OVER)
PREINIT:
    int ksize;
    int vsize;
    int dmode;
CODE:
    ksize = SvCUR(key);
    vsize = SvCUR(value);
    if      (QD_OVER == flags) { dmode = CR_DOVER; }
    else if (QD_KEEP == flags) { dmode = CR_DKEEP; }
    else if (QD_CAT  == flags) { dmode = CR_DCAT ; }
    else { croak("qdbm store error: unknown overlap flags\n"); }
    RETVAL = (bool)crput(
        crptr(db),
        SvPVbyte(key, ksize),
        ksize,
        SvPVbyte(value, vsize),
        vsize,
        dmode
    );
    if (!RETVAL && crfatalerror( crptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
DELETE(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
CODE:
    ksize = SvCUR(key);
    RETVAL = (bool)crout( crptr(db), SvPVbyte(key, ksize), ksize );
    if (!RETVAL && crfatalerror( crptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
EXISTS(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
CODE:
    ksize = SvCUR(key);
    vsize = crvsiz( crptr(db), SvPVbyte(key, ksize), ksize );
    RETVAL = (bool)(-1 != vsize);
OUTPUT:
    RETVAL

datum_key
FIRSTKEY(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
CODE:
    if ( criterinit( crptr(db) ) ) {
        key = criternext( crptr(db), &ksize );
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else {
        croak( "qdbm iterator init error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

datum_key
NEXTKEY(QDBM_File db, datum_key prev_key)
PREINIT:
    int ksize;
    char* key;
CODE:
    key = criternext( crptr(db), &ksize );
    if (NULL != key) {
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else if ( !crfatalerror( crptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm get key error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
set_align(QDBM_File db, int align)
CODE:
    RETVAL = (bool)crsetalign( crptr(db), align );
    if (!RETVAL) {
        croak( "qdbm set_align error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
set_fbp_size(QDBM_File db, int size)
CODE:
    RETVAL = (bool)crsetfbpsiz( crptr(db), size);
    if (!RETVAL) {
        croak( "qdbm set_fbp_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
sync(QDBM_File db)
CODE:
    RETVAL = (bool)crsync( crptr(db) );
    if (!RETVAL) {
        croak( "qdbm sync error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
optimize(QDBM_File db, int buckets = -1)
CODE:
    RETVAL = (bool)croptimize( crptr(db), buckets );
    if (!RETVAL) {
        croak( "qdbm optimize error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_record_size(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
CODE:
    ksize = SvCUR(key);
    RETVAL = crvsiz( crptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

bool
iterator_init(QDBM_File db)
CODE:
    RETVAL = (bool)criterinit( crptr(db) );
OUTPUT:
    RETVAL

SV*
get_name(QDBM_File db)
PREINIT:
    char* name;
CODE:
    name = crname( crptr(db) );
    if (NULL != name) {
        RETVAL = newSVpv(name, 0);
        cbfree(name);
    }
    else {
        croak( "qdbm get_name error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_size(QDBM_File db)
CODE:
    RETVAL = crfsiz( crptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_buckets(QDBM_File db)
CODE:
    RETVAL = crbnum( crptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_used_buckets(QDBM_File db)
CODE:
    RETVAL = crbusenum( crptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_used_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_records(QDBM_File db)
CODE:
    RETVAL = crrnum( crptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_records error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
is_writable(QDBM_File db)
CODE:
    RETVAL = (bool)crwritable( crptr(db) );
OUTPUT:
    RETVAL

bool
is_fatal_error(QDBM_File db)
CODE:
    RETVAL = (bool)crfatalerror( crptr(db) );
OUTPUT:
    RETVAL

time_t
get_mtime(QDBM_File db)
CODE:
    RETVAL = crmtime( crptr(db) );
OUTPUT:
    RETVAL

bool
repair(SV* package, char* filename)
CODE:
    if ( SvROK(package) ) {
        croak("qdbm repair error: called via instance method\n");
    }
    else {
        RETVAL = (bool)crrepair(filename);
        if (!RETVAL) {
            croak( "qdbm repair error: %s\n", dperrmsg(dpecode) );
        }
    }
OUTPUT:
    RETVAL

bool
export_db(QDBM_File db, char* filename)
CODE:
    RETVAL = (bool)crexportdb( crptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm export error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
import_db(QDBM_File db, char* filename)
CODE:
    RETVAL = (bool)crimportdb( crptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm import error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

datum_value
fetch_lob(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    char* value;
CODE:
    ksize = SvCUR(key);
    value = crgetlob( crptr(db), SvPVbyte(key, ksize), ksize, 0, -1, &vsize );
    if (value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else if ( !crfatalerror( crptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
store_lob(QDBM_File db, datum_key key, datum_value value, int flags = QD_OVER)
PREINIT:
    int ksize;
    int vsize;
    int dmode;
CODE:
    ksize = SvCUR(key);
    vsize = SvCUR(value);
    if      (QD_OVER == flags) { dmode = CR_DOVER; }
    else if (QD_KEEP == flags) { dmode = CR_DKEEP; }
    else if (QD_CAT  == flags) { dmode = CR_DCAT ; }
    else { croak("qdbm store error: unknown overlap flags\n"); }
    RETVAL = (bool)crputlob(
        crptr(db),
        SvPVbyte(key, ksize),
        ksize,
        SvPVbyte(value, vsize),
        vsize,
        dmode
    );
    if (!RETVAL && crfatalerror( crptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
delete_lob(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
CODE:
    ksize = SvCUR(key);
    RETVAL = (bool)croutlob( crptr(db), SvPVbyte(key, ksize), ksize );
    if (!RETVAL && crfatalerror( crptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
exists_lob(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
CODE:
    ksize = SvCUR(key);
    vsize = crvsizlob( crptr(db), SvPVbyte(key, ksize), ksize );
    RETVAL = (bool)(-1 != vsize);
OUTPUT:
    RETVAL

int
count_lob_records(QDBM_File db)
CODE:
    RETVAL = crrnumlob( crptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_records error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

MODULE = QDBM_File    PACKAGE = QDBM_File::BTree

INCLUDE: dbm_filter.xsh

QDBM_File
TIEHASH(char* dbtype, char* filename, int flags = O_CREAT|O_RDWR, int mode = 0644, SV* comparer = &PL_sv_undef)
PREINIT:
    VILLA* dbp;
    int o_flags;
    VLCFUNC cmpptr;
CODE:
    RETVAL = NULL;
    cmpptr = SvOK(comparer) ? btree_compare : VL_CMPLEX;
    o_flags = ( (flags & O_WRONLY) || (flags & O_RDWR) ) ? VL_OWRITER : VL_OREADER;
    if (flags & O_CREAT) o_flags |= VL_OCREAT;
    if (flags & O_TRUNC) o_flags |= VL_OTRUNC;

    dbp = vlopen(filename, o_flags, cmpptr);

    if (NULL != dbp) {
        Newxz(RETVAL, 1, QDBM_File_type);
        RETVAL->dbp = (void*)dbp;
        RETVAL->comparer = newSVsv(comparer);
    }
    else {
        croak( "qdbm open error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
DESTROY(QDBM_File db)
CODE:
    if (db) {
        if ( vlclose( vlptr(db) ) ) {
            if (db->comparer)           SvREFCNT_dec(db->comparer);
            if (db->filter_fetch_key)   SvREFCNT_dec(db->filter_fetch_key);
            if (db->filter_store_key)   SvREFCNT_dec(db->filter_store_key);
            if (db->filter_fetch_value) SvREFCNT_dec(db->filter_fetch_value);
            if (db->filter_store_value) SvREFCNT_dec(db->filter_store_value);
            Safefree(db);
        }
        else {
            croak( "qdbm close error: %s\n", dperrmsg(dpecode) );
        }
    }

datum_value
FETCH(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    char* value;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    value = vlget( vlptr(db), SvPVbyte(key, ksize), ksize, &vsize );

    if (NULL != value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else if ( !vlfatalerror( vlptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
STORE(QDBM_File db, datum_key key, datum_value value, int flags = QD_OVER)
PREINIT:
    int ksize;
    int vsize;
    int dmode;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    vsize = SvCUR(value);
    if      (QD_OVER == flags) { dmode = VL_DOVER; }
    else if (QD_KEEP == flags) { dmode = VL_DKEEP; }
    else if (QD_CAT  == flags) { dmode = VL_DCAT ; }
    else if (QD_DUP  == flags) { dmode = VL_DDUP ; }
    else if (QD_DUPR == flags) { dmode = VL_DDUPR; }
    else { croak("qdbm store error: unknown overlap flags\n"); }
    RETVAL = (bool)vlput(
        vlptr(db),
        SvPVbyte(key, ksize),
        ksize,
        SvPVbyte(value, vsize),
        vsize,
        dmode
    );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
DELETE(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vlout( vlptr(db), SvPVbyte(key, ksize), ksize );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
EXISTS(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    vsize = vlvsiz( vlptr(db), SvPVbyte(key, ksize), ksize );
    RETVAL = (bool)(-1 != vsize);
OUTPUT:
    RETVAL

datum_key
FIRSTKEY(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    if ( vlcurfirst( vlptr(db) ) ) {
        key = vlcurkey( vlptr(db), &ksize );
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else if ( !vlfatalerror( vlptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm iterator init error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

datum_key
NEXTKEY(QDBM_File db, datum_key prev_key)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    if ( vlcurnext( vlptr(db) ) ) {
        key = vlcurkey( vlptr(db), &ksize );
        if (NULL != key) {
            RETVAL = newSVpvn(key, ksize);
            cbfree(key);
        }
        else if ( !vlfatalerror( vlptr(db) ) ) {
            XSRETURN_UNDEF;
        }
        else {
            croak( "qdbm get key error: %s\n", dperrmsg(dpecode) );
        }
    }
    else if ( !vlfatalerror( vlptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm iterator error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
CLEAR(QDBM_File db)
CODE:
    croak("qdbm clear error: method not implemented\n");

int
get_record_size(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = vlvsiz( vlptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

int
count_match_records(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = vlvnum( vlptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

bool
delete_list(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vloutlist( vlptr(db), SvPVbyte(key, ksize), ksize );
    if ( !RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
fetch_list(QDBM_File db, datum_key key)
PREINIT:
    int i;
    int ksize;
    int vsize;
    const char* value;
    CBLIST* list;
    SV* value_sv;
    dMY_CXT;
PPCODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    list = vlgetlist( vlptr(db), SvPVbyte(key, ksize), ksize );

    if (NULL != list) {
        for (i = 0; i < cblistnum(list); i++) {
            value = cblistval(list, i, &vsize);
            value_sv = newSVpvn(value, vsize);
            DBM_ckFilter(value_sv, filter_fetch_value, "filter_fetch_value");
            XPUSHs( sv_2mortal(value_sv) );
        }
        cblistclose(list);
    }
    else if ( !vlfatalerror( vlptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }

bool
store_list(QDBM_File db, datum_key key, ...)
PREINIT:
    int i;
    int ksize;
    int vsize;
    CBLIST* list;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    list = cblistopen();
    for (i = 2; i < items; i++) {
        DBM_ckFilter( ST(i), filter_store_value, "filter_store_value" );
        vsize = SvCUR( ST(i) );
        cblistpush(list, SvPVbyte( ST(i), vsize ), vsize);
    }
    ksize = SvCUR(key);
    RETVAL = (bool)vlputlist( vlptr(db), SvPVbyte(key, ksize), ksize, list );
    if ( !RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL
CLEANUP:
    cblistclose(list);

bool
iterator_init(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurfirst( vlptr(db) );
OUTPUT:
    RETVAL

bool
move_first(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurfirst( vlptr(db) );
OUTPUT:
    RETVAL

bool
move_last(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurlast( vlptr(db) );
OUTPUT:
    RETVAL

bool
move_next(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurnext( vlptr(db) );
OUTPUT:
    RETVAL

bool
move_prev(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurprev( vlptr(db) );
OUTPUT:
    RETVAL

bool
move_forward(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vlcurjump( vlptr(db), SvPVbyte(key, ksize), ksize, VL_JFORWARD );
OUTPUT:
    RETVAL

bool
move_backword(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vlcurjump( vlptr(db), SvPVbyte(key, ksize), ksize, VL_JBACKWARD );
OUTPUT:
    RETVAL

datum_key
get_current_key(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    key = vlcurkey( vlptr(db), &ksize );
    if (NULL != key) {
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else {
        XSRETURN_UNDEF;
    }
OUTPUT:
    RETVAL

datum_key
get_current_value(QDBM_File db)
PREINIT:
    int vsize;
    char* value;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    value = vlcurval( vlptr(db), &vsize );
    if (NULL != value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else {
        XSRETURN_UNDEF;
    }
OUTPUT:
    RETVAL

bool
store_current(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vlcurput(
        vlptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPCURRENT
    );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
store_after(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vlcurput(
        vlptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPAFTER
    );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
store_before(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vlcurput(
        vlptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPBEFORE
    );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
delete_current(QDBM_File db, datum_key key)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlcurout( vlptr(db) );
    if (!RETVAL && vlfatalerror( vlptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
set_tuning(QDBM_File db, int max_leaf_record, int max_non_leaf_index, int max_cache_leaf, int max_cache_non_leaf)
CODE:
    vlsettuning( vlptr(db), max_leaf_record, max_non_leaf_index, max_cache_leaf, max_cache_non_leaf );

bool
set_fbp_size(QDBM_File db, int size)
CODE:
    RETVAL = (bool)vlsetfbpsiz( vlptr(db), size );
    if (!RETVAL) {
        croak( "qdbm set_fbp_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
sync(QDBM_File db)
CODE:
    RETVAL = (bool)vlsync( vlptr(db) );
    if (!RETVAL) {
        croak( "qdbm sync error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
optimize(QDBM_File db)
CODE:
    RETVAL = (bool)vloptimize( vlptr(db) );
    if (!RETVAL) {
        croak( "qdbm optimize error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

SV*
get_name(QDBM_File db)
PREINIT:
    char* name;
CODE:
    name = vlname( vlptr(db) );
    if (NULL != name) {
        RETVAL = newSVpv(name, 0);
        cbfree(name);
    }
    else {
        croak( "qdbm get_name error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_size(QDBM_File db)
CODE:
    RETVAL = vlfsiz( vlptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_leafs(QDBM_File db)
CODE:
    RETVAL = vllnum( vlptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_leafs error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_non_leafs(QDBM_File db)
CODE:
    RETVAL = vlnnum( vlptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_non_leafs error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_records(QDBM_File db)
CODE:
    RETVAL = vlrnum( vlptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_records error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
is_writable(QDBM_File db)
CODE:
    RETVAL = (bool)vlwritable( vlptr(db) );
OUTPUT:
    RETVAL

bool
is_fatal_error(QDBM_File db)
CODE:
    RETVAL = (bool)vlfatalerror( vlptr(db) );
OUTPUT:
    RETVAL

time_t
get_mtime(QDBM_File db)
CODE:
    RETVAL = vlmtime( vlptr(db) );
OUTPUT:
    RETVAL

bool
begin_transaction(QDBM_File db)
CODE:
    RETVAL = (bool)vltranbegin( vlptr(db) );
OUTPUT:
    RETVAL

bool
commit(QDBM_File db)
CODE:
    RETVAL = (bool)vltrancommit( vlptr(db) );
OUTPUT:
    RETVAL

bool
rollback(QDBM_File db)
CODE:
    RETVAL = (bool)vltranabort( vlptr(db) );
OUTPUT:
    RETVAL

bool
repair(SV* package, char* filename, SV* comparer = &PL_sv_undef)
PREINIT:
    VLCFUNC cmpptr;
    dMY_CXT;
CODE:
    cmpptr = SvOK(comparer) ? btree_compare : VL_CMPLEX;
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = comparer;
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File::BTree") ) {
        croak("qdbm repair error: called via instance method\n");
    }
    else {
        RETVAL = (bool)vlrepair(filename, cmpptr);
        if (!RETVAL) {
            croak( "qdbm repair error: %s\n", dperrmsg(dpecode) );
        }
    }
OUTPUT:
    RETVAL

bool
export_db(QDBM_File db, char* filename)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlexportdb( vlptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm export error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
import_db(QDBM_File db, char* filename)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vlimportdb( vlptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm import error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

MODULE = QDBM_File    PACKAGE = QDBM_File::BTree::Multiple

INCLUDE: dbm_filter.xsh

QDBM_File
TIEHASH(char* dbtype, char* filename, int flags = O_CREAT|O_RDWR, int mode = 0644, SV* comparer = &PL_sv_undef)
PREINIT:
    VISTA* dbp;
    int o_flags;
    VSTCFUNC cmpptr;
CODE:
    RETVAL = NULL;
    cmpptr = SvOK(comparer) ? btree_compare : VST_CMPLEX;
    o_flags = ( (flags & O_WRONLY) || (flags & O_RDWR) ) ? VST_OWRITER : VST_OREADER;
    if (flags & O_CREAT) o_flags |= VST_OCREAT;
    if (flags & O_TRUNC) o_flags |= VST_OTRUNC;

    dbp = vstopen(filename, o_flags, cmpptr);

    if (NULL != dbp) {
        Newxz(RETVAL, 1, QDBM_File_type);
        RETVAL->dbp = (void*)dbp;
        RETVAL->comparer = newSVsv(comparer);
    }
    else {
        croak( "qdbm open error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
DESTROY(QDBM_File db)
CODE:
    if (db) {
        if ( vstclose( vstptr(db) ) ) {
            if (db->comparer)           SvREFCNT_dec(db->comparer);
            if (db->filter_fetch_key)   SvREFCNT_dec(db->filter_fetch_key);
            if (db->filter_store_key)   SvREFCNT_dec(db->filter_store_key);
            if (db->filter_fetch_value) SvREFCNT_dec(db->filter_fetch_value);
            if (db->filter_store_value) SvREFCNT_dec(db->filter_store_value);
            Safefree(db);
        }
        else {
            croak( "qdbm close error: %s\n", dperrmsg(dpecode) );
        }
    }

datum_value
FETCH(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    char* value;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    value = vstget( vstptr(db), SvPVbyte(key, ksize), ksize, &vsize );

    if (NULL != value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else if ( !vstfatalerror( vstptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
STORE(QDBM_File db, datum_key key, datum_value value, int flags = QD_OVER)
PREINIT:
    int ksize;
    int vsize;
    int dmode;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    vsize = SvCUR(value);
    if      (QD_OVER == flags) { dmode = VST_DOVER; }
    else if (QD_KEEP == flags) { dmode = VST_DKEEP; }
    else if (QD_CAT  == flags) { dmode = VST_DCAT ; }
    else if (QD_DUP  == flags) { dmode = VST_DDUP ; }
    else if (QD_DUPR == flags) { dmode = VST_DDUPR; }
    else { croak("qdbm store error: unknown overlap flags\n"); }
    RETVAL = (bool)vstput(
        vstptr(db),
        SvPVbyte(key, ksize),
        ksize,
        SvPVbyte(value, vsize),
        vsize,
        dmode
    );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
DELETE(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vstout( vstptr(db), SvPVbyte(key, ksize), ksize );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
EXISTS(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    vsize = vstvsiz( vstptr(db), SvPVbyte(key, ksize), ksize );
    RETVAL = (bool)(-1 != vsize);
OUTPUT:
    RETVAL

datum_key
FIRSTKEY(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    if ( vstcurfirst( vstptr(db) ) ) {
        key = vstcurkey( vstptr(db), &ksize );
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else if ( !vstfatalerror( vstptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm iterator init error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

datum_key
NEXTKEY(QDBM_File db, datum_key prev_key)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    if ( vstcurnext( vstptr(db) ) ) {
        key = vstcurkey( vstptr(db), &ksize );
        if (NULL != key) {
            RETVAL = newSVpvn(key, ksize);
            cbfree(key);
        }
        else if ( !vstfatalerror( vstptr(db) ) ) {
            XSRETURN_UNDEF;
        }
        else {
            croak( "qdbm get key error: %s\n", dperrmsg(dpecode) );
        }
    }
    else if ( !vstfatalerror( vstptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm iterator error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
CLEAR(QDBM_File db)
CODE:
    croak("qdbm clear error: method not implemented\n");

int
get_record_size(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = vstvsiz( vstptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

int
count_match_records(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = vstvnum( vstptr(db), SvPVbyte(key, ksize), ksize );
OUTPUT:
    RETVAL

bool
delete_list(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vstoutlist( vstptr(db), SvPVbyte(key, ksize), ksize );
    if ( !RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
fetch_list(QDBM_File db, datum_key key)
PREINIT:
    int i;
    int ksize;
    int vsize;
    const char* value;
    CBLIST* list;
    SV* value_sv;
    dMY_CXT;
PPCODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    list = vstgetlist( vstptr(db), SvPVbyte(key, ksize), ksize );

    if (NULL != list) {
        for (i = 0; i < cblistnum(list); i++) {
            value = cblistval(list, i, &vsize);
            value_sv = newSVpvn(value, vsize);
            DBM_ckFilter(value_sv, filter_fetch_value, "filter_fetch_value");
            XPUSHs( sv_2mortal(value_sv) );
        }
        cblistclose(list);
    }
    else if ( !vstfatalerror( vstptr(db) ) ) {
        XSRETURN_UNDEF;
    }
    else {
        croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
    }

bool
store_list(QDBM_File db, datum_key key, ...)
PREINIT:
    int i;
    int ksize;
    int vsize;
    CBLIST* list;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    list = cblistopen();
    for (i = 2; i < items; i++) {
        DBM_ckFilter( ST(i), filter_store_value, "filter_store_value" );
        vsize = SvCUR( ST(i) );
        cblistpush(list, SvPVbyte( ST(i), vsize ), vsize);
    }
    ksize = SvCUR(key);
    RETVAL = (bool)vstputlist( vstptr(db), SvPVbyte(key, ksize), ksize, list );
    if ( !RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL
CLEANUP:
    cblistclose(list);

bool
iterator_init(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurfirst( vstptr(db) );
OUTPUT:
    RETVAL

bool
move_first(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurfirst( vstptr(db) );
OUTPUT:
    RETVAL

bool
move_last(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurlast( vstptr(db) );
OUTPUT:
    RETVAL

bool
move_next(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurnext( vstptr(db) );
OUTPUT:
    RETVAL

bool
move_prev(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurprev( vstptr(db) );
OUTPUT:
    RETVAL

bool
move_forward(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vstcurjump( vstptr(db), SvPVbyte(key, ksize), ksize, VST_JFORWARD );
OUTPUT:
    RETVAL

bool
move_backword(QDBM_File db, datum_key key)
PREINIT:
    int ksize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    ksize = SvCUR(key);
    RETVAL = (bool)vstcurjump( vstptr(db), SvPVbyte(key, ksize), ksize, VST_JBACKWARD );
OUTPUT:
    RETVAL

datum_key
get_current_key(QDBM_File db)
PREINIT:
    int ksize;
    char* key;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    key = vstcurkey( vstptr(db), &ksize );
    if (NULL != key) {
        RETVAL = newSVpvn(key, ksize);
        cbfree(key);
    }
    else {
        XSRETURN_UNDEF;
    }
OUTPUT:
    RETVAL

datum_key
get_current_value(QDBM_File db)
PREINIT:
    int vsize;
    char* value;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    value = vstcurval( vstptr(db), &vsize );
    if (NULL != value) {
        RETVAL = newSVpvn(value, vsize);
        cbfree(value);
    }
    else {
        XSRETURN_UNDEF;
    }
OUTPUT:
    RETVAL

bool
store_current(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vstcurput(
        vstptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPCURRENT
    );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
store_after(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vstcurput(
        vstptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPAFTER
    );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
store_before(QDBM_File db, datum_value value)
PREINIT:
    int vsize;
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    vsize = SvCUR(value);
    RETVAL = (bool)vstcurput(
        vstptr(db),
        SvPVbyte(value, vsize),
        vsize,
        VL_CPBEFORE
    );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
delete_current(QDBM_File db)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstcurout( vstptr(db) );
    if (!RETVAL && vstfatalerror( vstptr(db) ) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
set_tuning(QDBM_File db, int max_leaf_record, int max_non_leaf_index, int max_cache_leaf, int max_cache_non_leaf)
CODE:
    vstsettuning( vstptr(db), max_leaf_record, max_non_leaf_index, max_cache_leaf, max_cache_non_leaf );

bool
set_fbp_size(QDBM_File db, int size)
CODE:
    RETVAL = (bool)vstsetfbpsiz( vstptr(db), size );
    if (!RETVAL) {
        croak( "qdbm set_fbp_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
sync(QDBM_File db)
CODE:
    RETVAL = (bool)vstsync( vstptr(db) );
    if (!RETVAL) {
        croak( "qdbm sync error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
optimize(QDBM_File db)
CODE:
    RETVAL = (bool)vstoptimize( vstptr(db) );
    if (!RETVAL) {
        croak( "qdbm optimize error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

SV*
get_name(QDBM_File db)
PREINIT:
    char* name;
CODE:
    name = vstname( vstptr(db) );
    if (NULL != name) {
        RETVAL = newSVpv(name, 0);
        cbfree(name);
    }
    else {
        croak( "qdbm get_name error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
get_size(QDBM_File db)
CODE:
    RETVAL = vstfsiz( vstptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_leafs(QDBM_File db)
CODE:
    RETVAL = vstlnum( vstptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_leafs error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_non_leafs(QDBM_File db)
CODE:
    RETVAL = vstnnum( vstptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_non_leafs error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_records(QDBM_File db)
CODE:
    RETVAL = vstrnum( vstptr(db) );
    if (-1 == RETVAL) {
        croak( "qdbm get_records error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
is_writable(QDBM_File db)
CODE:
    RETVAL = (bool)vstwritable( vstptr(db) );
OUTPUT:
    RETVAL

bool
is_fatal_error(QDBM_File db)
CODE:
    RETVAL = (bool)vstfatalerror( vstptr(db) );
OUTPUT:
    RETVAL

time_t
get_mtime(QDBM_File db)
CODE:
    RETVAL = vstmtime( vstptr(db) );
OUTPUT:
    RETVAL

bool
begin_transaction(QDBM_File db)
CODE:
    RETVAL = (bool)vsttranbegin( vstptr(db) );
OUTPUT:
    RETVAL

bool
commit(QDBM_File db)
CODE:
    RETVAL = (bool)vsttrancommit( vstptr(db) );
OUTPUT:
    RETVAL

bool
rollback(QDBM_File db)
CODE:
    RETVAL = (bool)vsttranabort( vstptr(db) );
OUTPUT:
    RETVAL

bool
repair(SV* package, char* filename, SV* comparer = &PL_sv_undef)
PREINIT:
    VSTCFUNC cmpptr;
    dMY_CXT;
CODE:
    cmpptr = SvOK(comparer) ? btree_compare : VST_CMPLEX;
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = comparer;
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File::BTree::Multiple") ) {
        croak("qdbm repair error: called via instance method\n");
    }
    else {
        RETVAL = (bool)vstrepair(filename, cmpptr);
        if (!RETVAL) {
            croak( "qdbm repair error: %s\n", dperrmsg(dpecode) );
        }
    }
OUTPUT:
    RETVAL

bool
export_db(QDBM_File db, char* filename)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstexportdb( vstptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm export error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
import_db(QDBM_File db, char* filename)
PREINIT:
    dMY_CXT;
CODE:
    SAVESPTR(MY_CXT.comparer);
    MY_CXT.comparer = db->comparer;
    RETVAL = (bool)vstimportdb( vstptr(db), filename );
    if (!RETVAL) {
        croak( "qdbm import error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

MODULE = QDBM_File    PACKAGE = QDBM_File::InvertedIndex

ODEUM*
new(char* dbtype, char* filename, int flags = O_CREAT|O_RDWR)
PREINIT:
    int o_flags;
CODE:
    o_flags = ( (flags & O_WRONLY) || (flags & O_RDWR) ) ? OD_OWRITER : OD_OREADER;
    if (flags & O_CREAT) o_flags |= OD_OCREAT;
    if (flags & O_TRUNC) o_flags |= OD_OTRUNC;

    RETVAL = odopen(filename, o_flags);
    if (NULL == RETVAL) {
        croak( "qdbm open error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

void
DESTROY(ODEUM* db)
CODE:
    if (db) {
        if ( !odclose(db) ) {
            croak( "qdbm close error: %s\n", dperrmsg(dpecode) );
        }
    }

bool
store_document(ODEUM* db, ODDOC* doc, int max_words = -1, bool over = (bool)TRUE)
CODE:
    RETVAL = (bool)odput(db, doc, max_words, over);
    if (!RETVAL) {
        croak( "qdbm store error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
delete_document_by_uri(ODEUM* db, const char* uri)
CODE:
    RETVAL = (bool)odout(db, uri);
    if ( !RETVAL && odfatalerror(db) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
delete_document_by_id(ODEUM* db, int id)
CODE:
    RETVAL = (bool)odoutbyid(db, id);
    if ( !RETVAL && odfatalerror(db) ) {
        croak( "qdbm delete error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

ODDOC*
get_document_by_uri(ODEUM* db, const char* uri)
CODE:
    RETVAL = odget(db, uri);
    if (NULL == RETVAL) {
        if ( odfatalerror(db) ) {
            croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
        }
        else {
            XSRETURN_UNDEF;
        }
    }
OUTPUT:
    RETVAL

ODDOC*
get_document_by_id(ODEUM* db, int id)
CODE:
    RETVAL = odgetbyid(db, id);
    if (NULL == RETVAL) {
        if ( odfatalerror(db) ) {
            croak( "qdbm fetch error: %s\n", dperrmsg(dpecode) );
        }
        else {
            XSRETURN_UNDEF;
        }
    }
OUTPUT:
    RETVAL

int
get_document_id(ODEUM* db, const char* uri)
CODE:
    RETVAL = odgetidbyuri(db, uri);
    if (-1 == RETVAL) {
        if ( odfatalerror(db) ) {
            croak( "qdbm get_id error: %s\n", dperrmsg(dpecode) );
        }
        else {
            XSRETURN_UNDEF;
        }
    }
OUTPUT:
    RETVAL

bool
exists_document_by_uri(ODEUM* db, const char* uri)
CODE:
    RETVAL = (bool)( -1 != odgetidbyuri(db, uri) );
OUTPUT:
    RETVAL

bool
exists_document_by_id(ODEUM* db, int id)
CODE:
    RETVAL = (bool)odcheck(db, id);
OUTPUT:
    RETVAL

void
search_document(ODEUM* db, const char* word, int max = -1)
PREINIT:
    int i;
    int length;
    ODPAIR* pair;
PPCODE:
    pair = odsearch(db, word, max, &length);
    if (NULL != pair) {
        for (i = 0; i < length; i++) {
            mXPUSHi(pair[i].id);
        }
        cbfree(pair);
    }
    else {
        XSRETURN_EMPTY;
    }

int
search_document_count(ODEUM* db, const char* word)
CODE:
    RETVAL = odsearchdnum(db, word);
    if ( -1 == RETVAL && odfatalerror(db) ) {
        croak( "qdbm search_document_count error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
iterator_init(ODEUM* db)
CODE:
    RETVAL = (bool)oditerinit(db);
    if (!RETVAL) {
        croak( "qdbm iterator_init error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

ODDOC*
get_next_document(ODEUM* db)
CODE:
    RETVAL = oditernext(db);
    if (NULL == RETVAL) {
        if ( odfatalerror(db) ) {
            croak( "qdbm get_next error: %s\n", dperrmsg(dpecode) );
        }
        else {
            XSRETURN_UNDEF;
        }
    }
OUTPUT:
    RETVAL

bool
sync(ODEUM* db)
CODE:
    RETVAL = (bool)odsync(db);
    if (!RETVAL) {
        croak( "qdbm sync error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
optimize(ODEUM* db)
CODE:
    RETVAL = (bool)odoptimize(db);
    if (!RETVAL) {
        croak( "qdbm optimize error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

SV*
get_name(ODEUM* db)
PREINIT:
    char* name;
CODE:
    name = odname(db);
    if (NULL != name) {
        RETVAL = newSVpv(name, 0);
        cbfree(name);
    }
    else {
        croak( "qdbm get_name error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

double
get_size(ODEUM* db)
CODE:
    RETVAL = odfsiz(db);
    if (-1 == RETVAL) {
        croak( "qdbm get_size error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_buckets(ODEUM* db)
CODE:
    RETVAL = odbnum(db);
    if (-1 == RETVAL) {
        croak( "qdbm get_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_used_buckets(ODEUM* db)
CODE:
    RETVAL = odbusenum(db);
    if (-1 == RETVAL) {
        croak( "qdbm get_used_buckets error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_documents(ODEUM* db)
CODE:
    RETVAL = oddnum(db);
    if (-1 == RETVAL) {
        croak( "qdbm get_documents error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

int
count_words(ODEUM* db)
CODE:
    RETVAL = odwnum(db);
    if (-1 == RETVAL) {
        croak( "qdbm get_words error: %s\n", dperrmsg(dpecode) );
    }
OUTPUT:
    RETVAL

bool
is_writable(ODEUM* db)
CODE:
    RETVAL = (bool)odwritable(db);
OUTPUT:
    RETVAL

bool
is_fatal_error(ODEUM* db)
CODE:
    RETVAL = (bool)odfatalerror(db);
OUTPUT:
    RETVAL

time_t
get_mtime(ODEUM* db)
CODE:
    RETVAL = odmtime(db);
OUTPUT:
    RETVAL

bool
merge(SV* package, const char* name, ...)
PREINIT:
    int i;
    int elemsize;
    CBLIST* elemnames;
CODE:
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File::InvertedIndex") ) {
        croak("qdbm merge error: called via instance method\n");
    }
    else {
        elemnames = cblistopen();
        for (i = 2; i < items; i++) {
            elemsize = SvCUR( ST(i) );
            cblistpush(elemnames, SvPVbyte( ST(i), elemsize ), elemsize);
        }
        RETVAL = (bool)odmerge(name, elemnames);
        if (!RETVAL) {
            croak( "qdbm merge error: %s\n", dperrmsg(dpecode) );
        }
    }
OUTPUT:
    RETVAL

void
_get_scores(ODEUM* db, ODDOC* doc, int max)
PREINIT:
    const char* key;
    const char* value;
    int ksize;
    int vsize;
    CBMAP* scores;
PPCODE:
    scores = oddocscores(doc, max, db);
    if ( 0 == cbmaprnum(scores) ) {
        cbmapclose(scores);
        XSRETURN_EMPTY;
    }
    else {
        cbmapiterinit(scores);
        while ( NULL != ( key = cbmapiternext(scores, &ksize) ) ) {
            value = cbmapiterval(key, &vsize);
            XPUSHs( sv_2mortal( newSVpvn(key, ksize) ) );
            XPUSHs( sv_2mortal( newSVpvn(value, vsize) ) );
        }
        cbmapclose(scores);
    }

void
set_tuning(SV* package, int index_buckets, int inverted_index_division_num, int dirty_buffer_buckets, int dirty_buffer_size)
CODE:
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File::InvertedIndex") ) {
        croak("qdbm break_text error: called via instance method\n");
    }
    else {
        odsettuning(
            index_buckets,
            inverted_index_division_num,
            dirty_buffer_buckets,
            dirty_buffer_size
        );
    }

void set_char_class(ODEUM* db, const char* space, const char* delimiter, const char* glue)
CODE:
    odsetcharclass(db, space, delimiter, glue);

void
analyze_text(SV* self, const char* text)
PREINIT:
    ODEUM* db;
    int i;
    const char* value;
    int vsize;
    CBLIST* appearance_words;
PPCODE:
    if ( sv_isobject(self) && sv_derived_from(self, "QDBM_File::InvertedIndex") ) {
        db = (ODEUM*)SvIV( (SV*)SvRV(self) );
        appearance_words = cblistopen();
        odanalyzetext(db, text, appearance_words, NULL);
    }
    else {
        appearance_words = odbreaktext(text);
    }
    if ( 0 == cblistnum(appearance_words) ) {
        cblistclose(appearance_words);
        XSRETURN_EMPTY;
    }
    else {
        for (i = 0; i < cblistnum(appearance_words); i++) {
            value = cblistval(appearance_words, i, &vsize);
            XPUSHs( sv_2mortal( newSVpvn(value, vsize) ) );
        }
        cblistclose(appearance_words);
    }

char*
normalize_word(SV* package, const char* asis)
PREINIT:
    char* normalized_word;
CODE:
    if ( sv_isobject(package) && sv_derived_from(package, "QDBM_File::InvertedIndex") ) {
        croak("qdbm normalize_word error: called via instance method\n");
    }
    else {
        normalized_word = odnormalizeword(asis);
        RETVAL = normalized_word;
    }
OUTPUT:
    RETVAL
CLEANUP:
    cbfree(normalized_word);

void
query(ODEUM *db, const char* query)
PREINIT:
    int i;
    int length;
    int vsize;
    const char* value;
    ODPAIR* pair;
    SV* errsv;
    CBLIST* errors;
PPCODE:
    errors = cblistopen();
    pair = odquery(db, query, &length, errors);
    if (NULL == pair) {
        errsv = newSVpv("qdbm query error:\n", 0);
        SAVEMORTALIZESV(errsv);
        for (i = 0; i < cblistnum(errors); i++) {
            value = cblistval(errors, i, &vsize);
            sv_catpv(errsv, value);
            sv_catpv(errsv, "\n");
        }
        cblistclose(errors);
        croak( SvPV_nolen(errsv) );
    }
    else {
        for (i = 0; i < length; i++) {
            mXPUSHi(pair[i].id);
        }
        cblistclose(errors);
        cbfree(pair);
    }

MODULE = QDBM_File    PACKAGE = QDBM_File::InvertedIndex::Document

ODDOC*
new(char* package, char* uri)
CODE:
    RETVAL = oddocopen(uri);
OUTPUT:
    RETVAL

void
set_attribute(ODDOC* doc, const char* name, const char* value)
CODE:
    oddocaddattr(doc, name, value);

const char*
get_attribute(ODDOC* doc, const char* name)
CODE:
    RETVAL = oddocgetattr(doc, name);
    if (NULL == RETVAL) {
        XSRETURN_UNDEF;
    }
OUTPUT:
    RETVAL

void
add_word(ODDOC* doc, const char* normal, const char* asis)
CODE:
    oddocaddword(doc, normal, asis);

int
get_id(ODDOC* doc)
CODE:
    RETVAL = oddocid(doc);
OUTPUT:
    RETVAL

const char*
get_uri(ODDOC* doc)
CODE:
    RETVAL = oddocuri(doc);
OUTPUT:
    RETVAL

void
get_normalized_words(ODDOC* doc)
PREINIT:
    int i;
    const char* value;
    int vsize;
    const CBLIST* words;
PPCODE:
    words = oddocnwords(doc);
    if ( 0 < cblistnum(words) ) {
        for (i = 0; i < cblistnum(words); i++) {
            value = cblistval(words, i, &vsize);
            XPUSHs( sv_2mortal( newSVpvn(value, vsize) ) );
        }
    }
    else {
        XSRETURN_EMPTY;
    }

void
get_appearance_words(ODDOC* doc)
PREINIT:
    int i;
    const char* value;
    int vsize;
    const CBLIST* words;
PPCODE:
    words = oddocawords(doc);
    if ( 0 < cblistnum(words) ) {
        for (i = 0; i < cblistnum(words); i++) {
            value = cblistval(words, i, &vsize);
            XPUSHs( sv_2mortal( newSVpvn(value, vsize) ) );
        }
    }
    else {
        XSRETURN_EMPTY;
    }

void
_get_scores(ODDOC* doc, int max, ODEUM* db = NULL)
PREINIT:
    const char* key;
    const char* value;
    int ksize;
    int vsize;
    CBMAP* scores;
PPCODE:
    scores = oddocscores(doc, max, db);
    if ( 0 == cbmaprnum(scores) ) {
        cbmapclose(scores);
        XSRETURN_EMPTY;
    }
    else {
        cbmapiterinit(scores);
        while ( NULL != ( key = cbmapiternext(scores, &ksize) ) ) {
            value = cbmapiterval(key, &vsize);
            XPUSHs( sv_2mortal( newSVpvn(key, ksize) ) );
            XPUSHs( sv_2mortal( newSVpvn(value, vsize) ) );
        }
        cbmapclose(scores);
    }

void
DESTROY(ODDOC* doc)
CODE:
    if (doc) {
        oddocclose(doc);
    }
