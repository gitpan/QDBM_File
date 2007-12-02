#!perl -T

# QDBM_File::Multiple::LOB test script based on DB_File - db_hash.t

use strict;
use Test::More tests => 21;
use Fcntl;
use File::Path;
use File::Spec;

BEGIN {
    use_ok('QDBM_File');
}

my $class = 'QDBM_File::Multiple::LOB';
my $tempdir = "t/db_hash_multi_lob_temp";
mkpath($tempdir);
my $tempfile = File::Spec->catfile($tempdir, "db_hash_multi_lob_test");

my %tie;
my $db = tie %tie, $class, $tempfile, O_RDWR|O_CREAT, 0640;

END {
    rmtree($tempdir);
}

isa_ok($db, $class);

$tie{'abc'} = 'ABC';

ok( exists $tie{'abc'} );
ok( $tie{'abc'} eq 'ABC' );
ok( !exists $tie{'def'} );
ok( !defined $tie{'def'} );

$tie{'abc'} = "Null \0 Value";
is( $tie{'abc'}, "Null \0 Value" );

delete $tie{'abc'};
ok( !exists $tie{'abc'} );

$tie{"null\0key"} = "Null Key";
is( $tie{"null\0key"}, "Null Key" );
delete $tie{"null\0key"};
ok( !exists $tie{"null\0key"} );

$tie{'a'} = "A";
$tie{'b'} = "B";

undef $db;
untie %tie;

$db = tie %tie, $class, $tempfile, O_RDWR, 0640;
ok($db);

is( $tie{'a'}, "A" );
is( $tie{'b'}, "B" );

$tie{'c'} = "C";
$tie{'d'} = "D";
$tie{'e'} = "E";
$tie{'f'} = "F";

is( $db->count_lob_records, 6 );

$tie{'empty value'} = '';
ok( $tie{'empty value'} eq '' );

# LOB not accept empty key
my $stat1 = eval { $tie{''} = 'empty key'; };
ok(!$stat1);

$tie{'cattest'} = "CAT";
$db->STORE('cattest', "TEST", QD_CAT);
is( $tie{'cattest'}, "CATTEST" );

my $stat2 = eval { $db->STORE('cattest', "KEEP", QD_KEEP); };
ok(!$stat2);

ok(0 < eval { $db->get_size; });
ok(eval { $db->sync; });
ok(eval { $db->optimize; });

undef $db;
untie %tie;
