#!perl -w

# QDBM_File test script based on DB_File - db_hash.t

use strict;
use Test::More tests => 54;
use Fcntl;
use File::Path;
use File::Spec;

BEGIN {
    use_ok('QDBM_File');
}

my $class = 'QDBM_File';
my $tempdir  = "t/db_hash_temp";
mkpath($tempdir);
my $tempfile = File::Spec->catfile($tempdir, "db_hash_test");

END {
    rmtree($tempdir);
}

my %tie;
my $db = tie %tie, $class, $tempfile, O_RDWR|O_CREAT, 0640;

isa_ok($db, $class);

sub count_ok {
    my $count = shift;
    my $i = 0;
    my ($key, $value);
    while ( ($key, $value) = each %tie  ) { $i++; }
    is($i, $count);
}

count_ok(0);

$tie{'abc'} = 'ABC';

ok( exists $tie{'abc'} );
is( $tie{'abc'}, 'ABC' );
ok( !exists $tie{'def'} );
ok( !defined $tie{'def'} );

is( $db->FETCH('abc', 0, 1), "A" );
is( $db->FETCH('abc', 1, 1), "B" );
is( $db->FETCH('abc', 2, 1), "C" );

$tie{'abc'} = "Null \0 Value";
is( $tie{'abc'}, "Null \0 Value" );

delete $tie{'abc'};
ok( !exists $tie{'abc'} );

$tie{"null\0key"} = "Null Key";
is( $tie{"null\0key"}, "Null Key" );
delete $tie{"null\0key"};
ok( !exists $tie{"null\0key"} );

count_ok(0);

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

{
    my @keys   = keys   %tie;
    my @values = values %tie;
    ok(@keys == 6 && @values == 6);
}

{
    my $i = 0;
    my ($key, $value);
    while ( ($key, $value) = each %tie ) {
        if ($key eq lc $value) { $i++; }
    }
    is($i, 6);
}

$tie{'empty value'} = '';
is( $tie{'empty value'}, '' );

$tie{''} = 'empty key';
is( $tie{''}, 'empty key' );

count_ok(8);

$tie{'cattest'} = "CAT";
$db->STORE('cattest', "TEST", QD_CAT);
is( $tie{'cattest'}, "CATTEST" );

my $stat = eval { $db->STORE("cattest", "KEEP", QD_KEEP); };
ok(!$stat);

ok(!$db->is_fatal_error);
ok($db->get_name =~ /db_hash_test/);
ok($db->get_mtime);
ok( 0 < $db->get_record_size("cattest") );
ok(0 < eval { $db->count_buckets; });
ok(0 < eval { $db->count_used_buckets; });
ok(0 < eval { $db->count_records; });
ok($db->is_writable);
ok(0 < eval { $db->get_size; });
ok($db->iterator_init);
ok(eval { $db->sync; });
ok(eval { $db->optimize; });

my $temp_export = File::Spec->catfile( $tempdir, "db_hash_export_test" );
ok(eval { $db->export_db($temp_export); });

undef $db;
untie %tie;

ok( $class->repair($tempfile) );

$db = tie %tie, $class, $tempfile, O_RDWR|O_CREAT|O_TRUNC, 0640;
count_ok(0);
ok(eval { $db->import_db($temp_export); });

is( $tie{'a'}, "A" );
is( $tie{'b'}, "B" );
is( $tie{'c'}, "C" );
is( $tie{'d'}, "D" );
is( $tie{'e'}, "E" );
is( $tie{'f'}, "F" );
is( $tie{'empty value'}, '' );
is( $tie{''}, 'empty key' );

my ($fetch_key, $store_key, $fetch_value, $store_value) = ("") x 4;

$db->filter_fetch_key(sub { $fetch_key = $_ });
$db->filter_store_key(sub { $store_key = $_ });
$db->filter_fetch_value(sub { $fetch_value = $_});
$db->filter_store_value(sub { $store_value = $_ });

$tie{'filter_key'} = 'filter_value';
is( $store_key, 'filter_key' );
is( $store_value, 'filter_value' );

is( $tie{'filter_key'}, 'filter_value' );
is( $fetch_value, 'filter_value' );

my $next_key = $db->FIRSTKEY;
is($fetch_key, $next_key);

undef $db;
untie %tie;
