use strict;
use warnings;
use ExtUtils::MakeMaker;

my $INC  = '-I.';
my $LIBS = '-lqdbm';
my $DEFINE = '-DQDBM_STATIC';

WriteMakefile(
    NAME          => 'QDBM_File',
    AUTHOR        => 'Toshiyuki Yamato <toshiyuki.yamato@gmail.com>',
    VERSION_FROM  => 'lib/QDBM_File.pm',
    ABSTRACT_FROM => 'lib/QDBM_File.pm',
    INC           => $INC,
    LIBS          => [$LIBS],
    DEFINE        => $DEFINE,
    XSOPT         => '-noprototypes',
    (
        (ExtUtils::MakeMaker->VERSION() gt '6.30')
            ? ('LICENSE' => 'perl')
            : ()
    ),
    PREREQ_PM => {
        'Test::More' => 0,
    },
);
