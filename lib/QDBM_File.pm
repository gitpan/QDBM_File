package QDBM_File;

use strict;

BEGIN {
    our $VERSION = '0.04';
    require XSLoader;
    XSLoader::load(__PACKAGE__, $VERSION);
}

require Exporter;

our @ISA    = qw(Exporter);
our @EXPORT = qw(QD_OVER QD_KEEP QD_CAT QD_DUP QD_DUPR);

package QDBM_File::Multiple::LOB;

our @ISA = qw(QDBM_File::Multiple);

*FETCH  = \&QDBM_File::Multiple::fetch_lob;
*STORE  = \&QDBM_File::Multiple::store_lob;
*DELETE = \&QDBM_File::Multiple::delete_lob;
*EXISTS = \&QDBM_File::Multiple::exists_lob;

package QDBM_File::InvertedIndex;

sub get_scores {
    my $self = shift;
    return wantarray ? $self->_get_scores(@_) : { $self->get_scores(@_) };
}

sub create_document {
    shift;
    return QDBM_File::InvertedIndex::Document->new(@_);
}

package QDBM_File::InvertedIndex::Document;

sub get_scores {
    my $self = shift;
    return wantarray ? $self->_get_scores(@_) : { $self->get_scores(@_) };
}

1;

__END__

=head1 NAME

QDBM_File - Tied access to Quick Database Manager

=head1 SYNOPSIS

    use QDBM_File;

    # hash db

    [$db =] tie %hash, 'QDBM_File', $filename, [$flags, $mode, $buckets];
    [$db =] tie %hash, 'QDBM_File::Multiple', $filename, [$flags, $mode, $buckets, $directories];
    [$db =] tie %hash, 'QDBM_File::Multiple::LOB', $filename, [$flags, $mode, $buckets, $directories];

    # b+tree
    # $compare_sub example: sub { $_[0] cmp $_[1] }

    [$db =] tie %hash, 'QDBM_File::BTree', $filename, [$flags, $mode, $compare_sub];
    [$db =] tie %hash, 'QDBM_File::BTree::Multiple', $filename, [$flags, $mode, $compare_sub];

    # inverted index

    $db = QDBM_File::InvertedIndex->new($filename, [$flags]);

    # hash db, btree, inverted index common api

    $int  = $db->get_size();
    $name = $db->get_name();
    $int  = $db->get_mtime();
    $bool = $db->sync();
    $bool = $db->optimize([$buckets]);
    $bool = $db->iterator_init();
    $bool = $db->is_writable();
    $bool = $db->is_fatal_error();

    # hash db, btree common api
    $bool = $db->STORE($key, $value, [$overlap_flags]);
    $int  = $db->get_record_size($key);
    $int  = $db->count_records();
    $bool = $class->repair($filename);
    $bool = $db->export_db($filename);
    $bool = $db->import_db($filename);

    # hash db only
    $bool = $db->set_align($align);
    $bool = $db->set_fbp_size($size);
    $int  = $db->count_buckets();
    $int  = $db->count_used_buckets();

    # Large Object: QDBM_File::Multiple only
    $bool  = $db->store_lob($key, $value, [$overlap_flags]);
    $value = $db->fetch_lob($key);
    $bool  = $db->delete_lob($key);
    $bool  = $db->exists_lob($key);
    $int   = $db->count_lob_records();

    # btree only
    $bool   = $db->store_list($key, @values);
    @values = $db->fetch_list($key);
    $bool   = $db->delete_list($key);

    $int    = $db->count_match_records($key);
    $int    = $db->count_leafs();
    $int    = $db->count_non_leafs();

    $bool   = $db->move_first();
    $bool   = $db->move_last();
    $bool   = $db->move_next();
    $bool   = $db->move_prev();
    $bool   = $db->move_forward($key);
    $bool   = $db->move_backword($key);

    $key    = $db->get_current_key();
    $value  = $db->get_current_value();
    $bool   = $db->store_current($value);
    $bool   = $db->store_after($value);
    $bool   = $db->store_before($value);
    $bool   = $db->delete_current($value);

    $bool   = $db->begin_transaction();
    $bool   = $db->commit();
    $bool   = $db->rollback();

    $db->set_tuning(
        $max_leaf_record,
        $max_non_leaf_index,
        $max_cache_leaf,
        $max_cache_non_leaf
    );

    # DBM_Filter
    $old_filter = $db->filter_store_key  ( sub { ... } );
    $old_filter = $db->filter_store_value( sub { ... } );
    $old_filter = $db->filter_fetch_key  ( sub { ... } );
    $old_filter = $db->filter_fetch_value( sub { ... } );

    untie %hash;

    # inverted index api
    $doc  = $class->create_document($uri);
    $bool = $db->store_document($doc, $max_words, $is_overwrite);

    $doc  = $db->get_document_by_uri($uri);
    $doc  = $db->get_document_by_id($id);
    $id   = $db->get_document_id($uri);
    $bool = $db->delete_document_by_uri($uri);
    $bool = $db->delete_document_by_id($id);
    $bool = $db->exists_document_by_uri($uri);
    $bool = $db->exists_document_by_id($id);

    $doc   = $db->get_next_document();
    @id    = $db->search_document($word);
    $int   = $db->search_document_count($word);
    $bool  = $class->merge($filename, @filenames);
    %score = $db->get_scores($doc, $max);

    $db->set_tuning(
        $index_buckets,
        $inverted_index_division_num,
        $dirty_buffer_buckets,
        $dirty_buffer_size
    );

    $db->set_char_class($space, $delimiter, $glue);
    @appearance_words = $db->analyze_text($text);
    @appearance_words = $class->analyze_text($text);
    $normalized_word  = $class->normalize_word($word);
    @id = $db->query($query);

    # document api
    $doc = QDBM_File::InvertedIndex::Document->new($uri);
    $doc->set_attribute($name, $value);
    $value = $doc->get_attribute($name);
    $doc->add_word($normalized_word, $appearance_word);
    $uri = $doc->get_uri();
    $id  = $doc->get_id();
    @normalized_words = $doc->get_normalized_words();
    @appearance_words = $doc->get_appearance_words();
    %score = $doc->get_scores($max, [$db]);

=head1 DESCRIPTION

QDBM_File is a module which allows Perl programs to make use of the
facilities provided by the qdbm library. If you use this module, you
should read QDBM manual pages.

Quick Database Manager is a high performance dbm library maintained by
Mikio Hirabayashi. QDBM_File provides various API, Depot, Curia, Villa,
Vista and Odeum. Documents are available at L<http://qdbm.sourceforge.net/>

=head1 EXPORT

QDBM_File exports these overlap flags:

=over 5

=item QD_OVER

Means the specified value overwrites the existing one.

=item QD_KEEP

Means the existing value is kept.

=item QD_CAT

Means the specified value is concatenated at the end of the
existing value.

=item QD_DUP

Means duplication of keys is allowed and the specified value
is added as the last one. It is BTree interface only.

=item QD_DUPR

Means duplication of keys is allowed and the specified value
is added as the first one. It is BTree interface only.

=back

=head1 METHODS

=over 2

=item TIEHASH

TIEHASH interface is similar to other xDBM_File.

    use Fcntl;
    use QDBM_File;

    my %hash;
    my $filename = "filename";
    my $db = tie %hash, "QDBM_File", $filename, O_RDWR|O_CREAT, 0644;
    $hash{"key"} = "value";

Hash db has optional argument $buckets, specifies bucket number of db,
and Q::Multiple has $directories, specifies division number of directory.

Q::BTree has $compare_sub, used for key comparison. It must return -1 or 0 or 1.
If $compare_sub is omitted, dictionary order is used.

    sub { $_[0] cmp $_[1] } # ordered by dictionary
    sub { $_[0] <=> $_[1] } # ordered by number

=item STORE

If using STORE as method, $overlap_flags can be used. If omitted,
QD_OVER is used.

    $db->STORE("key", "balue", QD_CAT);

=back

=head1 AUTHOR

Toshiyuki Yamato, C<< <toshiyuki.yamato at gmail.com> >>

=head1 BUGS AND WARNINGS

Currently umask flags is ignored implicitly, 0644 is always used.
It is used for other xDBM_File compatibility.

QDBM_File::Multiple::LOB is tied interface wrapper of store_lob,
fetch_lob, exists_lob, delete_lob. When using LOB, empty key can not
be used, and traversal access (keys, values, each) is not available.
It is a little inconvenient, so I recommend using QDBM_File::Multiple
and xxxxx_lob api directly.

=head1 SEE ALSO

L<DB_File(3)>, L<perldbmfilter>.

=head1 COPYRIGHT & LICENSE

Copyright 2007 Toshiyuki Yamato, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
