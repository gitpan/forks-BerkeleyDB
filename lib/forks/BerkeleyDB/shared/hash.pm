package forks::BerkeleyDB::shared::hash;

$VERSION = 0.01;
use strict;
use warnings;
use BerkeleyDB 0.27;
use vars qw(@ISA);
@ISA = qw(BerkeleyDB::Btree);

#---------------------------------------------------------------------------
sub new {
	my $type = shift;
	my $class = ref($type) || $type;
	my $self = $class->SUPER::new(@_);
	return undef unless defined $self;
	return bless($self, $class);
}

# standard Perl feature methods implemented:
#	TIEHASH
#	FETCH, STORE
#	CLEAR, DELETE
#	EXISTS
#	FIRSTKEY, NEXTKEY
#	SCALAR
#	UNTIE, DESTROY

#---------------------------------------------------------------------------
*TIEHASH = *TIEHASH = \&new;

#---------------------------------------------------------------------------
sub FETCH {
	my $self = shift;
	return undef unless @_;
	my $value = $self->SUPER::FETCH(@_);
	return $value;
}

sub STORE {
	my $self = shift;
	my $key = shift;
	return undef unless @_;
	my $value = shift;
	return undef unless $self->SUPER::STORE($key, $value, @_) == 0;
	return $value;
}

#---------------------------------------------------------------------------
sub DELETE {
	my $self = shift;
	return undef unless @_;
	my $key = shift;
	my $value = undef;
	my $cursor = $self->db_cursor(DB_WRITECURSOR);
	return undef unless $cursor->c_get($key, $value, DB_SET) == 0;	#set cursor position
	$cursor->c_del();
	$cursor->c_close();
	return $value;
}

sub CLEAR {
	my $self = shift;
	my $count = 0;
	$self->truncate($count);
	return defined $count && $count > 0 ? 1 : 0;
}

#---------------------------------------------------------------------------
#sub EXISTS {}	#use BerkeleyDB.pm method

#---------------------------------------------------------------------------
sub FIRSTKEY {
	my $self = shift;
	my ($key, $value) = ('', undef);
	my $cursor = $self->db_cursor();
	return (wantarray ? (undef, undef) : undef)
		unless $cursor->c_get($key, $value, DB_FIRST) == 0;
	$cursor->c_close();
	return wantarray ? ($key, $value) : $key;
}

sub NEXTKEY {
	my $self = shift;
	my $key = shift;
	my $value = undef;
	my $cursor = $self->db_cursor();
	return (wantarray ? (undef, undef) : undef)
		unless $cursor->c_get($key, $value, DB_SET) == 0;	#set cursor position
	return (wantarray ? (undef, undef) : undef)
		unless $cursor->c_get($key, $value, DB_NEXT) == 0;
	$cursor->c_close();
	return wantarray ? ($key, $value) : $key;
}

#---------------------------------------------------------------------------
sub SCALAR {
	my $self = shift;
	my $stat = $self->db_stat();
	return defined $stat->{bt_nkeys} && $stat->{bt_nkeys} > 0 ? 1 : 0;
}

#---------------------------------------------------------------------------
sub UNTIE {
	my $self = shift;
	my $status = eval { $self->db_sync(); };
	$status = eval { $self->db_close(); };
	return defined $status && $status == 0 ? 0 : 1;
}

sub DESTROY {
	my $self = shift;
	my $status = eval { $self->db_sync(); };
#my $warn = threads->tid.": In DESTROY hash: ".(defined $status ? $status : '');
	$status = eval { $self->db_close(); };
#warn $warn.(defined $status ? ", $status" : '');
	return defined $status && $status == 0 ? 0 : 1;
}

#---------------------------------------------------------------------------
1;

__END__
=pod

=head1 NAME

forks::BerkeleyDB::shared::hash - class for tie-ing hashes to BerkeleyDB Btree

=head1 DESCRIPTION

Helper class for L<forks::BerkeleyDB::shared>.  See documentation there.

=head1 AUTHOR

Eric Rybski <rybskej@yahoo.com>.

=head1 COPYRIGHT

Copyright (c) 2006 Eric Rybski <rybskej@yahoo.com>.
All rights reserved.  This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<forks::BerkeleyDB::shared>, L<forks::shared>.

=cut
