package forks::BerkeleyDB::shared::scalar;

$VERSION = 0.02;
use strict;
use warnings;
use BerkeleyDB 0.27;
use vars qw(@ISA);
@ISA = qw(BerkeleyDB::Recno);

#---------------------------------------------------------------------------
sub new {
	my $type = shift;
	my $class = ref($type) || $type;
	my $self = $class->SUPER::new(@_);
	return undef unless defined $self;
	return bless($self, $class);
}

# standard Perl feature methods implemented:
#	TIESCALAR
#	FETCH, STORE
#	UNTIE, DESTROY

#---------------------------------------------------------------------------
*TIESCALAR = *TIESCALAR = \&new;

#---------------------------------------------------------------------------
sub FETCH {
	my $self = shift;
	my ($key, $value) = (0, undef);
	return undef unless $self->db_get($key, $value) == 0;
	return $value;
}

sub STORE {
	my $self = shift;
	return unless @_;
	my $value = shift;
	my $key = 0;
	{
		no warnings 'uninitialized';
		return undef unless $self->db_put($key, $value) == 0;
	}
	return $value;
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
	$status = eval { $self->db_close(); };
	return defined $status && $status == 0 ? 0 : 1;
}

#---------------------------------------------------------------------------
1;

__END__
=pod

=head1 NAME

forks::BerkeleyDB::shared::scalar - class for tie-ing scalars to BerkeleyDB Recno

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
