package
	forks::BerkeleyDB::ElemNotExists;	#hide from PAUSE

$VERSION = 0.03;
use strict;
use warnings;

sub new {
	my $type = shift;
	return CORE::bless(\do { my $o }, ref($type) || $type);
}

1;
