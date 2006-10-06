package
	CORE::GLOBAL;	#hide from PAUSE
use subs qw(fork);
{
	no warnings 'redefine';
	*fork = \&forks::BerkeleyDB::shared::_fork;
}

package forks::BerkeleyDB::shared;

$VERSION = 0.03;
use strict;
use warnings;
use forks::BerkeleyDB::Config;
use BerkeleyDB 0.27;
use Storable qw(freeze thaw);
use Tie::Restore 0.11;
use Scalar::Util qw(blessed reftype);
#use Scalar::Util qw(weaken);

use constant DEBUG => forks::BerkeleyDB::Config::DEBUG();
use constant ENV_ROOT => forks::BerkeleyDB::Config::ENV_ROOT();
use constant ENV_PATH => forks::BerkeleyDB::Config::ENV_PATH();
#use Data::Dumper;

our %object_refs;	#refs of all shared objects (for CLONE use, and strong refs: allow shared vars to hold other shared vars as values; END{...} cleanup in all threads)
our @shared_cache;	#tied BDB array that stores shared variable objects for other threads to use to reconstitute if they were created outside their scope
our @shared_cache_attr_bless;	#tied BDB array that stores shared variable object attribute bless

use constant TERMINATOR => "\0";
use constant ELEM_NOT_EXISTS => "!";	#indicates element does not exist (used for arrays)

########################################################################
sub _filter_fetch_value {
#warn "output: '$_', defined=",defined $_,",length=",length $_ if DEBUG;
	if (!defined $_ || length $_ == 0) { $_ = undef; }
	elsif (length $_ == 1 && $_ eq ELEM_NOT_EXISTS) {
		$_ = forks::BerkeleyDB::ElemNotExists->new();
	}
	else {
		if (substr($_, -1) eq TERMINATOR) {	#regular data value
			chop($_);
		}
		else {	#is a shared var, retie to same shared ordinal
#warn Dumper($_, $object_refs{$_}, defined $shared_cache[$_] ? thaw($shared_cache[$_]) : undef) if DEBUG;
			if (!defined $object_refs{$_} || !defined $object_refs{$_}->{bdb_is_connected} || !$object_refs{$_}->{bdb_is_connected}) {	#shared var created outside scope of this thread or needs to be reloaded: load object from shared var cache & reconnect to db
#warn "*********".threads->tid().": _filter_fetch_value -> obj \#$_ recreated\n"; #if DEBUG;
				my $obj = defined $object_refs{$_} && defined $object_refs{$_}->{bdb_module} 
					? $object_refs{$_}
					: eval { @{thaw($forks::BerkeleyDB::shared::shared_cache[$_])}[0] };
				_croak( "Unable to load object state for shared variable \#$_" ) unless defined $obj;
				my $sub = '_tie'.$obj->{type};
				{
					no strict 'refs';
					&{$sub}($obj);
				}
			}
			my $class = $shared_cache_attr_bless[$_];

			if ($object_refs{$_}->{'type'} eq 'scalar')
				{ my $s; tie $s, 'Tie::Restore', $object_refs{$_}; $_ = $class ? CORE::bless(\$s, $class) : \$s; }
			elsif ($object_refs{$_}->{'type'} eq 'array')
				{ my @a; tie @a, 'Tie::Restore', $object_refs{$_}; $_ = $class ? CORE::bless(\@a, $class) : \@a; }
			elsif ($object_refs{$_}->{'type'} eq 'hash')
				{ my %h; tie %h, 'Tie::Restore', $object_refs{$_}; $_ = $class ? CORE::bless(\%h, $class) : \%h; }
#			elsif ($object_refs{$_}->{'type'} eq 'scalar')
#				{ my *h; tie *h, 'Tie::Restore', $object_refs{$_}; $_ = $class ? CORE::bless(\*h, $class) : \*h; }
			else {
				_croak( "Unable to restore shared variable \#$_: ".ref($object_refs{$_}) );
			}
		}
	}
}

sub _filter_store_value {
#warn "input: '$_', defined=",defined $_,",length=",length $_ if DEBUG;
	if (defined $_) {
		if (ref($_)) {	#does this support both share(@a) and share(\@_)?
			if (UNIVERSAL::isa($_, 'forks::BerkeleyDB::ElemNotExists')) { $_ = ELEM_NOT_EXISTS; }
			else {
				my $tied = reftype($_) eq 'SCALAR' ? tied ${$_} 
					: reftype($_) eq 'ARRAY' ? tied @{$_} 
					: reftype($_) eq 'HASH' ? tied %{$_} 
					: reftype($_) eq 'GLOB' ? tied *{$_} : undef;
#warn "input: ".Dumper(ref $_, reftype $_, blessed $_, $tied, $_) if DEBUG;
				if (UNIVERSAL::isa($tied, 'threads::shared')) {	#store shared ref ordinal
					$_ = $tied->{'ordinal'};
				}
				else {	#future: transparently bless any type of object across all threads?
					_croak( "Invalid value for shared scalar: ".(reftype($_) || $_) );
				}
			}
		}
		else {
			$_ .= TERMINATOR();
		}
	}
#warn "input final: defined=",defined $_,",length=",length $_ if DEBUG;
}

########################################################################
BEGIN {
	use forks::shared (); die "forks version 0.18 required--this is only version $threads::VERSION" unless defined $forks::VERSION && $forks::VERSION >= 0.18;
	use forks::BerkeleyDB::shared::array;
	
	*_croak = *_croak = \&threads::_croak;
	
	_croak( "Must first 'use forks::BerkeleyDB'\n" ) unless $INC{'forks/BerkeleyDB.pm'};

	#need to store separate, serialized, db-disconnected copy in a separate database, so other threads can re-create arrayrefs and hashrefs
	sub _tie_shared_cache () {
		tie @shared_cache, 'forks::BerkeleyDB::shared::array', (
			-Filename => ENV_PATH."/shared.bdb",
			-Flags    => DB_CREATE,
			-Mode     => 0666,
			-Env      => $forks::BerkeleyDB::bdb_env,
		);

		tie @shared_cache_attr_bless, 'forks::BerkeleyDB::shared::array', (
			-Filename => ENV_PATH."/shared_attr_bless.bdb",
			-Flags    => DB_CREATE,
			-Mode     => 0666,
			-Env      => $forks::BerkeleyDB::bdb_env,
		);
	}
	
	sub _untie_shared_cache () {
		untie @shared_cache;
		untie @shared_cache_attr_bless;
	}
	
	sub _fork {
		### safely sync & close databases ###
		{
			local $@;
			foreach my $key (keys %object_refs) {
				if ($object_refs{$key}->{bdb_is_connected}) {
#					eval { $object_refs{$key}->{bdb}->db_sync(); };
					eval { $object_refs{$key}->{bdb}->db_close(); };
					$object_refs{$key}->{bdb_is_connected} = 0;
				}
				$object_refs{$key}->{bdb_is_connected} = 0;	#hint that this object must be recreated from cache
			}
		}
		_untie_shared_cache();
		
		### do the fork ###
		my $pid = forks::BerkeleyDB::_fork();

		if (!defined $pid || $pid) { #in parent
			### immediately retie to critical databases ###
			_tie_shared_cache();
#			foreach my $key (keys %object_refs) {
#				my $sub = 'forks::BerkeleyDB::shared::_tie'.$object_refs{$key}->{type};
#				{
#					no strict 'refs';
#					$object_refs{$key} = &{$sub}($object_refs{$key});
#				}
#			}
		}
				
		return $pid;
	};
	
	*import = *import = \&forks::shared::import;
	
	*_ORIG_CLONE = *_ORIG_CLONE = \&forks::BerkeleyDB::CLONE;
	{
		no warnings 'redefine';
		*forks::BerkeleyDB::CLONE = \&_CLONE;
	}

	sub _CLONE {	#reopen environment and immediately retie to critical databases
		_ORIG_CLONE(@_);
		_tie_shared_cache();
	#	local $@;
	#	foreach my $key (keys %object_refs) {
	#		if ($object_refs{$key}->{bdb_is_connected}) {
	##			eval { $object_refs{$key}->{bdb}->db_sync(); };
	#			eval { $object_refs{$key}->{bdb}->db_close(); };
	#			$object_refs{$key}->{bdb_is_connected} = 0;
	#		}
	#warn "In clone (tid #".threads->tid."): $key -> ".ref($object_refs{$key}) if DEBUG;
	#		my $sub = '_tie'.$object_refs{$key}->{type};
	#		{
	#			no strict 'refs';
	#			&{$sub}($object_refs{$key});
	#		}
	#	}
	}

	### create the base environment ###
	_tie_shared_cache();
}

END {
	{
		local $@;
		foreach my $key (keys %object_refs) {
			if ($object_refs{$key}->{bdb_is_connected}) {
#				eval { $object_refs{$key}->{bdb}->db_sync(); };
				eval { $object_refs{$key}->{bdb}->db_close(); };
				$object_refs{$key}->{bdb_is_connected} = 0;
			}
		}
	}
	eval { _untie_shared_cache(); };
}

########################################################################
sub _tiescalar ($) {
	my $obj = shift;
	return $obj unless ref($obj);
	$shared_cache[$obj->{ordinal}] = freeze([$obj]) unless defined $obj->{bdb_module};
	
	### create the database and store as additional property in the object ###
	$obj->{bdb_module} = __PACKAGE__.'::'.$obj->{type};
	(my $module_inc = $obj->{bdb_module}) =~ s/::/\//go; 
	eval "use $obj->{bdb_module}" unless exists $INC{$module_inc};
	my $bdb_path = ENV_PATH.'/'.$obj->{ordinal}.".bdb";
	$obj->{bdb} = $obj->{bdb_module}->new(
		-Filename => $bdb_path,
		-Flags    => DB_CREATE,
		-Mode     => 0666,
		-Env      => $forks::BerkeleyDB::bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_is_connected} = 1;

	### store ref in package variable ###
	$object_refs{$obj->{ordinal}} = $obj;
#	weaken($object_refs{$obj->{ordinal}});
	
	return $obj;
}

sub _tiearray ($) {
	my $obj = shift;
	return $obj unless ref($obj);
	$shared_cache[$obj->{ordinal}] = freeze([$obj]) unless defined $obj->{bdb_module};

	### create the database and store as additional property in the object ###
	$obj->{bdb_module} = __PACKAGE__.'::'.$obj->{type};
	(my $module_inc = $obj->{bdb_module}) =~ s/::/\//go; 
	eval "use $obj->{bdb_module}" unless exists $INC{$module_inc};
	my $bdb_path = ENV_PATH.'/'.$obj->{ordinal}.".bdb";
	$obj->{bdb} = $obj->{bdb_module}->new(
		-Filename => $bdb_path,
		-Flags    => DB_CREATE,
		-Property => DB_RENUMBER,
		-Mode     => 0666,
		-Env      => $forks::BerkeleyDB::bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_is_connected} = 1;
	
	### store ref in package variable ###
	$object_refs{$obj->{ordinal}} = $obj;
#	weaken($object_refs{$obj->{ordinal}});

	return $obj;
}

sub _tiehash ($) {
	my $obj = shift;
	return $obj unless ref($obj);
	$shared_cache[$obj->{ordinal}] = freeze([$obj]) unless defined $obj->{bdb_module};

	### create the database and store as additional property in the object ###
	$obj->{bdb_module} = __PACKAGE__.'::'.$obj->{type};
	(my $module_inc = $obj->{bdb_module}) =~ s/::/\//go; 
	eval "use $obj->{bdb_module}" unless exists $INC{$module_inc};
	my $bdb_path = ENV_PATH.'/'.$obj->{ordinal}.".bdb";
	$obj->{bdb} = $obj->{bdb_module}->new(
		-Filename => $bdb_path,
		-Flags    => DB_CREATE,
		-Mode     => 0666,
		-Env      => $forks::BerkeleyDB::bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_is_connected} = 1;
	
	### store ref in package variable ###
	$object_refs{$obj->{ordinal}} = $obj;
#	weaken($object_refs{$obj->{ordinal}});

	return $obj;
}

sub _tiehandle ($) {
	my $obj = shift;
	return $obj unless ref($obj);
	$shared_cache[$obj->{ordinal}] = freeze([$obj]) unless defined $obj->{bdb_module};

	$obj->{bdb_module} = __PACKAGE__.'::'.$obj->{type};
	$obj->{bdb} = undef;
	$obj->{bdb_is_connected} = 1;
	
	### store ref in package variable ###
	$object_refs{$obj->{ordinal}} = $obj;

	return $obj;
}

########################################################################
### overload some subs and methods in forks and forks::shared ###
{
	no warnings 'redefine';	#allow overloading without warnings

	sub threads::shared::_bless {
		my $it  = shift;
		my $ref = reftype $it;
		my $class = shift;
		my $object;
		
		if ($ref eq 'SCALAR') {
			$object = tied ${$it};
#			my $ref2 = reftype ${$it} || '';	#not necessary?
#			if ($ref2 eq 'SCALAR') {
#				$object = tied ${${$it}};
#			} elsif ($ref2 eq 'ARRAY') {
#				$object = tied @{${$it}};
#			} elsif ($ref2 eq 'HASH') {
#				$object = tied %{${$it}};
#			} elsif ($ref2 eq 'GLOB') {
#				$object = tied *{${$it}};
#			} else {
#				$object = tied ${$it};
#			}
		} elsif ($ref eq 'ARRAY') {
			$object = tied @{$it};
		} elsif ($ref eq 'HASH') {
			$object = tied %{$it};
		} elsif ($ref eq 'GLOB') {
			$object = tied *{$it};
		}

		if (defined $object && blessed $object && $object->isa('threads::shared')) {
			my $ordinal = $object->{'ordinal'};
			$shared_cache_attr_bless[$object->{ordinal}] = $class;
		}
	}

	sub threads::shared::TIESCALAR {
		return forks::BerkeleyDB::shared::_tiescalar(shift->_tie( 'scalar',@_ ));
	}
	sub threads::shared::TIEARRAY {
		return forks::BerkeleyDB::shared::_tiearray(shift->_tie( 'array',@_ ));
	}
	sub threads::shared::TIEHASH {
		return forks::BerkeleyDB::shared::_tiehash(shift->_tie( 'hash',@_ ));
	}
	sub threads::shared::TIEHANDLE {
		return forks::BerkeleyDB::shared::_tiehandle(shift->_tie( 'handle',@_ ));
	}

	sub threads::shared::AUTOLOAD {
		my $self = shift;
		if (!defined $self->{bdb_is_connected} || !$self->{bdb_is_connected}) {	#shared var needs to be reloaded: load shared var cache & connect to db
#warn "*********".threads->tid().": threads::shared::AUTOLOAD -> obj \#$self->{ordinal}\n"; #if DEBUG;
			my $obj = defined $object_refs{$self->{ordinal}} && defined $object_refs{$self->{ordinal}}->{bdb_module} 
				? $object_refs{$self->{ordinal}}
				: eval { @{thaw($forks::BerkeleyDB::shared::shared_cache[$self->{ordinal}])}[0] };
			_croak( "Unable to load object state for shared variable \#$self->{ordinal}" ) unless defined $obj;
			my $sub = 'forks::BerkeleyDB::shared::_tie'.$obj->{type};
			{
				no strict 'refs';
				$self = &{$sub}($obj);
			}
		}
		(my $sub = $threads::shared::AUTOLOAD) =~ s/^.*::/$self->{'bdb_module'}::/;
#warn "$sub, $self->{ordinal}" if DEBUG;
#warn Dumper(\@_) if DEBUG;
		my @result;
		@result = $self->{'bdb'}->$sub(@_) if defined $self->{'bdb'};
		wantarray ? @result : $result[0];
	}

	sub threads::shared::UNTIE {
		my $self = shift;
		return if $self->{'CLONE'} != $threads::shared::CLONE;
		if (defined $self->{'bdb_module'}) {
			my $sub = "$self->{'bdb_module'}::UNTIE";
			my @result;
			{
				no strict 'refs';
				@result = &{$sub}(@_);
			}
		}
		delete $object_refs{$self->{ordinal}};
		threads::shared::_command( '_untie',$self->{'ordinal'} );
	}

	sub threads::shared::DESTROY {
		my $self = shift;
		return if $self->{'CLONE'} != $threads::shared::CLONE;
		if (defined $self->{'bdb_module'}) {
			my $sub = "$self->{'bdb_module'}::DESTROY";
			my @result;
			{
				no strict 'refs';
				@result = &{$sub}(@_);
			}
			$self->{bdb_is_connected} = 0;
		}
		delete $object_refs{$self->{ordinal}};
		threads::shared::_command( '_tied',$self->{'ordinal'},$self->{'module'}.'::DESTROY' );
	}
}

1;

__END__
=pod

=head1 NAME

forks::BerkeleyDB::shared - high-performance drop-in replacement for threads::shared

=head1 SYNOPSYS

  use forks::BerkeleyDB;
  use forks::BerkeleyDB::shared;

  my $variable : shared;
  my @array    : shared;
  my %hash     : shared;

  share( $variable );
  share( @array );
  share( %hash );

  lock( $variable );
  cond_wait( $variable );
  cond_wait( $variable, $lock_variable );
  cond_timedwait( $variable, abs time );
  cond_timedwait( $variable, abs time, $lock_variable );
  cond_signal( $variable );
  cond_broadcast( $variable );

=head1 DESCRIPTION

forks::BerkeleyDB::shared is a drop-in replacement for L<threads::shared>, written as an
extension of L<forks::shared>.  The goal of this module improve upon the core performance
of L<forks::shared> at a level comparable to native ithreads (L<threads::shared>).

Depending on how you architect your data processing, as well as how your target platform
filesystem has been configured and tuned, you should expect to achieve around 75%
the performance of native ithreads for all shared variable operations.  Given that this
module is written entirely in pure perl, this is an outstanding benchmark and is a testament
to the performance of BerkeleyDB.  Performance could likely be further improved by migrating
some of the code to XS (especially some of the operator methods in tied module packages).

=head1 USAGE

See L<forks::shared> for common usage information.

=head2 Location of database files

This module will use $ENV{TMPDIR} (unless taint is on) or /tmp for all back-end database and
other support files.  For the most part, BerkeleyDB will use shared memory for as much frequently
accesed data as possible, so you probably won't notice drive-based performance hits.  For optimal
performance, use a partition with a physical drive dedicate for tempory space usage.

=head1 NOTES

Forks 0.19 or later is required to support transparent blessing across threads.  This feature
will be silently disabled if this requirement is not met.

Currently optimizes SCALAR, ARRAY, and HASH shared variables.  HANDLE type is supported 
using the default method implemented by L<forks::shared>.

Shared variable access and modification are NOT guaranteed to be handled as atomic events.  
This deviates from undocumented L<forks> behavior, where all these events are atomic, but
it correctly models the expected behavior of L<threads>.  Thus, don't forget to lock() 
your shared variable before using them concurrently in multiple threads; otherwise, results
may not be what you expect.

When share is used on arrays, hashes, array refs or hash refs, any data they contain will 
be lost.  This correctly models the expected behavior of L<threads>, but not (currently) 
of L<forks>.

=head1 TODO

Monitor number of connected shared variables per thread and dynamically disconnect uncommonly
used vars based on last usage and/or frequency of usage (to meet BDB environment lock limits).

Implement shared variable locks, signals, and waiting with BerkeleyDB.

=head1 AUTHOR

Eric Rybski <rybskej@yahoo.com>.

=head1 COPYRIGHT

Copyright (c) 2006 Eric Rybski <rybskej@yahoo.com>.
All rights reserved.  This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<forks::shared>, L<threads::shared>

=cut
