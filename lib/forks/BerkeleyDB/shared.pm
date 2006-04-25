package forks::BerkeleyDB::shared;

$VERSION = 0.01;
use strict;
use warnings;
use BerkeleyDB 0.27;
use Storable qw(freeze thaw);
use Tie::Restore 0.11;
#use Scalar::Util qw(weaken);

use constant DEBUG => 0;
#use Data::Dumper;

our %object_refs;	#refs of all shared objects (for CLONE use, and strong refs: allow shared vars to hold other shared vars as values; END{...} cleanup in all threads)
our @shared_cache;	#tied BDB array that stores shared variable objects for other threads to use to reconstitute if they were created outside their scope
our $bdb_env;	#berkeleydb environment

use constant TERMINATOR => "\0";
use constant ELEM_NOT_EXISTS => "!";	#indicates element does not exist (used for arrays)

########################################################################
sub _filter_fetch_value {
#warn "output: '$_', defined=",defined $_,",length=",length $_ if DEBUG;
	if (!defined $_ || length $_ == 0) { $_ = undef; }
	elsif (length $_ == 1 && $_ eq ELEM_NOT_EXISTS) {
		$_ = forks::BerkeleyDB::shared::Elem::NotExists->new();
	}
	else {
		if (substr($_, -1) eq TERMINATOR) {	#regular data value
			chop($_);
		}
		else {	#is a shared var, retie to same shared ordinal
#warn Dumper($_, $object_refs{$_}, defined $shared_cache[$_] ? thaw($shared_cache[$_]) : undef) if DEBUG;
			if (!defined $object_refs{$_} || !defined $object_refs{$_}->{bdb_reconnect} || $object_refs{$_}->{bdb_reconnect}) {	#shared var created outside scope of this thread or needs to be reloaded: load object from shared var cache & reconnect to db
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
			my $scalar; my @array; my %hash; #my *handle;
			if ($object_refs{$_}->{'type'} eq 'scalar') { tie $scalar, 'Tie::Restore', $object_refs{$_}; $_ = \$scalar; }
			elsif ($object_refs{$_}->{'type'} eq 'array') { tie @array, 'Tie::Restore', $object_refs{$_}; $_ = \@array; }
			elsif ($object_refs{$_}->{'type'} eq 'hash') { tie %hash, 'Tie::Restore', $object_refs{$_}; $_ = \%hash; }
#			elsif ($object_refs{$_}->{'type'} eq 'scalar') { tie *handle, 'Tie::Restore', $object_refs{$_}; $_ = \*handle; }
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
			if (UNIVERSAL::isa($_, 'forks::BerkeleyDB::shared::Elem::NotExists')) { $_ = ELEM_NOT_EXISTS; }
			else {
				my $tied = ref($_) eq 'SCALAR' ? tied ${$_} 
					: ref($_) eq 'ARRAY' ? tied @{$_} 
					: ref($_) eq 'HASH' ? tied %{$_} 
					: ref($_) eq 'GLOB' ? tied *{$_} : undef;
#warn "input: ".Dumper(ref $_, $tied, $_) if DEBUG;
				if (UNIVERSAL::isa($tied, 'threads::shared')) {	#store shared ref ordinal
					$_ = $tied->{'ordinal'};
				}
				else {	#future: transparently bless any type of object across all threads?
					_croak( "Invalid value for shared scalar: ".Dumper($_) );
#					_croak( "Invalid value for shared scalar" );
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
	use forks::shared (); die "forks version 0.18 required--this is only version $threads::VERSION" unless $threads::VERSION >= 0.18;
	use File::Spec;
	use constant ENV_ROOT => File::Spec->tmpdir().'/perlforks';
	use constant ENV_PATH => ENV_ROOT.'/env.'.$$;	#would prefer $threads::SHARED, although current pid should be safe as long as it's main thread
	
	*_croak = *_croak = \&threads::_croak;
	
	_croak( "Must first 'use forks::BerkeleyDB'\n" ) unless $INC{'forks/BerkeleyDB.pm'};

	sub _open_env () {
		### open the base environment ###
		return new BerkeleyDB::Env(
			-Home  => ENV_PATH,
			-Flags => DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL,
		) or _croak( "Can't create BerkeleyDB::Env (home=".ENV_PATH."): $BerkeleyDB::Error" );
	}
	
	sub _purge_env () {
		opendir(ENVDIR, ENV_PATH);
		my @files_to_del = grep(!/^(\.|\.\.)$/, readdir(ENVDIR));
		closedir(ENVDIR);
		warn "unlinking: ".join(', ', map(ENV_PATH."/$_", @files_to_del)) if DEBUG;
		foreach (@files_to_del) {
			my $file = ENV_PATH."/$_";
			$file =~ m/^([\/-\@\w_.]+)$/so;	#untaint
			_croak( "Unable to unlink file '$1'. Please manually remove this file." ) unless unlink $1;
		}
	}
	
	#need to store separate, serialized, db-disconnected copy in a separate database, so other threads can re-create arrayrefs and hashrefs
	sub _tie_shared_cache () {
		use forks::BerkeleyDB::shared::array;
		untie @shared_cache;
		tie @shared_cache, 'forks::BerkeleyDB::shared::array', (
			-Filename => ENV_PATH."/shared.bdb",
			-Flags    => DB_CREATE,
			-Mode     => 0666,
			-Env      => $bdb_env,
		);
	}
	
	*CORE::GLOBAL::fork = sub {
		### safely sync & close databases, close environment ###
		foreach my $key (keys %forks::BerkeleyDB::shared::object_refs) {
#			eval { $forks::BerkeleyDB::shared::object_refs{$key}->{bdb}->db_sync(); };
			eval { $forks::BerkeleyDB::shared::object_refs{$key}->{bdb}->db_close(); };
			$object_refs{$key}->{bdb_reconnect} = 1;	#hint that this object must be recreated from cache
		}
		untie @forks::BerkeleyDB::shared::shared_cache;
		$forks::BerkeleyDB::shared::bdb_env = undef;
		
		### do the fork ###
		my $pid = CORE::fork;

		if (!defined $pid || $pid) { #in parent
			### re-open environment and immediately retie to critical databases ###
			$forks::BerkeleyDB::shared::bdb_env = forks::BerkeleyDB::shared::_open_env();
			forks::BerkeleyDB::shared::_tie_shared_cache();
#			foreach my $key (keys %forks::BerkeleyDB::shared::object_refs) {
#				my $sub = 'forks::BerkeleyDB::shared::_tie'.$forks::BerkeleyDB::shared::object_refs{$key}->{type};
#				{
#					no strict 'refs';
#					$forks::BerkeleyDB::shared::object_refs{$key} = &{$sub}($forks::BerkeleyDB::shared::object_refs{$key});
#				}
#			}
		}
				
		return $pid;
	};

	### create/purge necessary paths to create clean environment ###
	if (-d ENV_PATH) {
		_purge_env();
	}
	else {
		unless (-d ENV_ROOT) {
			my $status = mkdir ENV_ROOT, 0777;
			_croak( "Can't create directory ".ENV_ROOT ) unless $status;
		}
		mkdir ENV_PATH, 0777 or _croak( "Can't create directory ".ENV_PATH );
	}
	
	### create the base environment ###
	$bdb_env = _open_env();
	_tie_shared_cache();
}

END {
	foreach my $key (keys %object_refs) {
#		eval { $object_refs{$key}->{bdb}->db_sync(); };
		eval { $object_refs{$key}->{bdb}->db_close(); };
	}
	untie @shared_cache;
	#also remove database if no threads connected to any databases (maybe use recno DB to monitor num of threads connected per shared var)?
}

sub CLONE {	#reopen environment and immediately retie to critical databases
	$bdb_env = _open_env();
	_tie_shared_cache();
#	foreach my $key (keys %object_refs) {
##		eval { $object_refs{$key}->{bdb}->db_sync(); };
#		eval { $object_refs{$key}->{bdb}->db_close(); };
#warn "In clone (tid #".threads->tid."): $key -> ".ref($object_refs{$key}) if DEBUG;
#		my $sub = '_tie'.$object_refs{$key}->{type};
#		{
#			no strict 'refs';
#			&{$sub}($object_refs{$key});
#		}
#	}
}

*import = *import = \&forks::shared::import;

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
		-Env      => $bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_reconnect} = 0;

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
		-Env      => $bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_reconnect} = 0;
	
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
		-Env      => $bdb_env,
	) or _croak( "Can't create bdb $bdb_path" );
	$obj->{bdb}->filter_fetch_value(\&_filter_fetch_value);
	$obj->{bdb}->filter_store_value(\&_filter_store_value);
	$obj->{bdb_reconnect} = 0;
	
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
	$obj->{bdb_reconnect} = 0;
	
	### store ref in package variable ###
	$object_refs{$obj->{ordinal}} = $obj;

	return $obj;
}

########################################################################
### overload some subs and methods in forks and forks::shared ###
{
	no warnings 'redefine';	#allow overloading without warnings

	*threads::_new = \&threads::new;
	*threads::new = sub {
		my $class = shift;

		### safely sync & close databases, close environment ###
		foreach my $key (keys %forks::BerkeleyDB::shared::object_refs) {
#			eval { $object_refs{$key}->{bdb}->db_sync(); };
			eval { $object_refs{$key}->{bdb}->db_close(); };
			$object_refs{$key}->{bdb_reconnect} = 1;	#hint that this object must be recreated from cache
		}
		untie @forks::BerkeleyDB::shared::shared_cache;
		$forks::BerkeleyDB::shared::bdb_env = undef;
		
		### do whatever threads::new usually does ###
		my @result = $class->_new(@_);
		
		### re-open environment and immediately retie to critical databases ###
		$forks::BerkeleyDB::shared::bdb_env = forks::BerkeleyDB::shared::_open_env();
		forks::BerkeleyDB::shared::_tie_shared_cache();
#		foreach my $key (keys %forks::BerkeleyDB::shared::object_refs) {
#			my $sub = 'forks::BerkeleyDB::shared::_tie'.$object_refs{$key}->{type};
#			{
#				no strict 'refs';
#				&{$sub}($object_refs{$key});
#			}
#		}
		
		return wantarray ? @result : $result[0];
	};
	
	*threads::_isthread = \&threads::isthread;
	*threads::isthread = sub {
		forks::BerkeleyDB::shared::CLONE();	#retie shared vars
		threads::_isthread(@_);
	};

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
		if (!defined $self->{bdb_reconnect} || $self->{bdb_reconnect}) {	#shared var needs to be reloaded: load shared var cache & connect to db
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
		}
		delete $object_refs{$self->{ordinal}};
		threads::shared::_command( '_tied',$self->{'ordinal'},$self->{'module'}.'::DESTROY' );
	}
}

########################################################################
package forks::BerkeleyDB::shared::Elem::NotExists;
use strict;
use warnings;

sub new {
	my $type = shift;
	my $class = ref($type) || $type;
	return bless({}, $class);
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
extension of L<forks::shared>.  The goal of this module is to attempt to improve upon the core
performance of L<forks::shared> at a level comparable to native ithreads (L<threads::shared>).

Depending on how you architect your data processing, you should expect to achieve approx 75%
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

Currently optimizes SCALAR, ARRAY, and HASH shared variables.  HANDLE type is supported 
using the default method implemented by forks::shared.

Shared variable access and modification are NOT guaranteed to be handled as atomic events.  
This deviates from undocumented L<forks> behavior, where all these events are atomic, but
it correctly models the expected behavior of L<threads>.  Thus, don't forget to lock() 
your shared variable before using them concurrently in multiple threads; otherwise, results
may not be what you expect.

When share is used on arrays, hashes, array refs or hash refs, any data they contain will 
be lost.  This correctly models the expected behavior of L<threads>, but not (currently) 
of L<forks>.

=head1 CAVIATS

=head1 TODO

Implement shared variable locks, signals, and waiting with BerkeleyDB.

Support transparent bless across threads.

=head1 AUTHOR

Eric Rybski <rybskej@yahoo.com>.

=head1 COPYRIGHT

Copyright (c) 2006 Eric Rybski <rybskej@yahoo.com>.
All rights reserved.  This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<forks::shared>, L<threads::shared>

=cut
