package forks::BerkeleyDB;

$VERSION = 0.02;
use forks::BerkeleyDB::Config;
use BerkeleyDB 0.27;
use Storable qw(freeze thaw);

use constant DEBUG => forks::BerkeleyDB::Config::DEBUG();
use constant ENV_ROOT => forks::BerkeleyDB::Config::ENV_ROOT();
use constant ENV_PATH => forks::BerkeleyDB::Config::ENV_PATH();

our $bdb_env;	#berkeleydb environment

BEGIN {
	use forks (); die "forks version 0.18 required--this is only version $threads::VERSION" unless $threads::VERSION >= 0.18;

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

	sub _tie_support_vars () {

	}

	sub _untie_support_vars () {

	}
	
	sub _fork {
		### safely sync & close databases, close environment ###
		forks::BerkeleyDB::_untie_support_vars();
		$forks::BerkeleyDB::bdb_env = undef;
		
		### do the fork ###
		my $pid = CORE::fork;

		if (!defined $pid || $pid) { #in parent
			### re-open environment and immediately retie to critical databases ###
			$forks::BerkeleyDB::bdb_env = forks::BerkeleyDB::_open_env();
			forks::BerkeleyDB::_tie_support_vars();
		}
				
		return $pid;
	};

	{
		no warnings 'redefine';
		*CORE::GLOBAL::fork = \&_fork;
	}
	
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
	$bdb_env = forks::BerkeleyDB::_open_env();
	_tie_support_vars();
}

END {
	_untie_support_vars();
	$bdb_env = undef;
	#also remove database if no threads connected to any databases (maybe use recno DB to monitor num of threads connected per shared var)?
}

*import = *import = \&forks::import;

sub CLONE {	#reopen environment and immediately retie to critical databases
	$bdb_env = _open_env();
	_tie_support_vars();
}

########################################################################
### overload some subs and methods in forks and forks::shared ###
{
	no warnings 'redefine';	#allow overloading without warnings

	*_ORIG_new = *_ORIG_new = \&threads::new;
	*threads::new = \&_new;
	
	sub _new {
		my $class = shift;

		### safely sync & close databases, close environment ###
		forks::BerkeleyDB::_untie_support_vars();
		$forks::BerkeleyDB::bdb_env = undef;
		
		### do whatever threads::new usually does ###
		my @result = _ORIG_new($class, @_);
		
		### re-open environment and immediately retie to critical databases ###
		$forks::BerkeleyDB::bdb_env = forks::BerkeleyDB::_open_env();
		forks::BerkeleyDB::_tie_support_vars();
		
		return wantarray ? @result : $result[0];
	};

	*_ORIG_isthread = *_ORIG_isthread = \&threads::isthread;
	*threads::isthread = \&_isthread;

	sub _isthread {
		forks::BerkeleyDB::_ORIG_isthread(@_);
		forks::BerkeleyDB::CLONE();	#retie shared vars
	};
}

1;

__END__
=pod

=head1 NAME

forks::BerkeleyDB - high-performance drop-in replacement for threads

=head1 SYNOPSYS

  use forks::BerkeleyDB;

  my $thread = threads->new( sub {       # or ->create or async()
    print "Hello world from a thread\n";
  } );

  $thread->join;

  threads->detach;
  $thread->detach;

  my $tid    = $thread->tid;
  my $owntid = threads->tid;

  my $self    = threads->self;
  my $threadx = threads->object( $tidx );

  threads->yield();

  $_->join foreach threads->list;

  unless (fork) {
    threads->isthread; # intended to be used in a child-init Apache handler
  }

  use forks qw(debug);
  threads->debug( 1 );

  perl -Mforks::BerkeleyDB -Mforks::BerkeleyDB::shared threadapplication

=head1 DESCRIPTION

forks::BerkeleyDB is a drop-in replacement for threads, written as an extension of L<forks>.
The goal of this module is to improve upon the core performance of L<forks> at a level
comparable to native ithreads.

=head1 USAGE

See L<forks> for common usage information.

=head1 NOTES

Testing has been performed against BerkeleyDB 4.3.x.  Full compatibility is expected with
BDB 4.x and likely with 3.x as well.  Unclear if all tie methods are compatible with 2.x.
This module is currently not compatible with BDB 1.x.

On thread spawn, all existing bdb connections and the environment are closed prior to
the fork.  This may be an unnecessary step (e.g. it may be safe to simply re-open these
in the child after the fork) but is currently done as a precaution.

=head1 CAVIATS

Environment and database files aren't currently purged after application exits.  Files are
unlinked if they ever collide with a new process' shared vars, and care has gone into insuring
that no two running processes will ever collide, so it is not a critical issue. This will
probably be resolved in the future by storing shared var therad usage in a separate database,
and auto-purging when thus db refcount drops to 0 (in an END block to insure it cleanup
occurs as frequently as possible.

This module overrides CORE::GLOBAL::fork to insure BerkeleyDB resources are correctly managed
before and after a fork occurs.  This insures that processes will be able to safely use
threads->isthread.  You may encounter issues with your application or other modules it uses
also override CORE::GLOBAL::fork.  To work around this, you should either modify your 
CORE::GLOBAL::fork to support chaining or avoid modifying CORE::GLOBAL::fork altogether.

=head1 TODO

Use shared process shutdown to purge BerkeleyDB database files.

Implement thread joined data using BerkeleyDB.

Determine what additional functions should be migrated to BerkeleyDB backend vs. those that
should remain as part of the forks package.

=head1 AUTHOR

Eric Rybski <rybskej@yahoo.com>.

=head1 COPYRIGHT

Copyright (c) 2006 Eric Rybski <rybskej@yahoo.com>.
All rights reserved.  This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<forks>, L<threads>

=cut

1;
