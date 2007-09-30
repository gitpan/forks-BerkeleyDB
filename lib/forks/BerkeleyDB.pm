package forks::BerkeleyDB;

$VERSION = 0.052;

package
	CORE::GLOBAL;	#hide from PAUSE
use subs qw(fork);
{
	no warnings 'redefine';
	$forks::BerkeleyDB::_parent_fork = \&fork
		if defined($forks::VERSION) && $forks::VERSION >= 0.22;
	*fork = \&forks::BerkeleyDB::_fork;
}

package forks::BerkeleyDB;

use forks::BerkeleyDB::Config;
use BerkeleyDB 0.27;
use Storable qw(freeze thaw);

use constant DEBUG => forks::BerkeleyDB::Config::DEBUG();
use constant ENV_ROOT => forks::BerkeleyDB::Config::ENV_ROOT();
use constant ENV_SUBPATH => forks::BerkeleyDB::Config::ENV_SUBPATH();
use constant ENV_PATH => forks::BerkeleyDB::Config::ENV_PATH();

our $bdb_env;	#berkeleydb environment
our $bdb_locksig_env;	#berkeleydb lock/signal environment

BEGIN {
	### allow user to enable BDB locks (disabled by default) ###
	if (exists $ENV{'THREADS_BDB_LOCKS'}) {	#TODO: convert to import argument in future (i.e. lock_model => 'bdb')
		$ENV{'THREADS_BDB_LOCKS'} =~ m#^(.*)$#s;
		no warnings 'redefine';
		*USE_BDB_LOCKS = $ENV{'THREADS_BDB_LOCKS'} ? sub { 1 } : sub { 0 };
	} else {
		*USE_BDB_LOCKS = sub { 0 };
	}
}

BEGIN {
	$forks::DEFER_INIT_BEGIN_REQUIRE = 1;	#feature in forks 0.26 and later
	require forks; die "forks version 0.18 required--this is only version $forks::VERSION"
		unless defined($forks::VERSION) && $forks::VERSION >= 0.18;
	
	### set up environment characteristics ###
	*_croak = *_croak = \&threads::_croak;
	{
		no warnings 'redefine';
		*threads::_end_server_post_shutdown = *threads::_end_server_post_shutdown
			= sub {
				eval {
					forks::BerkeleyDB::_purge_env();
				};
			}
			if defined($forks::VERSION) && $forks::VERSION >= 0.23;
	}

	sub _open_env () {
		### open the base environment ###
		return $bdb_env = new BerkeleyDB::Env(
			-Home  => ENV_PATH,
			-Flags => DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL,
		) or _croak( "Can't create BerkeleyDB::Env (home=".ENV_PATH."): $BerkeleyDB::Error" );
		if (USE_BDB_LOCKS) {
			return $bdb_locksig_env = new BerkeleyDB::Env(
				-Home  => ENV_PATH_LOCKSIG,
				-Flags => DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL,
			) or _croak( "Can't create BerkeleyDB::Env (home=".ENV_PATH_LOCKSIG."): $BerkeleyDB::Error" );
		}
	}

	sub _close_env () {
		### close and undefine the base environment ###
		$bdb_env->close() if defined $bdb_env && UNIVERSAL::isa($_[0], 'BerkeleyDB::Env');
		$bdb_env = undef;
	}

	sub _purge_env (;$) {
		my @env_dirs = @_ ? @_ : (ENV_PATH, (USE_BDB_LOCKS() ? ENV_PATH_LOCKSIG : ()));
		foreach my $env_dir (@env_dirs) {
			opendir(ENVDIR, $env_dir);
			my @files_to_del = reverse grep(!/^(\.|\.\.)$/, readdir(ENVDIR));
			closedir(ENVDIR);
			warn "unlinking: ".join(', ', map("$env_dir/$_", @files_to_del)) if DEBUG;
			foreach (@files_to_del) {
				my $file = "$env_dir/$_";
				$file =~ m/^([\/-\@\w_.]+)$/so;	#untaint
				_croak( "Unable to unlink file '$1'. Please manually remove this file." ) unless unlink $1;
			}
		}
	}

	sub _tie_support_vars () {

	}

	sub _untie_support_vars () {

	}
	
	sub _fork {
		### safely sync & close databases, close environment ###
		_untie_support_vars();
		_close_env();
		
		### do the fork ###
		my $pid = defined($_parent_fork) ? $_parent_fork->() : CORE::fork;

		if (!defined $pid || $pid) { #in parent
			### re-open environment and immediately retie to critical databases ###
			_open_env();
			_tie_support_vars();
		}
				
		return $pid;
	};
	
	*import = *import = \&forks::import;

	### create/purge necessary paths to create clean environment ###
	my @env_dirs = (ENV_PATH, (USE_BDB_LOCKS() ? ENV_PATH_LOCKSIG : ()));
	foreach my $env_dir (@env_dirs) {
		if (-d $env_dir) {
			_purge_env($env_dir);
		}
		else {
			my $curpath = '';
			foreach (split(/\//o, $env_dir)) {
				$curpath .= $_ eq '' ? '/' : "$_/";
				unless (-d $curpath) {
					my $status = mkdir $curpath, 0777;
					_croak( "Can't create directory ".ENV_ROOT.': '.$! ) unless $status || -d $curpath;
				}
				chmod 0777, $curpath;
			}
		}
	}

	### create the base environment ###
	_open_env();
	_tie_support_vars();
}

END {
	eval { _untie_support_vars(); };
	eval { _close_env(); };
	#also remove database if no threads connected to any databases (maybe use recno DB to monitor num of threads connected per shared var)?
}

sub CLONE {	#reopen environment and immediately retie to critical databases
	_open_env();
	_tie_support_vars();
}

1;

__END__
=pod

=head1 NAME

forks::BerkeleyDB - high-performance drop-in replacement for threads

=head1 VERSION

This documentation describes version 0.052.

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

=head1 REQUIRED MODULES

 BerkeleyDB (0.27)
 Devel::Required (0.07)
 forks (0.23)
 Storable (any)
 Tie::Restore (0.11)

=head1 USAGE

See L<forks> for common usage information.

=head1 NOTES

If you have forks.pm 0.23 or later installed, all database files created during runtime
will be automatically purged when the main thread exits.  If you have created a large number
of shared variables, you may experience a slight delay during process exit.  Note that these
files may not be cleaned up if the main thread or process group is terminated using SIGKILL,
although existance of these files after exit should not have an adverse affect on other
currently running or future forks::BerkeleyDB processes.

Testing has been performed against BerkeleyDB 4.3.x.  Full compatibility is expected with
BDB 4.x and likely with 3.x as well.  Unclear if all tie methods are compatible with 2.x.
This module is currently not compatible with BDB 1.x.

=head1 CAVIATS

This module defines CORE::GLOBAL::fork to insure BerkeleyDB resources are correctly managed
before and after a fork occurs.  This insures that processes will be able to safely use
threads->isthread.  You may encounter issues with your application or other modules it uses
also define CORE::GLOBAL::fork.  To work around this, you should modify your CORE::GLOBAL::fork
to support chaining, like the following

	use subs 'fork';
	*_oldfork = \&CORE::GLOBAL::fork;
	sub fork {
		#your code here
		...
		_oldfork->() if ref(*oldfork) eq 'SUB';
	}

=head1 TODO

Implement thread joined data using BerkeleyDB.

Determine what additional functions should be migrated to BerkeleyDB backend vs. those that
should remain as part of the forks package.

Add a high security mode, where all BerkeleyDB data is encrypted using either
native encryption (preferred, if available) or an external cryptography module
of the user's choice (i.e. Crypt::* interface module, or something that
supports a standard interface given an object instance).

Consider porting all shared variable tied class support into package classes,
instead of depending on BerkeleyDB module parent classes for some methods, to
insure method behavior consistency no matter which BerkeleyDB.pm version is installed.

Consider merging shared scalars into one or more BDB recno tables, to minimize
use of environment locks and database files (at the cost of write cursor performance,
if multiple threads attempting to write to different SVs in same physical table).

Consider rewriting all SV actions to use write cursor (unless complete action is already
atomic in BDB API) to insure perltie actions are atomic in nature.  Intention is to allow
use of SV without always requiring a lock (for apps that require highest possible
concurrency).

Consider implementing "atomic" shared variable classes, which allow all non-iterative
operations to be atomic without locks.  This would require overload of all math and
string operators.  Hopefully this will be enabled with an attribute, such
as 'sharedatomic'.  I don't believe this can be achieved with perltie, so only non-blessed
primitives would be allowed for scalars.

May need to enable DB_ENV->failchk when shared var process detects that a thread
has unexpectedly exited.  If return value is DB_RUNRECOVERY, then we likely need
to terminate the entire application (as the shared bdb environment is no longer
guaranteed to be stable.

Consider using bdb txn subsystem environment for locking and signaling. Theoretically,
this should require: 1 recno for locks (idx=sid, value=tid holding lock), 1 recno for waiting
(idx=sid, value=[list if tid waiting]), and N queue for signaling (1 per thread; thread block 
on own queue to wimulate waiting; push from other source acts as signal).  Txn would be
used on locks and waiting recno databases (locking individual elements with cursors). Deadlock
detection could be enabled using BDB deadlock detection engine.  Would need hooks into deadlock
detection forks.pm interface.

=head1 CAVIATS

It appears that BerkeleyDB libdb 4.4.x environments are not fully thread-safe
with BerkeleyDB CDB mode on some platforms.  Thus, it is highly recommended you
use libdb 4.3.x and earlier, or 4.5.x and later.

=head1 AUTHOR

Eric Rybski <rybskej@yahoo.com>.

=head1 COPYRIGHT

Copyright (c) 2006-2007 Eric Rybski <rybskej@yahoo.com>.
All rights reserved.  This program is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<forks>, L<threads>

=cut

1;
