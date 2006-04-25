package forks::BerkeleyDB;

$VERSION = 0.01;
use forks (); die "forks version 0.18 required--this is only version $threads::VERSION" unless $threads::VERSION >= 0.18;

*import = *import = \&forks::import;

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
The goal of this module is to attempt to improve upon the core performance of L<forks> at a 
level comparable to native ithreads.

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
