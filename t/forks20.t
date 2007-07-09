#!/usr/local/bin/perl -T -w
BEGIN {				# Magic Perl CORE pragma
    if ($ENV{PERL_CORE}) {
        chdir 't' if -d 't';
        @INC = '../lib';
    }
}

BEGIN {delete $ENV{THREADS_DEBUG}} # no debugging during testing!

use lib '../lib';
use forks::BerkeleyDB; # must be done _before_ Test::More which loads real threads.pm
use forks::BerkeleyDB::shared;
use Config;

my ($reason,$tests,$entries);
BEGIN {
    $entries = 25;
    $tests = 3 + (3 * $entries);

    eval {require Thread::Queue};
    $reason = '';
    $reason = 'Thread::Queue not found'
     unless defined $Thread::Queue::VERSION;
    $reason ||= 'Cannot test Thread::Queue with an unthreaded Perl'
     unless $Config{'useithreads'};

    $tests = 1 if $reason;
} #BEGIN

use Test::More tests => $tests;
use strict;
use warnings;

SKIP: {
    skip $reason, $tests if $reason;

    my $q = Thread::Queue->new;
    isa_ok( $q,'Thread::Queue', "Check if object has correct type" );

#------------------------------------------------------------------------
# queueing from child thread, dequeuing from main thread

    threads->new( sub {
        $q->enqueue( 1..$entries );
    } )->join;

    is( $q->pending,$entries,"Check all $entries entries on queue" );

    foreach (1..$entries) {
        my $value = $q->dequeue;
        is( $value,$_,"Check whether '$_' gotten from queue in main" );
    }

#------------------------------------------------------------------------
# queueing from main thread, non-blocking dequeuing from child thread

    $q = Thread::Queue->new( 1..$entries );

    is( $q->pending,$entries,"Check all $entries entries on queue" );

    threads->new( sub {
        foreach (1..$entries) {
            my $value = $q->dequeue_nb;
            is( $value,$_,"Check '$_' gotten from queue in child" );
        }
    } )->join;

#------------------------------------------------------------------------
# queueing and dequeueing from child threads

    my $enqueue = threads->new( sub {
        foreach (1..$entries) {
            $q->enqueue( $_ );
        }
    } );

    my $dequeue = threads->new( sub {
        foreach (1..$entries) {
            my $value = $q->dequeue;
            is( $value,$_,"Check '$_' gotten from queue in other child" );
        }
    } );

    $enqueue->join;
    $dequeue->join;

#------------------------------------------------------------------------
} #SKIP
