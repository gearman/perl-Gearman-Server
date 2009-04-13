#!/usr/bin/perl

=head1 NAME

gearmand - Gearman client/worker connector.

=head1 SYNOPSIS

 gearmand --daemon

=head1 DESCRIPTION

This is the main executable for L<Gearman::Server>.  It provides
command-line configuration of port numbers, pidfiles, and
daemonization.

=head1 OPTIONS

=over

=item --daemonize / -d

Make the daemon run in the background (good for init.d scripts, bad
for running under daemontools/supervise).

=item --port=7003 / -p 7003

Set the port number, defaults to 7003.

=item --pidfile=/some/dir/gearmand.pid

Write a pidfile when starting up

=item --debug=1

Enable debugging (currently the only debug output is when a client or worker connects).

=item --accept=10

Number of new connections to accept each time we see a listening socket ready. This doesn't usually
need to be tuned by anyone, however in dire circumstances you may need to do it quickly.

=item --wakeup=3

Number of workers to wake up per job inserted into the queue.

Zero (0) is a perfectly acceptable answer, and can be used if you don't care much about job latency.
This would bank on the base idea of a worker checking in with the server every so often.

Negative One (-1) indicates that all sleeping workers should be woken up.

All other negative numbers will cause the server to throw exception and not start.

=item --wakeup-delay=

Time interval before waking up more workers (the value specified by --wakeup) when jobs are still in
the queue.

Zero (0) means go as fast as possible, but not all at the same time. Similar to -1 on --wakeup, but
is more cooperative in gearmand's multitasking model.

Negative One (-1) means that this event won't happe, so only the initial workers will be woken up to
handle jobs in the queue.

=back

=head1 COPYRIGHT

Copyright 2005-2007, Danga Interactive

You are granted a license to use it under the same terms as Perl itself.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

Brad Whitaker <whitaker@danga.com>

=head1 SEE ALSO

L<Gearman::Server>

L<Gearman::Client>

L<Gearman::Worker>

L<Gearman::Client::Async>

=cut

package Gearmand;
use strict;
use warnings;
BEGIN {
    $^P = 0x200;  # Provide informative names to anonymous subroutines
}
use FindBin;
use lib "$FindBin::Bin/lib";
use Gearman::Server;

use Getopt::Long;
use Carp;
use Danga::Socket 1.52;
use IO::Socket::INET;
use POSIX ();
use Gearman::Util;
use vars qw($DEBUG);
use Scalar::Util ();

$DEBUG = 0;

my (
    $daemonize,
    $nokeepalive,
    $notify_pid,
    $opt_pidfile,
    $accept,
    $wakeup,
    $wakeup_delay,
   );
my $conf_port = 7003;

Getopt::Long::GetOptions(
                         'd|daemonize'    => \$daemonize,
                         'p|port=i'       => \$conf_port,
                         'debug=i'        => \$DEBUG,
                         'pidfile=s'      => \$opt_pidfile,
                         'accept=i'       => \$accept,
                         'wakeup=i'       => \$wakeup,
                         'wakeup-delay=f' => \$wakeup_delay,
                         'notifypid|n=i'  => \$notify_pid,  # for test suite only.
                         );

daemonize() if $daemonize;

# true if we've closed listening socket, and we're waiting for a
# convenient place to kill the process
our $graceful_shutdown = 0;

$SIG{'PIPE'} = "IGNORE";  # handled manually

my $server = Gearman::Server->new(
                                  wakeup          => $wakeup,
                                  wakeup_delay    => $wakeup_delay,
             );
my $ssock  = $server->create_listening_sock($conf_port, accept_per_loop => $accept);

if ($opt_pidfile) {
    open my $fh, '>', $opt_pidfile or die "Could not open $opt_pidfile: $!";
    print $fh "$$\n";
    close $fh;
}

sub shutdown_graceful {
    return if $graceful_shutdown;

    my $ofds = Danga::Socket->OtherFds;
    delete $ofds->{fileno($ssock)};
    $ssock->close;
    $graceful_shutdown = 1;
    shutdown_if_calm();
}

sub shutdown_if_calm {
    exit 0 unless $server->jobs_outstanding;
}

sub daemonize {
    my ($pid, $sess_id, $i);

    ## Fork and exit parent
    if ($pid = fork) { exit 0; }

    ## Detach ourselves from the terminal
    croak "Cannot detach from controlling terminal"
        unless $sess_id = POSIX::setsid();

    ## Prevent possibility of acquiring a controling terminal
    $SIG{'HUP'} = 'IGNORE';
    if ($pid = fork) { exit 0; }

    ## Change working directory
    chdir "/";

    ## Clear file creation mask
    umask 0;

    ## Close open file descriptors
    close(STDIN);
    close(STDOUT);
    close(STDERR);

    ## Reopen stderr, stdout, stdin to /dev/null
    open(STDIN,  "+>/dev/null");
    open(STDOUT, "+>&STDIN");
    open(STDERR, "+>&STDIN");
}

kill 'USR1', $notify_pid if $notify_pid;
Danga::Socket->EventLoop();

# Local Variables:
# mode: perl
# c-basic-indent: 4
# indent-tabs-mode: nil
# End:
