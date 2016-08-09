use strict;
use warnings;

use IO::Socket::INET;
use Test::More;
use Test::Exception;
use Sys::Hostname ();

use Socket qw/
    IPPROTO_TCP
    SOL_SOCKET
    SOCK_STREAM
    AF_UNIX
    SOCK_STREAM
    PF_UNSPEC
    /;

my $mn = qw/
    Gearman::Server
    /;

use_ok($mn);

can_ok $mn, qw/
    create_listening_sock
    new_client
    note_disconnected_client
    clients
    to_inprocess_server
    start_worker
    enqueue_job
    wake_up_sleepers
    _wake_up_some
    on_client_sleep
    jobs_outstanding
    jobs
    job_by_handle
    note_job_finished
    set_max_queue
    new_job_handle
    job_of_unique
    set_unique_job
    grab_job
    /;

subtest "new", sub {
    my $gs = new_ok($mn);

    my @khr = qw/
        client_map
        sleepers
        sleepers_list
        job_queue
        job_of_handle
        max_queue
        job_of_uniq
        wakeup_timers
        /;

    foreach (@khr) {
        is(ref($gs->{$_}), "HASH", join "->", $mn, "{$_} is hash ref")
            && is(keys(%{ $gs->{$_} }), 0, join "->", $mn, "{$_} empty");
    }

    is(ref($gs->{listeners}),
        "ARRAY", join "->", $mn, "{listeners} is array ref");

    # && is(@{$gs->{listeners}}, 0, join "->", $mn, "{listeners} empty")

    is($gs->{wakeup},       3,  "wakeup 3");
    is($gs->{wakeup_delay}, .1, "wakeup_delay .1");
    is($gs->{handle_ct},    0,  "handle_ct");
    is($gs->{handle_base}, "H:" . Sys::Hostname::hostname() . ":",
        "handle_base");

    $gs = new_ok($mn, [wakeup => -1, wakeup_delay => -1]);
    is($gs->{wakeup}, -1, "wakeup -1");
    is($gs->{wakeup}, -1, "wakeup_delay -1");

    for (qw/wakeup wakeup_delay/) {
        throws_ok { $mn->new($_ => -2) } qr/Invalid value passed in $_ option/,
            "Invalid value passed in $_ option";
    }

    throws_ok { $mn->new(foo => 1) } qr/Unknown options/, "Unknown options";
};

subtest "create listening sock/new client", sub {
    my $port = _free_port();
    $port || plan skip_all => "couldn't find free port";
    my $gs = new_ok($mn);
    my ($accept, $la);
    ok(
        my $sock = $gs->create_listening_sock(
            $port,
            accept_per_loop => $accept,
            local_addr      => $la
        )
    );
    isa_ok($sock, "IO::Socket::INET");
};

subtest "client", sub {
    my ($port, $la) = (_free_port());
    $port || plan skip_all => "couldn't find free port";

    my $sock = new_ok(
        "IO::Socket::INET",
        [
            LocalPort => $port,
            Type      => SOCK_STREAM,
            Proto     => IPPROTO_TCP,
            Blocking  => 0,
            Reuse     => 1,
            Listen    => 1024,
        ]
    );

    my $gs = new_ok($mn);
    ok(my $nc = $gs->new_client($sock), "new_client");
    isa_ok($nc, "Gearman::Server::Client");
    ok(my @cl = $gs->clients, "clients");
    is(@cl,    1,   "clients count");
    is($cl[0], $nc, "same client");
    ok($gs->note_disconnected_client($nc), "note_disocnnected_client");
};

subtest "maxqueue", sub {
    my $gs = new_ok($mn);
    my ($f, $c) = ("foo", int(rand(5) + 1));
    ok($gs->set_max_queue($f, $c), "set_max_queue($f, $c)");
    is($gs->{max_queue}{$f}, $c, "max_queue $f = $c");
    $c = 0;
    ok($gs->set_max_queue($f, $c), "set_max_queue($f, $c)");
    is($gs->{max_queue}{$f}, undef, "max_queue $f = $c");
};

done_testing;

sub _free_port {
    my ($la, $port) = shift;
    my ($type, $retry, $sock) = ("tcp", 5);
    $la ||= "127.0.0.1";
    do {
        unless ($port) {
            $port = int(rand(10_000)) + int(rand(30_000));
        }

        IO::Socket::INET->new(
            LocalAddr => $la,
            LocalPort => $port,
            Proto     => $type,
            ReuseAddr => 1
        ) or undef($port);

    } until ($port || --$retry == 0);

    return;
    return $port;
} ## end sub _free_port

