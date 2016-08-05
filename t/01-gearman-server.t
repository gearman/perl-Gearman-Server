use strict;
use warnings;

use Test::More;
use Test::Exception;
use Sys::Hostname ();

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

    throws_ok { $mn->new(foo => 1) } qr/Unknown options/,
            "Unknown options";
};

subtest "create_listening_sock", sub {
    my $gs   = new_ok($mn);
    my $port = 12345;
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

done_testing;
