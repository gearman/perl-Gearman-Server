use strict;
use warnings;

use IO::Socket::INET;
use Net::EmptyPort qw/ empty_port /;
use Socket qw/
    IPPROTO_TCP
    SOCK_STREAM
    /;
use Test::More;

my $mn = "Gearman::Server::Client";
use_ok("Gearman::Server");
use_ok($mn);
isa_ok($mn, "Danga::Socket");

can_ok(
    $mn, qw/
        CMD_can_do
        CMD_can_do_timeout
        CMD_cant_do
        CMD_echo_req
        CMD_get_status
        CMD_grab_job
        CMD_option_req
        CMD_pre_sleep
        CMD_reset_abilities
        CMD_set_client_id
        CMD_submit_job
        CMD_submit_job_bg
        CMD_submit_job_high
        CMD_work_complete
        CMD_work_exception
        CMD_work_fail
        CMD_work_status
        TXTCMD_clients
        TXTCMD_gladiator
        TXTCMD_jobs
        TXTCMD_maxqueue
        TXTCMD_status
        TXTCMD_version
        TXTCMD_workers
        _cmd_submit_job
        _setup_can_do_list
        close
        error_packet
        eurl
        event_err
        event_hup
        event_read
        event_write
        option
        process_line
        process_cmd
        res_packet
        /
);
my ($gs, $gc) = (new_ok("Gearman::Server"));

subtest "new", sub {
    my $port = empty_port();
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
    $gc = new_ok($mn, [$sock, $gs]);

    foreach (qw/fast_buffer can_do_list/) {
        isa_ok($gc->{$_}, "ARRAY", $_) && is(@{ $gc->{$_} }, 0, "$_ empty");
    }

    foreach (qw/can_do doing options/) {
        isa_ok($gc->{$_}, "HASH", $_)
            && is(keys(%{ $gc->{$_} }), 0, "$_ empty");
    }

    foreach (qw/sleeping can_do_iter jobs_done_since_sleep/) {
        is($gc->{$_}, 0, "$_ = 0");
    }

    is($gc->{fast_read}, undef, "fast_read");
    is($gc->{read_buf},  '',    "read_buf");
    is($gc->{client_id}, '-',   "client_id");
    is($gc->{server},    $gs,   "server");
};

done_testing;

