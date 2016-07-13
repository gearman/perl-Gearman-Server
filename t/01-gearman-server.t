use strict;
use warnings;

use Test::More;

my $mn = qw/
    Gearman::Server
    /;

use_ok($mn);
my $gs = new_ok($mn);

can_ok $gs, qw/
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
done_testing;
