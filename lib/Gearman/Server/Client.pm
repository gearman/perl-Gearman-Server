package Gearman::Server::Client;

=head1 NAME

Gearman::Server::Client

=head1 NAME

Used by L<Gearman::Server> to instantiate connections from clients.
Clients speak either a binary protocol, for normal operation (calling
functions, grabbing function call requests, returning function values,
etc), or a text-based line protocol, for relatively rare
administrative / monitoring commands.

The binary protocol commands aren't currently documented. (FIXME) But
they're well-implemented in L<Gearman::Client>, L<Gearman::Worker>,
and L<Gearman::Client::Async>, if that's any consolation.

The line-based administrative commands are documented below.

=cut

use strict;
use Danga::Socket;
use base 'Danga::Socket';
use fields (
            'can_do',  # { $job_name => $timeout } $timeout can be undef indicating no timeout
            'can_do_list',
            'can_do_iter',
            'fast_read',
            'fast_buffer',
            'read_buf',
            'sleeping',   # 0/1:  they've said they're sleeping and we haven't woken them up
            'timer', # Timer for job cancellation
            'doing',  # { $job_handle => Job }
            'client_id',  # opaque string, no whitespace.  workers give this so checker scripts
                          # can tell apart the same worker connected to multiple jobservers.
            'server',     # pointer up to client's server
            'options',
            'jobs_done_since_sleep',
            );

# 60k read buffer default, similar to perlbal's backend read.
use constant READ_SIZE => 60 * 1024;
use constant MAX_READ_SIZE => 512 * 1024;

# Class Method:
sub new {
    my Gearman::Server::Client $self = shift;
    my ($sock, $server) = @_;
    $self = fields::new($self) unless ref $self;
    $self->SUPER::new($sock);

    $self->{fast_read}   = undef; # Number of bytes to read as fast as we can (don't try to process them)
    $self->{fast_buffer} = []; # Array of buffers used during fast read operation
    $self->{read_buf}    = '';
    $self->{sleeping}    = 0;
    $self->{can_do}      = {};
    $self->{doing}       = {}; # handle -> Job
    $self->{can_do_list} = [];
    $self->{can_do_iter} = 0;  # numeric iterator for where we start looking for jobs
    $self->{client_id}   = "-";
    $self->{server}      = $server;
    $self->{options}     = {};
    $self->{jobs_done_since_sleep} = 0;

    return $self;
}

sub option {
    my Gearman::Server::Client $self = shift;
    my $option = shift;

    return $self->{options}->{$option};
}

sub close {
    my Gearman::Server::Client $self = shift;

    my $doing = $self->{doing};

    while (my ($handle, $job) = each %$doing) {
        my $msg = Gearman::Util::pack_res_command("work_fail", $handle);
        $job->relay_to_listeners($msg);
        $job->note_finished(0);
    }

    # Clear the doing list, since it may contain a set of jobs which contain
    # references back to us.
    %$doing = ();

    # Remove self from sleepers, otherwise it will be leaked if another worker
    # for the job never connects.
    my $sleepers = $self->{server}{sleepers};
    my $sleepers_list = $self->{server}{sleepers_list};
    for my $job (@{ $self->{can_do_list} }) {
        my $sleeping = $sleepers->{$job};
        delete $sleeping->{$self};

        my $new_sleepers_list;
        for my $client (@{$sleepers_list->{$job}}) {
            next unless $client;
            push @{$new_sleepers_list}, $client unless $sleeping->{$client};
        }
        if ($new_sleepers_list) {
            $self->{server}{sleepers_list}->{$job} = $new_sleepers_list;
        } else {
            delete $self->{server}{sleepers_list}->{$job};
        }

        delete $sleepers->{$job} unless %$sleeping;
    }

    $self->{server}->note_disconnected_client($self);

    $self->CMD_reset_abilities;

    $self->SUPER::close;
}

# Client
sub event_read {
    my Gearman::Server::Client $self = shift;

    my $read_size = $self->{fast_read} || READ_SIZE;
    my $bref = $self->read($read_size);

    # Delay close till after buffers are written on EOF. If we are unable
    # to write 'err' or 'hup' will be thrown and we'll close faster.
    return $self->write(sub { $self->close } ) unless defined $bref;

    if ($self->{fast_read}) {
        push @{$self->{fast_buffer}}, $$bref;
        $self->{fast_read} -= length($$bref);

        # If fast_read is still positive, then we need to read more data
        return if ($self->{fast_read} > 0);

        # Append the whole giant read buffer to our main read buffer
        $self->{read_buf} .= join('', @{$self->{fast_buffer}});

        # Reset the fast read state for next time.
        $self->{fast_buffer} = [];
        $self->{fast_read} = undef;
    } else {
        # Exact read size length likely means we have more sitting on the
        # socket. Buffer up to half a meg in one go.
        if (length($$bref) == READ_SIZE) {
            my $limit = int(MAX_READ_SIZE / READ_SIZE);
            my @crefs = ($$bref);
            while (my $cref = $self->read(READ_SIZE)) {
                push(@crefs, $$cref);
                last if (length($$cref) < READ_SIZE || $limit-- < 1);
            }
            $bref = \join('', @crefs);
        }
        $self->{read_buf} .= $$bref;
    }

    my $found_cmd;
    do {
        $found_cmd = 1;
        my $blen = length($self->{read_buf});

        if ($self->{read_buf} =~ /^\0REQ(.{8,8})/s) {
            my ($cmd, $len) = unpack("NN", $1);
            if ($blen < $len + 12) {
                # Start a fast read loop to get all the data we need, less
                # what we already have in the buffer.
                $self->{fast_read} = $len + 12 - $blen;
                return;
            }

            $self->process_cmd($cmd, substr($self->{read_buf}, 12, $len));

            # and slide down buf:
            $self->{read_buf} = substr($self->{read_buf}, 12+$len);

        } elsif ($self->{read_buf} =~ s/^(\w.+?)?\r?\n//) {
            # ASCII command case (useful for telnetting in)
            my $line = $1;
            $self->process_line($line);
        } else {
            $found_cmd = 0;
        }
    } while ($found_cmd);
}

sub event_write {
    my $self = shift;
    my $done = $self->write(undef);
    $self->watch_write(0) if $done;
}

# Line based command processor
sub process_line {
    my Gearman::Server::Client $self = shift;
    my $line = shift;

    if ($line && $line =~ /^(\w+)\s*(.*)/) {
        my ($cmd, $args) = ($1, $2);
        $cmd = lc($cmd);
        my $code = $self->can("TXTCMD_$cmd");
        if ($code) {
            $code->($self, $args);
            return;
        }
    }

    return $self->err_line('unknown_command');
}

=head1 Binary Protocol Structure

All binary protocol exchanges between clients (which can be callers,
workers, or both) and the Gearman server have common packet header:

  4 byte magic  -- either "\0REQ" for requests to the server, or
                   "\0RES" for responses from the server
  4 byte type   -- network order integer, representing the packet type
  4 byte length -- network order length, for data segment.
  data          -- optional, if length is non-zero

=head1 Binary Protocol Commands

=head2 echo_req (type=16)

A debug command.  The server will reply with the same data, in a echo_res (type=17) packet.

=head2 (and many more...)

FIXME: auto-generate protocol docs from internal Gearman::Util table,
once annotated with some English?

=cut

sub CMD_echo_req {
    my Gearman::Server::Client $self = shift;
    my $blobref = shift;

    return $self->res_packet("echo_res", $$blobref);
}

sub CMD_work_status {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;
    my ($handle, $nu, $de) = split(/\0/, $$ar);

    my $job = $self->{doing}{$handle};
    return $self->error_packet("not_worker") unless $job && $job->worker == $self;

    my $msg = Gearman::Util::pack_res_command("work_status", $$ar);
    $job->relay_to_listeners($msg);
    $job->status([$nu, $de]);
    return 1;
}

sub CMD_work_complete {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    $$ar =~ s/^(.+?)\0//;
    my $handle = $1;

    my $job = delete $self->{doing}{$handle};
    return $self->error_packet("not_worker") unless $job && $job->worker == $self;

    my $msg = Gearman::Util::pack_res_command("work_complete", join("\0", $handle, $$ar));
    $job->relay_to_listeners($msg);
    $job->note_finished(1);
    if (my $timer = $self->{timer}) {
        $timer->cancel;
        $self->{timer} = undef;
    }

    return 1;
}

sub CMD_work_fail {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;
    my $handle = $$ar;
    my $job = delete $self->{doing}{$handle};
    return $self->error_packet("not_worker") unless $job && $job->worker == $self;

    my $msg = Gearman::Util::pack_res_command("work_fail", $handle);
    $job->relay_to_listeners($msg);
    $job->note_finished(1);
    if (my $timer = $self->{timer}) {
        $timer->cancel;
        $self->{timer} = undef;
    }

    return 1;
}

sub CMD_work_exception {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    $$ar =~ s/^(.+?)\0//;
    my $handle = $1;
    my $job = $self->{doing}{$handle};

    return $self->error_packet("not_worker") unless $job && $job->worker == $self;

    my $msg = Gearman::Util::pack_res_command("work_exception", join("\0", $handle, $$ar));
    $job->relay_to_option_listeners($msg, "exceptions");

    return 1;
}

sub CMD_pre_sleep {
    my Gearman::Server::Client $self = shift;
    $self->{'sleeping'} = 1;
    $self->{server}->on_client_sleep($self);
    return 1;
}

sub CMD_grab_job {
    my Gearman::Server::Client $self = shift;

    my $job;
    my $can_do_size = scalar @{$self->{can_do_list}};

    unless ($can_do_size) {
        $self->res_packet("no_job");
        return;
    }

    # the offset where we start asking for jobs, to prevent starvation
    # of some job types.
    $self->{can_do_iter} = ($self->{can_do_iter} + 1) % $can_do_size;

    my $tried = 0;
    while ($tried < $can_do_size) {
        my $idx = ($tried + $self->{can_do_iter}) % $can_do_size;
        $tried++;
        my $job_to_grab = $self->{can_do_list}->[$idx];
        $job = $self->{server}->grab_job($job_to_grab)
            or next;

        $job->worker($self);
        $self->{doing}{$job->handle} = $job;

        my $timeout = $self->{can_do}->{$job_to_grab};
        if (defined $timeout) {
            my $timer = Danga::Socket->AddTimer($timeout, sub {
                return $self->error_packet("not_worker") unless $job->worker == $self;

                my $msg = Gearman::Util::pack_res_command("work_fail", $job->handle);
                $job->relay_to_listeners($msg);
                $job->note_finished(1);
                $job->clear_listeners;
                $self->{timer} = undef;
            });
            $self->{timer} = $timer;
        }
        return $self->res_packet("job_assign",
                                 join("\0",
                                      $job->handle,
                                      $job->func,
                                      ${$job->argref},
                                      ));
    }

    $self->res_packet("no_job");
}

sub CMD_can_do {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    $self->{can_do}->{$$ar} = undef;
    $self->_setup_can_do_list;
}

sub CMD_can_do_timeout {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    my ($task, $timeout) = $$ar =~ m/([^\0]+)(?:\0(.+))?/;

    if (defined $timeout) {
        $self->{can_do}->{$task} = $timeout;
    } else {
        $self->{can_do}->{$task} = undef;
    }

    $self->_setup_can_do_list;
}

sub CMD_option_req {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    my $success = sub {
        return $self->res_packet("option_res", $$ar);
    };

    if ($$ar eq 'exceptions') {
        $self->{options}->{exceptions} = 1;
        return $success->();
    }

    return $self->error_packet("unknown_option");
}

sub CMD_set_client_id {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    $self->{client_id} = $$ar;
    $self->{client_id} =~ s/\s+//g;
    $self->{client_id} = "-" unless length $self->{client_id};
}

sub CMD_cant_do {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;

    delete $self->{can_do}->{$$ar};
    $self->_setup_can_do_list;
}

sub CMD_get_status {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;
    my $job = $self->{server}->job_by_handle($$ar);

    # handles can't contain nulls
    return if $$ar =~ /\0/;

    my ($known, $running, $num, $den);
    $known = 0;
    $running = 0;
    if ($job) {
        $known = 1;
        $running = $job->worker ? 1 : 0;
        if (my $stat = $job->status) {
            ($num, $den) = @$stat;
        }
    }

    $num = '' unless defined $num;
    $den = '' unless defined $den;

    $self->res_packet("status_res", join("\0",
                                         $$ar,
                                         $known,
                                         $running,
                                         $num,
                                         $den));
}

sub CMD_reset_abilities {
    my Gearman::Server::Client $self = shift;

    $self->{can_do} = {};
    $self->_setup_can_do_list;
}

sub _setup_can_do_list {
    my Gearman::Server::Client $self = shift;
    $self->{can_do_list} = [ keys %{$self->{can_do}} ];
    $self->{can_do_iter} = 0;
}

sub CMD_submit_job    {  push @_, 1; &_cmd_submit_job; }
sub CMD_submit_job_bg {  push @_, 0; &_cmd_submit_job; }
sub CMD_submit_job_high {  push @_, 1, 1; &_cmd_submit_job; }

sub _cmd_submit_job {
    my Gearman::Server::Client $self = shift;
    my $ar = shift;
    my $subscribe = shift;
    my $high_pri = shift;

    return $self->error_packet("invalid_args", "No func/uniq header [$$ar].")
        unless $$ar =~ s/^(.+?)\0(.*?)\0//;

    my ($func, $uniq) = ($1, $2);

    my $job = Gearman::Server::Job->new($self->{server}, $func, $uniq, $ar, $high_pri);

    if ($subscribe) {
        $job->add_listener($self);
    } else {
        # background mode
        $job->require_listener(0);
    }

    $self->res_packet("job_created", $job->handle);
    $self->{server}->wake_up_sleepers($func);
}

sub res_packet {
    my Gearman::Server::Client $self = shift;
    my ($code, $arg) = @_;
    $self->write(Gearman::Util::pack_res_command($code, $arg));
    return 1;
}

sub error_packet {
    my Gearman::Server::Client $self = shift;
    my ($code, $msg) = @_;
    $self->write(Gearman::Util::pack_res_command("error", "$code\0$msg"));
    return 0;
}

sub process_cmd {
    my Gearman::Server::Client $self = shift;
    my $cmd = shift;
    my $blob = shift;

    my $cmd_name = "CMD_" . Gearman::Util::cmd_name($cmd);
    my $ret = eval {
        $self->$cmd_name(\$blob);
    };
    return $ret unless $@;
    warn "Error: $@\n";
    return $self->error_packet("server_error", $@);
}

sub event_err { my $self = shift; $self->close; }
sub event_hup { my $self = shift; $self->close; }

############################################################################

=head1 Line based commands

These commands are used for administrative or statistic tasks to be done on the gearman server. They can be entered using a line based client (telnet, etc.) by connecting to the listening port (7003) and are also intended to be machine parsable.

=head2 "workers"

Emits list of registered workers, their fds, IPs, client ids, and list of registered abilities (function names they can do).  Of format:

  fd ip.x.y.z client_id : func_a func_b func_c
  fd ip.x.y.z client_id : func_a func_b func_c
  fd ip.x.y.z client_id : func_a func_b func_c
  .

It ends with a line with just a period.

=cut

sub TXTCMD_workers {
    my Gearman::Server::Client $self = shift;

    foreach my $cl (sort { $a->{fd} <=> $b->{fd} } $self->{server}->clients) {
        my $fd = $cl->{fd};
        $self->write("$fd " . $cl->peer_ip_string . " $cl->{client_id} : @{$cl->{can_do_list}}\n");

    }
    $self->write(".\n");
}

=head2 "status"

The output format of this function is tab separated columns as follows, followed by a line consisting of a fullstop and a newline (".\n") to indicate the end of output.

=over

=item Function name

A string denoting the name of the function of the job

=item Number in queue

A positive integer indicating the total number of jobs for this function in the queue. This includes currently running ones as well (next column)

=item Number of jobs running

A positive integer showing how many jobs of this function are currently running

=item Number of capable workers

A positive integer denoting the maximum possible count of workers that could be doing this job. Though they may not all be working on it due to other tasks holding them busy.

=back

=cut

sub TXTCMD_status {
    my Gearman::Server::Client $self = shift;

    my %funcs; # func -> 1  (set of all funcs to display)

    # keep track of how many workers can do which functions
    my %can;
    foreach my $client ($self->{server}->clients) {
        foreach my $func (@{$client->{can_do_list}}) {
            $can{$func}++;
            $funcs{$func} = 1;
        }
    }

    my %queued_funcs;
    my %running_funcs;

    foreach my $job ($self->{server}->jobs) {
        my $func = $job->func;
        $queued_funcs{$func}++;
        if ($job->worker) {
            $running_funcs{$func}++;
        }
    }

    # also include queued functions (even if there aren't workers)
    # in our list of funcs to show.
    $funcs{$_} = 1 foreach keys %queued_funcs;

    foreach my $func (sort keys %funcs) {
        my $queued  = $queued_funcs{$func}  || 0;
        my $running = $running_funcs{$func} || 0;
        my $can     = $can{$func}           || 0;
        $self->write( "$func\t$queued\t$running\t$can\n" );
    }

    $self->write( ".\n" );
}

=head2 "jobs"

Output format is zero or more lines of:

    [Job function name]\t[Uniq (coalescing) key]\t[Worker address]\t[Number of listeners]\n

Follows by a single line of:

    .\n

\t is a literal tab character
\n is perl's definition of newline (literal \n on linux, something else on win32)

=cut

sub TXTCMD_jobs {
    my Gearman::Server::Client $self = shift;

    foreach my $job ($self->{server}->jobs) {
        my $func = $job->func;
        my $uniq = $job->uniq;
        my $worker_addr = "-";

        if (my $worker = $job->worker) {
            $worker_addr = $worker->peer_addr_string;
        }

        my $listeners = $job->listeners;

        $self->write("$func\t$uniq\t$worker_addr\t$listeners\n");
    }

    $self->write(".\n");
}

=head2 "clients"

Output format is zero or more sections of:

=over

One line of:

    [Client Address]\n

Followed by zero or more lines of:

    \t[Job Function]\t[Uniq (coalescing) key]\t[Worker Address]\n

=back

Follows by a single line of:

    .\n

\t is a literal tab character
\n is perl's definition of newline (literal \n on linux, something else on win32)

=cut

sub TXTCMD_clients {
    my Gearman::Server::Client $self = shift;

    my %jobs_by_client;

    foreach my $job ($self->{server}->jobs) {
        foreach my $client ($job->listeners) {
            my $ent = $jobs_by_client{$client} ||= [];
            push @$ent, $job;
        }
    }

    foreach my $client ($self->{server}->clients) {
        my $client_addr = $client->peer_addr_string;
        $self->write("$client_addr\n");
        my $jobs = $jobs_by_client{$client} || [];

        foreach my $job (@$jobs) {
            my $func = $job->func;
            my $uniq = $job->uniq;
            my $worker_addr = "-";

            if (my $worker = $job->worker) {
                $worker_addr = $worker->peer_addr_string;
            }
            $self->write("\t$func\t$uniq\t$worker_addr\n");
        }

    }

    $self->write(".\n");
}

sub TXTCMD_gladiator {
    my Gearman::Server::Client $self = shift;
    my $args = shift || "";
    my $has_gladiator = eval "use Devel::Gladiator; use Devel::Peek; 1;";
    if ($has_gladiator) {
        my $all = Devel::Gladiator::walk_arena();
        my %ct;
        foreach my $it (@$all) {
            $ct{ref $it}++;
            if (ref $it eq "CODE") {
                my $name = Devel::Peek::CvGV($it);
                $ct{$name}++ if $name =~ /ANON/;
            }
        }
        $all = undef;  # required to free memory
        foreach my $n (sort { $ct{$a} <=> $ct{$b} } keys %ct) {
            next unless $ct{$n} > 1 || $args eq "all";
            $self->write(sprintf("%7d $n\n", $ct{$n}));
        }
    }
    $self->write(".\n");
}

=head2 "maxqueue" function [max_queue_size]

For a given function of job, the maximum queue size is adjusted to be max_queue_size jobs long. A negative value indicates unlimited queue size.

If the max_queue_size value is not supplied then it is unset (and the default maximum queue size will apply to this function).

This function will return OK upon success, and will return ERR incomplete_args upon an invalid number of arguments.

=cut

sub TXTCMD_maxqueue {
    my Gearman::Server::Client $self = shift;
    my $args = shift;
    my ($func, $max) = split /\s+/, $args;

    unless (length $func) {
        return $self->err_line('incomplete_args');
    }

    $self->{server}->set_max_queue($func, $max);
    $self->write("OK\n");
}

=head2 "shutdown" ["graceful"]

Close the server.  Or "shutdown graceful" to close the listening socket, then close the server when traffic has died away.

=cut

sub TXTCMD_shutdown {
    my Gearman::Server::Client $self = shift;
    my $args = shift;
    if ($args eq "graceful") {
        $self->write("OK\n");
        Gearmand::shutdown_graceful();
    } elsif (! $args) {
        $self->write("OK\n");
        exit 0;
    } else {
        $self->err_line('unknown_args');
    }
}

=head2 "version"

Returns server version.

=cut

sub TXTCMD_version {
    my Gearman::Server::Client $self = shift;
    $self->write("$Gearman::Server::VERSION\n");
}

sub err_line {
    my Gearman::Server::Client $self = shift;
    my $err_code = shift;
    my $err_text = {
        'unknown_command' => "Unknown server command",
        'unknown_args' => "Unknown arguments to server command",
        'incomplete_args' => "An incomplete set of arguments was sent to this command",
    }->{$err_code};

    $self->write("ERR $err_code " . eurl($err_text) . "\r\n");
    return 0;
}

sub eurl {
    my $a = $_[0];
    $a =~ s/([^a-zA-Z0-9_\,\-.\/\\\: ])/uc sprintf("%%%02x",ord($1))/eg;
    $a =~ tr/ /+/;
    return $a;
}

1;
