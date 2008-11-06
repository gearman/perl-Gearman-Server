package Gearman::Server;

=head1 NAME

Gearman::Server - function call "router" and load balancer

=head1 DESCRIPTION

You run a Gearman server (or more likely, many of them for both
high-availability and load balancing), then have workers (using
L<Gearman::Worker> from the Gearman module, or libraries for other
languages) register their ability to do certain functions to all of
them, and then clients (using L<Gearman::Client>,
L<Gearman::Client::Async>, etc) request work to be done from one of
the Gearman servers.

The servers connect them, routing function call requests to the
appropriate workers, multiplexing responses to duplicate requests as
requested, etc.

More than likely, you want to use the provided L<gearmand> wrapper
script, and not use Gearman::Server directly.

=cut

use strict;
use Gearman::Server::Client;
use Gearman::Server::Listener;
use Gearman::Server::Job;
use Socket qw(IPPROTO_TCP SOL_SOCKET SOCK_STREAM AF_UNIX SOCK_STREAM PF_UNSPEC);
use Carp qw(croak);
use Sys::Hostname ();
use IO::Handle ();

use fields (
            'client_map',    # fd -> Client
            'sleepers',      # func -> { "Client=HASH(0xdeadbeef)" => Client }
            'job_queue',     # job_name -> [Job, Job*]  (key only exists if non-empty)
            'job_of_handle', # handle -> Job
            'max_queue',     # func -> configured max jobqueue size
            'job_of_uniq',   # func -> uniq -> Job
            'handle_ct',     # atomic counter
            'handle_base',   # atomic counter
            'listeners',     # arrayref of listener objects
            );

our $VERSION = "1.09";

=head1 METHODS

=head2 new

  $server_object = Gearman::Server->new( %options )

Creates and returns a new Gearman::Server object, which attaches itself to the Danga::Socket event loop. The server will begin operating when the Danga::Socket runloop is started. This means you need to start up the runloop before anything will happen.

Options:

=over

=item port

Specify a port which you would like the Gearman::Server to listen on for TCP connections (not necessary, but useful)

=back

=cut

sub new {
    my ($class, %opts) = @_;
    my $self = ref $class ? $class : fields::new($class);

    $self->{client_map}    = {};
    $self->{sleepers}      = {};
    $self->{job_queue}     = {};
    $self->{job_of_handle} = {};
    $self->{max_queue}     = {};
    $self->{job_of_uniq}   = {};
    $self->{listeners}     = [];

    $self->{handle_ct} = 0;
    $self->{handle_base} = "H:" . Sys::Hostname::hostname() . ":";

    my $port = delete $opts{port};
    croak("Unknown options") if %opts;
    $self->create_listening_sock($port);

    return $self;
}

sub debug {
    my ($self, $msg) = @_;
    #warn "$msg\n";
}

=head2 create_listening_sock

  $server_object->create_listening_sock( $portnum )

Add a TCP port listener for incoming Gearman worker and client connections.

=cut

sub create_listening_sock {
    my ($self, $portnum) = @_;
    my $ssock = IO::Socket::INET->new(LocalPort => $portnum,
                                      Type      => SOCK_STREAM,
                                      Proto     => IPPROTO_TCP,
                                      Blocking  => 0,
                                      Reuse     => 1,
                                      Listen    => 10 )
        or die "Error creating socket: $@\n";

    my $listeners = $self->{listeners};
    push @$listeners, Gearman::Server::Listener->new($ssock, $self);

    return $ssock;
}

sub new_client {
    my ($self, $sock) = @_;
    my $client = Gearman::Server::Client->new($sock, $self);
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;
}

sub note_disconnected_client {
    my ($self, $client) = @_;
    delete $self->{client_map}{$client->{fd}};
}

sub clients {
    my $self = shift;
    return values %{ $self->{client_map} };
}

# Returns a socket that is connected to the server, we can then use this
# socket with a Gearman::Client::Async object to run clients and servers in the
# same thread.
sub to_inprocess_server {
    my $self = shift;

    my ($psock, $csock);
    socketpair($csock, $psock, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or  die "socketpair: $!";

    $csock->autoflush(1);
    $psock->autoflush(1);

    IO::Handle::blocking($csock, 0);
    IO::Handle::blocking($psock, 0);

    my $client = Gearman::Server::Client->new($csock, $self);

    my ($package, $file, $line) = caller;
    $client->{peer_ip}  = "[$package|$file|$line]";
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;

    return $psock;
}

=head2 start_worker

  $pid = $server_object->start_worker( $prog )

  ($pid, $client) = $server_object->start_worker( $prog )

Fork and start a worker process named by C<$prog> and returns the pid (or pid and client object).

=cut

sub start_worker {
    my ($self, $prog) = @_;

    my ($psock, $csock);
    socketpair($csock, $psock, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or  die "socketpair: $!";

    $csock->autoflush(1);
    $psock->autoflush(1);

    my $pid = fork;
    unless (defined $pid) {
        warn "fork failed: $!\n";
        return undef;
    }

    # child process
    unless ($pid) {
        local $ENV{'GEARMAN_WORKER_USE_STDIO'} = 1;
        close(STDIN);
        close(STDOUT);
        open(STDIN, '<&', $psock) or die "Unable to dup socketpair to STDIN: $!";
        open(STDOUT, '>&', $psock) or die "Unable to dup socketpair to STDOUT: $!";
        if (UNIVERSAL::isa($prog, "CODE")) {
            $prog->();
            exit 0; # shouldn't get here.  subref should exec.
        }
        exec $prog;
        die "Exec failed: $!";
    }

    close($psock);

    IO::Handle::blocking($csock, 0);
    my $sock = $csock;

    my $client = Gearman::Server::Client->new($sock, $self);

    $client->{peer_ip}  = "[gearman_child]";
    $client->watch_read(1);
    $self->{client_map}{$client->{fd}} = $client;
    return wantarray ? ($pid, $client) : $pid;
}

sub enqueue_job {
    my ($self, $job, $highpri) = @_;
    my $jq = ($self->{job_queue}{$job->{func}} ||= []);

    if (defined (my $max_queue_size = $self->{max_queue}{$job->{func}})) {
        $max_queue_size--; # Subtract one, because we're about to add one more below.
        while (@$jq > $max_queue_size) {
            my $delete_job = pop @$jq;
            my $msg = Gearman::Util::pack_res_command("work_fail", $delete_job->handle);
            $delete_job->relay_to_listeners($msg);
            $delete_job->note_finished;
        }
    }

    if ($highpri) {
        unshift @$jq, $job;
    } else {
        push @$jq, $job;
    }

    $self->{job_of_handle}{$job->{'handle'}} = $job;
}

sub wake_up_sleepers {
    my ($self, $func) = @_;
    my $sleepmap = $self->{sleepers}{$func} or return;

    foreach my $addr (keys %$sleepmap) {
        my Gearman::Server::Client $c = $sleepmap->{$addr};
        next if $c->{closed} || ! $c->{sleeping};
        $c->res_packet("noop");
        $c->{sleeping} = 0;
    }

    delete $self->{sleepers}{$func};
    return;
}

sub on_client_sleep {
    my ($self, $cl) = @_;

    foreach my $cd (@{$cl->{can_do_list}}) {
        # immediately wake the sleeper up if there are things to be done
        if ($self->{job_queue}{$cd}) {
            $cl->res_packet("noop");
            $cl->{sleeping} = 0;
            return;
        }

        my $sleepmap = ($self->{sleepers}{$cd} ||= {});
        $sleepmap->{"$cl"} ||= $cl;
    }
}

sub jobs_outstanding {
    my Gearman::Server $self = shift;
    return scalar keys %{ $self->{job_queue} };
}

sub jobs {
    my Gearman::Server $self = shift;
    return values %{ $self->{job_of_handle} };
}

sub job_by_handle {
    my ($self, $handle) = @_;
    return $self->{job_of_handle}{$handle};
}

sub note_job_finished {
    my Gearman::Server $self = shift;
    my Gearman::Server::Job $job = shift;

    if (length($job->{uniq})) {
        delete $self->{job_of_uniq}{$job->{func}}{$job->{uniq}};
    }
    delete $self->{job_of_handle}{$job->{handle}};
}

# <0/undef/"" to reset.  else integer max depth.
sub set_max_queue {
    my ($self, $func, $max) = @_;
    if (defined $max && length $max && $max >= 0) {
        $self->{max_queue}{$func} = int($max);
    } else {
        delete $self->{max_queue}{$func};
    }
}

sub new_job_handle {
    my $self = shift;
    return $self->{handle_base} . (++$self->{handle_ct});
}

sub job_of_unique {
    my ($self, $func, $uniq) = @_;
    return undef unless $self->{job_of_uniq}{$func};
    return $self->{job_of_uniq}{$func}{$uniq};
}

sub set_unique_job {
    my ($self, $func, $uniq, $job) = @_;
    $self->{job_of_uniq}{$func} ||= {};
    $self->{job_of_uniq}{$func}{$uniq} = $job;
}

sub grab_job {
    my ($self, $func) = @_;
    return undef unless $self->{job_queue}{$func};

    my $empty = sub {
        delete $self->{job_queue}{$func};
        return undef;
    };

    my Gearman::Server::Job $job;
    while (1) {
        $job = shift @{$self->{job_queue}{$func}};
        return $empty->() unless $job;
        return $job unless $job->require_listener;

        foreach my Gearman::Server::Client $c (@{$job->{listeners}}) {
            return $job if $c && ! $c->{closed};
        }
        $job->note_finished(0);
    }
}


1;
__END__

=head1 SEE ALSO

L<gearmand>

=cut
