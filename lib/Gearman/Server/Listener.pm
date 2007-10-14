package Gearman::Server::Listener;

use strict;
use base 'Danga::Socket';
use fields qw(server);

use Errno qw(EAGAIN);
use Socket qw(IPPROTO_TCP TCP_NODELAY);

sub new {
    my Gearman::Server::Listener $self = shift;
    my $sock = shift;
    my $server = shift;

    $self = fields::new($self) unless ref $self;

    # make sure provided listening socket is non-blocking
    IO::Handle::blocking($sock, 0);

    $self->SUPER::new($sock);

    $self->{server} = $server;
    $self->watch_read(1);

    return $self;
}

sub event_read {
    my Gearman::Server::Listener $self = shift;

    my $listen_sock = $self->sock;

    local $!;

    if (my $csock = $listen_sock->accept) {
        IO::Handle::blocking($csock, 0);
        setsockopt($csock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;

        my $server = $self->{server};

        $server->debug(sprintf("Listen child making a Client for %d.", fileno($csock)));
        $server->new_client($csock);
        return;
    }

    return if $! == EAGAIN;

    warn "Error accepting incoming connection: $!\n";

    $self->watch_read(0);

    Danga::Socket->AddTimer( .1, sub {
        $self->watch_read(1);
    });
}

1;
