package Gearman::Server::Job;
use version ();
$Gearman::Server::Job::VERSION = version->declare("1.140_001");

use strict;
use warnings;

=head1 NAME

Gearman::Server::Job - job representation of L<Gearman::Server>

=head1 DESCRIPTION

=head1 METHODS

=cut

use Gearman::Server::Client;
use Scalar::Util;
use Sys::Hostname;

use fields (
    'func',
    'uniq',
    'argref',

    # arrayref of interested Clients
    'listeners',
    'worker',
    'handle',

    # [1, 100]
    'status',
    'require_listener',

    # Gearman::Server that owns us
    'server',
);

sub new {
    my Gearman::Server::Job $self = shift;
    my ($server, $func, $uniq, $argref, $highpri) = @_;

    $self = fields::new($self) unless ref $self;

    # if they specified a uniq, see if we have a dup job running already
    # to merge with
    if (length($uniq)) {

        # a unique value of "-" means "use my args as my unique key"
        $uniq = $$argref if $uniq eq "-";
        if (my $job = $server->job_of_unique($func, $uniq)) {

            # found a match
            return $job;
        }

        # create a new key
        $server->set_unique_job($func, $uniq => $self);
    } ## end if (length($uniq))

    $self->{'server'}           = $server;
    $self->{'func'}             = $func;
    $self->{'uniq'}             = $uniq;
    $self->{'argref'}           = $argref;
    $self->{'require_listener'} = 1;
    $self->{'listeners'}        = [];
    $self->{'handle'}           = $server->new_job_handle;

    $server->enqueue_job($self, $highpri);
    return $self;
} ## end sub new

=head2 add_listener($client)

=cut

sub add_listener {
    my Gearman::Server::Job $self  = shift;
    my Gearman::Server::Client $li = shift;

    push @{ $self->{listeners} }, $li;
    Scalar::Util::weaken($self->{listeners}->[-1]);
} ## end sub add_listener

=head2 relay_to_listeners($msg)

=cut

sub relay_to_listeners {
    my Gearman::Server::Job $self = shift;
    foreach my Gearman::Server::Client $c (@{ $self->{listeners} }) {
        next if !$c || $c->{closed};
        $c->write($_[0]);
    }
} ## end sub relay_to_listeners

=head2 relay_to_option_listeners($msg, [$option])

=cut

sub relay_to_option_listeners {
    my Gearman::Server::Job $self = shift;
    my $option = $_[1];
    foreach my Gearman::Server::Client $c (@{ $self->{listeners} }) {
        next if !$c || $c->{closed};
        next unless $c->option($option);
        $c->write($_[0]);
    }

} ## end sub relay_to_option_listeners

=head2 clear_listeners()

=cut

sub clear_listeners {
    my Gearman::Server::Job $self = shift;
    $self->{listeners} = [];
}

=head2 listeners()

=cut

sub listeners {
    my Gearman::Server::Job $self = shift;
    return @{ $self->{listeners} };
}

=head2 uniq()

=cut

sub uniq {
    my Gearman::Server::Job $self = shift;
    return $self->{uniq};
}

=head2 note_finished($success)

=cut

sub note_finished {
    my Gearman::Server::Job $self = shift;
    my $success = shift;

    $self->{server}->note_job_finished($self);
} ## end sub note_finished

=head2 worker()

=cut

# accessors:
sub worker {
    my Gearman::Server::Job $self = shift;
    return $self->{'worker'} unless @_;
    return $self->{'worker'} = shift;
}

=head2 require_listener([$require])

=cut

sub require_listener {
    my Gearman::Server::Job $self = shift;
    return $self->{'require_listener'} unless @_;
    return $self->{'require_listener'} = shift;
}

=head2 status([numerator,denominator])

=cut

# takes arrayref of [numerator,denominator]
sub status {
    my Gearman::Server::Job $self = shift;
    return $self->{'status'} unless @_;
    return $self->{'status'} = shift;
}

=head2 handle()

=cut

sub handle {
    my Gearman::Server::Job $self = shift;
    return $self->{'handle'};
}

=head2 func()

=cut

sub func {
    my Gearman::Server::Job $self = shift;
    return $self->{'func'};
}

=head2 argref()

=cut

sub argref {
    my Gearman::Server::Job $self = shift;
    return $self->{'argref'};
}

1;
