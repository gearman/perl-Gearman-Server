package Test::Gearman::Server;

@ISA = qw/
    Exporter
    /;
@EXPORT_OK = qw/
    free_local_port
    /;

use strict;
use warnings;
use IO::Socket::INET;

sub free_local_port {
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

    return $port;
} ## end sub free_local_port

1;

