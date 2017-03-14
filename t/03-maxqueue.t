use strict;
use warnings;

use File::Spec;
use FindBin ();
use IO::Socket::INET;
use Net::EmptyPort ();
use Test::More;
use Test::TCP;

my $host = "127.0.0.1";
Net::EmptyPort::can_bind($host) || plan skip_all => "can not bind to $host";

my $dir = File::Spec->catdir($FindBin::Bin, File::Spec->updir());
my $bin = File::Spec->catdir($dir, "bin", "gearmand");
-e $bin || plan skip_all => "no gearmand";

my $gs = Test::TCP->new(
    host => $host,
    code => sub {
        my ($port) = @_;
        exec $^X, join('', "-I", File::Spec->catdir($dir, "lib")), $bin,
            join('=', "--port", $port);
    }
);

my ($func, $count) = ("doit", int(rand(3) + 1));
my $peer_addr = join(':', $host, $gs->port);

subtest "set maxqueue", sub {
    my $sock
        = new_ok("IO::Socket::INET", [PeerAddr => $peer_addr, Timeout => 2]);
    my $k = "MAXQUEUE";
    my $cmd = join(' ', $k, $func, $count);
    ok($sock->write($cmd . $/), "write($cmd)");
    ok(my $r = $sock->getline(), "getline");
    ok($r =~ m/^OK\b/i, "match OK");
};

done_testing();
