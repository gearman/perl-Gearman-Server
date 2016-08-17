use strict;
use warnings;

use File::Spec;
use FindBin ();
use Gearman::Client;
use Gearman::Worker;
use IO::Socket::INET;
use Net::EmptyPort ();
use Proc::Guard;
use Test::More;
use Test::TCP;

my $host = "127.0.0.1";
Net::EmptyPort::can_bind($host) || plan skip_all => "can not bind to $host";

my $dir = File::Spec->catdir($FindBin::Bin, File::Spec->updir());
my $bin = File::Spec->catdir($dir, "bin", "gearmand");
-e $bin || plan skip_all => "no gearmand";

my $gs = Test::TCP->new(
    listen => 1,
    host   => $host,
    code   => sub {
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
    sleep(2);
    ok(my $r = $sock->getline(), "getline");
    ok($r =~ m/^OK\b/i, "match OK");
};

subtest "start worker", sub {
    my $gw = new_ok("Gearman::Worker", [job_servers => [$peer_addr]]);
    $gw->register_function($func);

    Proc::Guard->new(
        code => sub {
            $gw->work() while 1;
        }
    );
};

subtest "client", sub {
    my $gc
        = new_ok("Gearman::Client", [job_servers => [$peer_addr]]);
    ok($gc->do_task($func));
};
done_testing();
