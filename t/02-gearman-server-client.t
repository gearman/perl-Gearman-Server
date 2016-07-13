use strict;
use warnings;

use IO::Socket::INET;
use Test::More;

my $mn = "Gearman::Server::Client";

use_ok("Gearman::Server");
use_ok($mn);

isa_ok($mn, "Danga::Socket");

# new_ok($mn, [IO::Socket::INET->new(), new_ok("Gearman::Server")]);

done_testing;

