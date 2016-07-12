use strict;
use warnings;

use Test::More;

my $mn = qw/
    Gearman::Server
    /;

use_ok($mn);
my $gs = new_ok($mn);

done_testing;

