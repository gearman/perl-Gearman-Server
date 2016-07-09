use strict;
use warnings;
use Test::More;

my @mn = qw/
    Gearman::Server
    Gearman::Server::Client
    Gearman::Server::Listener
    Gearman::Server::Job
    /;

my $v = '1.13.001';


foreach my $n (@mn) {
    use_ok($n);
    my $_v = eval '$' . $n . '::VERSION';
    is($_v, $v, "$n version is $v");
} ## end foreach my $n (@mn)

done_testing;

