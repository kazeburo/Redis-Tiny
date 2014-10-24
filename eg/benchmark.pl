#!/usr/bin/env perl

use strict;
use warnings;
use 5.10.0;
use Benchmark qw/cmpthese/;

use Redis::Fast;
use Redis::Tiny;
use Redis;

my $redis = Redis->new;
my $fast = Redis::Fast->new;
my $tiny = Redis::Tiny->new;

$tiny->command(qw!set foo foovalue!);
say $fast->get('foo');

cmpthese(
    -1,
    {
        fast => sub {
            for (1..10) {
                my $val = $fast->get('foo');
            }
        },
        tiny => sub {
            for (1..10) {
                my $data = $tiny->command(qw/get foo/);
            }
        },
        redis => sub {
            for (1..10) {
                my $data = $redis->get('foo');
            }
        },
    }
);


