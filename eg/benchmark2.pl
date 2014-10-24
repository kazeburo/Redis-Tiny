#!/usr/bin/env perl

use strict;
use warnings;
use 5.10.0;
use Benchmark qw/cmpthese/;
use Redis::Tiny;

my $tiny = Redis::Tiny->new;
$tiny->command(qw!set foo foovalue!);

cmpthese(
    -1,
    {
        tiny => sub {
            for (1..10) {
                my $data = $tiny->command(qw/get foo/);
            }
        },
    }
);
