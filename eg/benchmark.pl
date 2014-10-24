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

print "single incr =======\n";

cmpthese(
    -1,
    {
        fast => sub {
            for (1..10) {
                my $val = $fast->incr('incrfoo');
            }
        },
        tiny => sub {
            for (1..10) {
                my $data = $tiny->command(qw/incr incrfoo/);
            }
        },
        tiny_noreply => sub {
            for (1..10) {
                $tiny->command(qw/incr incrfoo/);
            }
        },
        redis => sub {
            for (1..10) {
                my $data = $redis->incr('incrfoo');
            }
        },
    }
);

print "pipeline =======\n";

my $cb = sub {};
cmpthese(
    -1,
    {
        fast => sub {
            for (1..10) {
                $fast->del('user-fail',$cb);
                $fast->del('ip-fail',$cb);
                $fast->lpush('user-log','xxxxxxxxxxx',$cb);
                $fast->lpush('login-log','yyyyyyyyyyy',$cb);
                $fast->wait_all_responses;
            }
        },
        tiny => sub {
            for (1..10) {
                my $val = $tiny->command(
                    [qw/del user-fail/],
                    [qw/del ip-fail/],
                    [qw/lpush user-log xxxxxxxxxxx/],
                    [qw/lpush login-log yyyyyyyyyyy/]
                );
            }
        },
        tiny_noreply => sub {
            for (1..10) {
                $tiny->command(
                    [qw/del user-fail/],
                    [qw/del ip-fail/],
                    [qw/lpush user-log xxxxxxxxxxx/],
                    [qw/lpush login-log yyyyyyyyyyy/]
                );
            }
        },
        redis => sub {
            for (1..10) {
                $redis->del('user-fail',$cb);
                $redis->del('ip-fail',$cb);
                $redis->lpush('user-log','xxxxxxxxxxx',$cb);
                $redis->lpush('login-log','yyyyyyyyyyy',$cb);
                $redis->wait_all_responses;
            }
        },
    }
);

