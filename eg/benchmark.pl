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
my $tiny_noreply = Redis::Tiny->new(noreply=>1);

$tiny->command(qw!set foo foovalue!);
say $fast->get('foo');


cmpthese(
    20_000,
    {
        fast => sub {
            my $val = $fast->get('foo');
        },
        tiny => sub {
            my $data = $tiny->command(qw/get foo/);
        },
        redis => sub {
            my $data = $redis->get('foo');
        },
    }
);

print "single incr =======\n";

cmpthese(
    20_000,
    {
        fast => sub {
            my $val = $fast->incr('incrfoo');
        },
        tiny => sub {
            my $data = $tiny->command(qw/incr incrfoo/);
        },
        tiny_noreply => sub {
            $tiny_noreply->command(qw/incr incrfoo/);
        },
        redis => sub {
            my $data = $redis->incr('incrfoo');
        },
    }
);

print "pipeline =======\n";

my $cb = sub {};
cmpthese(
    20_000,
    {
        fast => sub {
            $fast->del('user-fail',$cb);
            $fast->del('ip-fail',$cb);
            $fast->lpush('user-log','xxxxxxxxxxx',$cb);
            $fast->lpush('login-log','yyyyyyyyyyy',$cb);
            $fast->wait_all_responses;
        },
        tiny => sub {
            my $val = $tiny->command(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
        tiny_noreply => sub {
            $tiny_noreply->command(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
        redis => sub {
            $redis->del('user-fail',$cb);
            $redis->del('ip-fail',$cb);
            $redis->lpush('user-log','xxxxxxxxxxx',$cb);
            $redis->lpush('login-log','yyyyyyyyyyy',$cb);
            $redis->wait_all_responses;
        },
    }
);

