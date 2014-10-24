package Redis::Tiny;

use 5.008005;
use strict;
use warnings;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use IO::Socket qw(:crlf IPPROTO_TCP TCP_NODELAY);
use IO::Socket::INET;
use IO::Select;
use Time::HiRes qw/time/;
use Redis::Request::XS;
use Redis::Parser::XS;

our $VERSION = "0.01";
our $READ_BYTES = 131072;

sub new {
    my $class = shift;
    my %args = ref $_ ? %{$_[0]} : @_;
    %args = (
        server => '127.0.0.1:6379',
        timeout => 10,
        last_error => '',
        utf8 => 0,
        noreply => 0,
        %args,
    );
    my $server = shift;
    my $self = bless \%args, $class;
    $self;
}

sub connect {
    my $self = shift;
    return $self->{sock} if $self->{sock};
    $self->{sockbuf} = '';
    my $socket = IO::Socket::INET->new(
        PeerAddr => $self->{server},
        Timeout => $self->{timeout},
    ) or return;
    $socket->blocking(0);
    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        or die "setsockopt(TCP_NODELAY) failed:$!";
    $self->{sock} = $socket;
    $self->{fileno} = fileno($socket);
    $socket;
}

sub last_error {
    my $self = shift;
    if ( @_ ) {
        delete $self->{sock};
        $self->{last_error} = shift;
        return;
    }
    return $self->{last_error};
}

my $dummy_buf='';
sub command {
    my $self = shift;
    return unless @_;
    my $cmds = 1;
    if ( ref $_[0] eq 'ARRAY' ) {
        $cmds = @_;
    }
    my $msg = $self->{utf8} 
        ? build_request_redis_utf8(@_)
        : build_request_redis(@_);
    my $sended = $self->write_all($msg) 
        or $self->last_error('failed to send message: '. (($!) ? "$!" : "timeout") );
    if ( $self->{noreply} ) {
        sysread $self->{sock}, $dummy_buf, $READ_BYTES, 0;
        return $sended;
    }
    my $res = $self->read_message($cmds) or return;
    return $res->[0] if $cmds == 1;
    $res;
}

sub read_message {
    my $self = shift;
    my $requires = shift // 1;
    $self->{sockbuf} = '';
    my @msgs;
    $self->{do_select} = 1;
    while (1) {
        my $len = parse_redis($self->{sockbuf}, \@msgs);
        if ( ! defined $len ) {
            return $self->last_error('incorrect protocol message');
        }
        last if ( @msgs >= $requires );
        $self->read_timeout(\$self->{sockbuf}, $READ_BYTES, length $self->{sockbuf})
            or return $self->last_error('failed to read message: ' . (($!) ? "$!" : "timeout"));
    }
    return \@msgs;
}

sub read_timeout {
    my ($self, $buf, $len, $off) = @_;
    my $ret;
    my $sock = $self->connect or return;
    my $timeout = $self->{timeout};
    goto WAIT_READ if delete $self->{do_select};
 DO_READ:
    $ret = sysread $sock, $$buf, $len, $off
        and return $ret;
    unless ((! defined($ret)
                 && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK))) {
        return;
    }
 WAIT_READ:
    while (1) {
        my $efd = '';
        vec($efd, $self->{fileno}, 1) = 1;
        my ($rfd, $wfd) = ($efd, '');
        my $start_at = time;
        my $nfound = select($rfd, $wfd, $efd, $timeout);
        $timeout -= (time - $start_at);
        last if $nfound;
        return if $timeout <= 0;
    }
    goto DO_READ;
}

sub write_timeout {
    my ($self, $buf, $len, $off) = @_;
    my $ret;
    my $sock = $self->connect or return;
    my $timeout = $self->{timeout};
 DO_WRITE:
    $ret = syswrite $sock, $buf, $len, $off
        and return $ret;
    unless ((! defined($ret)
                 && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK))) {
        return;
    }
    while (1) {
        my $efd = '';
        vec($efd, $self->{fileno}, 1) = 1;
        my ($rfd, $wfd) = ($efd, '');
        my $start_at = time;
        my $nfound = select($rfd, $wfd, $efd, $timeout);
        $timeout -= (time - $start_at);
        last if $nfound;
        return if $timeout <= 0;
    }
    goto DO_WRITE;
}

sub write_all {
    my ($self, $buf) = @_;
    my $off = 0;
    while (my $len = length($buf) - $off) {
        my $ret = $self->write_timeout($buf, $len, $off)
            or return;
        $off += $ret;
    }
    return length $buf;
}


1;
__END__

=encoding utf-8

=head1 NAME

Redis::Simple - It's new $module

=head1 SYNOPSIS

    use Redis::Simple;

=head1 DESCRIPTION

Redis::Simple is ...

=head1 LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

=cut

