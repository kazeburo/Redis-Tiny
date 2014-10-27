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
    $socket->blocking(0) or die $!;
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
    my $fileno = $self->{fileno} || fileno($self->connect);
    my $sended = send_request_redis(
        $fileno,
        $self->{timeout},
        @_
    ) or $self->last_error('failed to send message: '. (($!) ? "$!" : "timeout") );
    if ( $self->{noreply} ) {
        Redis::Request::XS::phantom_read_redis($fileno);
        return "0 but true";
    }
    my @res;
    my $res = Redis::Request::XS::read_message_redis($fileno, $self->{timeout}, \@res, $cmds);
    ( $res == -1 ) and return $self->last_error('failed to read message: message corruption');
    ( $res == -2 ) and return $self->last_error('failed to read message: '. (($!) ? "$!" : "timeout"));
    return $res[0] if $cmds == 1;
    @res;
}

sub read_message {
    my $self = shift;
    my $requires = shift // 1;
    $self->{sockbuf} = '';
    my @msgs;
    while (1) {
        $self->read_timeout(\$self->{sockbuf}, $READ_BYTES, length $self->{sockbuf})
            or return $self->last_error('failed to read message: ' . (($!) ? "$!" : "timeout"));
        my $len = Redis::Request::XS::parse_reply($self->{sockbuf}, \@msgs);
        if ( ! defined $len ) {
            return $self->last_error('incorrect protocol message');
        }
        last if ( @msgs >= $requires );
        if ( $len > 0 ) {
            $self->{sockbuf} = substr($self->{sockbuf},0,$len);
        }
        $self->{do_select} = 1;
    }
    return \@msgs;
}

sub read_timeout {
    my ($self, $buf, $len, $off) = @_;
    my $ret;
    my $sock = ($self->{sock} ||=  $self->connect) or return;
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
        #my $start_at = time;
        my $nfound = select($rfd, $wfd, $efd, $timeout);
        #$timeout -= (time - $start_at);
        last if $nfound;
        #return if $timeout <= 0;
    }
    goto DO_READ;
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

