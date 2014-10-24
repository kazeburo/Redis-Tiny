package Redis::Tiny;

use 5.008005;
use strict;
use warnings;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use IO::Socket qw(:crlf IPPROTO_TCP TCP_NODELAY);
use IO::Socket::INET;
use IO::Select;
use Encode;

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

sub command {
    my $self = shift;
    return unless @_;
    my $cmds = 1;
    if ( ref $_[0] eq 'ARRAY' ) {
        $cmds = @_;
    }
    $self->send_message(@_) or return;
    my $res = $self->read_message($cmds) or return;
    return $res->[0] if $cmds == 1;
    $res;
}

sub send_message {
    my $self = shift;
    return unless @_;
    my $msg='';
    if ( ref $_[0] eq 'ARRAY') {
        for my $msgs ( @_ ) {
            $msg .= '*'.scalar(@$msgs).$CRLF;
            for my $m (@$msgs) {
                utf8::encode($m) if $self->{utf8};
                $msg .= '$'.length($m).$CRLF;
                $msg .= $m.$CRLF;
            }
        }        
    }
    else {
        $msg .= '*'.scalar(@_).$CRLF;
        for my $m (@_) {
            utf8::encode($m) if $self->{utf8};
            $msg .= '$'.length($m).$CRLF;
            $msg .= $m.$CRLF;
        }
    }
    $self->write_all($msg) or $self->last_error('failed to send message: '. (($!) ? "$!" : "timeout") );
}

sub read_message {
    my $self = shift;
    my $requires = shift // 1;
    $self->{sockbuf} = '';
    my @msgs;
    $self->{do_select} = 1;
    while (1) {
        while ( my $msg = $self->parse_reply(\$self->{sockbuf}) ) {
            push @msgs, $msg if defined $msg;
        }
        last if ( @msgs >= $requires );
        $self->read_timeout(\$self->{sockbuf}, $READ_BYTES, length $self->{sockbuf})
            or return $self->last_error('failed to read message: ' . (($!) ? "$!" : "timeout"));
    }
    return \@msgs;
}

sub parse_reply {
    my $self = shift;
    my $buf = shift;

    return if length($$buf) < 2;
    my $first_crlf = index($$buf, $CRLF);
    $first_crlf -= 1;
    return if $first_crlf < 0;
    my $s = substr($$buf,0,1, '');
    my $first_val = substr($$buf, 0, $first_crlf, '');
    substr($$buf, 0, 2, '');
    if ( $s eq '+' ) {
        # 1 line reply
        utf8::decode($first_val) if $self->{utf8};
        return { data => $first_val };
    }
    elsif ( $s eq '-' ) {
        # error
        # -ERR unknown command 'a'
        return { data => undef, error => $first_val };
    }
    elsif ( $s eq ':' ) {
        # numeric
        # :1404956783
        return { data => $first_val };
    }
    elsif ( $s eq '$' ) {
        # bulk
        # C: get mykey
        # S: $3
        # S: foo
        my $size = $first_val;
        if ( $size eq '-1' ) {
            return { data => undef };
        }
        return if length($$buf) < $size + 2;
        my $data = substr($$buf, 0, $size+2,'');
        $data = substr($data,0,$size);
        utf8::decode($data) if $self->{utf8};
        return { data => $data };
    }
    elsif ( $s eq '*' ) {
        # multibulk
        # *3
        # $3
        # foo
        # $-1
        # $3
        # baa
        #
        ## null list/timeout
        # *-1
        #
        my $size = $first_val;
        if ( $size eq '-1' ) {
            return { data => undef };
        }
        return { data => [] } if $size eq '0';
        my @res;
        while ( @res < $size ) {
            return unless ( $$buf =~ m!^\$(-?\d+?)$CRLF!sm );
            my $length = $1;
            substr($$buf, 0, length($length)+3,'');
            if ( $length eq '-1' ) {
                push @res, undef;
                next;
            }
            return if length($$buf) < $length + 2;
            my $data = substr($$buf, 0, $length,'');
            substr($$buf, 0, 2,'');
            utf8::decode($data) if $self->{utf8};
            push @res, $data;
        }
        return { data => \@res };
    }
    die "failed parse_reply $$buf===\n";
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

