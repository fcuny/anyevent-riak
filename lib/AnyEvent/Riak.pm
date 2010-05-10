package AnyEvent::Riak;

use strict;
use warnings;

use Carp;
use JSON;
use AnyEvent;
use AnyEvent::HTTP;
use MIME::Base64;
use YAML::Syck;

use Moose;
with qw/
  AnyEvent::Riak::Role::CVCB
  AnyEvent::Riak::Role::HTTPUtils
  /;

use AnyEvent::Riak::Bucket;

our $VERSION = '0.02';

has host => (is => 'rw', isa => 'Str', default => 'http://127.0.0.1:8098');
has path => (is => 'rw', isa => 'Str', default => 'riak');
has mapred_path => (is => 'rw', isa => 'Str', default => 'mapred');
has r           => (is => 'rw', isa => 'Int', default => 2);
has w           => (is => 'rw', isa => 'Int', default => 2);
has dw          => (is => 'rw', isa => 'Int', default => 2);
has client_id   => (
    is  => 'rw',
    isa => 'Str',
    default =>
      sub { "perl_anyevent_riak" . encode_base64(int(rand(10737411824)), '') }
);

sub is_alive {
    my ($self, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        GET     => $self->_build_uri($self->host, [qw/ping/]),
        headers => $self->_build_headers($options{params}),
        sub {
            my ($body, $headers) = @_;
            if ($headers->{Status} == 200) {
                $cv->send($cb->(1));
            }
            else {
                $cv->send($cb->(0));
            }
        },
    );
    return $cv;
}

sub list_bucket {
    my ($self, $bucket_name, %options) = @_;
    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        GET => $self->_build_uri(
            [$self->{path}, $bucket_name],
            $options{params}
        ),
        headers => $self->_build_headers($options{params}),
        sub {

            my ($body, $headers) = @_;
            if ($body && $headers->{Status} == 200) {
                my $res = JSON::decode_json($body);
                $cv->send($cb->($res));
            }
            else {
                $cv->send(undef);
            }
        }
    );
    return $cv;
}

sub set_bucket {
    my ($self, $bucket, $schema, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        PUT =>
          $self->_build_uri([$self->{path}, $bucket], $options{params}),
        headers => $self->_build_headers($options{params}),
        body    => JSON::encode_json($schema),
        sub {
            my ($body, $headers) = @_;
            if ($headers->{Status} == 204) {
                $cv->send($cb->(1));
            }
            else {
                $cv->send($cb->(0));
            }
        }
    );
    $cv;
}

sub fetch {
    my ($self, $bucket, $key, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        GET => $self->_build_uri(
            [$self->{path}, $bucket, $key],
            $options{params}
        ),
        headers => $self->_build_headers($options{params}),
        sub {
            my ($body, $headers) = @_;
            if ($body && $headers->{Status} == 200) {
                $cv->send($cb->(JSON::decode_json($body)));
            }
            else {
                $cv->send($cb->(0));
            }
        }
    );
    $cv;
}

sub store {
    my ($self, $bucket, $key, $object, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    my $json = JSON::encode_json($object);

    http_request(
        POST => $self->_build_uri(
            [$self->{path}, $bucket, $key],
            $options{params}
        ),
        headers => $self->_build_headers($options{params}),
        body    => $json,
        sub {
            my ($body, $headers) = @_;
            my $result;
            if ($headers->{Status} == 204) {
                $result = $body ? JSON::decode_json($body) : 1;
            }
            else {
                $result = 0;
            }
            $cv->send($cb->($result));
        }
    );
    $cv;
}

sub delete {
    my ($self, $bucket, $key, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        DELETE => $self->_build_uri(
            [$self->{path}, $bucket, $key],
            $options{params}
        ),
        headers => $self->_build_headers($options{params}),
        sub {
            $cv->send($cb->(@_));
        }
    );
    $cv;
}

sub bucket {
    my ($self, $name) = @_;
    return AnyEvent::Riak::Bucket->new(name => $name, _client => $self);
}

no Moose;

1;

__END__

=head1 NAME

AnyEvent::Riak - Non-blocking Riak client

=head1 SYNOPSIS

    use AnyEvent::Riak;

    my $riak = AnyEvent::Riak->new(
        host => 'http://127.0.0.1:8098',
        path => 'riak',
    );

    die "Riak is not running" unless $riak->is_alive->recv;

    my $bucket = $riak->set_bucket('foo', {props => {n_val => 5}})->recv;

This version is not compatible with the previous version (0.01) of this module and with Riak < 0.91.

For a complete description of the Riak REST API, please refer to
L<https://wiki.basho.com/display/RIAK/REST+API>.

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using C<AnyEvent>. This client allows you to connect to a Riak instance, create, modify and delete Riak objects.

=head2 METHODS

=over 4

=item B<is_alive>([callback => sub { }, params => { }])

Check if the Riak server is alive. If the ping is successful, 1 is returned,
else 0.

    my $ping = $riak->is_alive->recv;

=item B<list_bucket>($bucketname, [callback => sub { }, params => { }])

Get the schema and key list for 'bucket'. Possible parameters are:

=over 2

=item

props=[true|false] - whether to return the bucket properties

=item

keys=[true|false|stream] - whether to return the keys stored in the bucket

=back

If the operation failed, C<undef> is returned, else an hash reference
describing the bucket is returned.

    my $bucket = $riak->list_bucket(
        'bucketname',
        parameters => {
            props => 'false',
        },
        callback => sub {
            my $struct = shift;
            if ( scalar @{ $struct->{keys} } ) {
                # do something
            }
        }
    );

=item B<set_bucket>($bucketname, $bucketschema, [parameters => { }, callback => sub { }])

Sets bucket properties like n_val and allow_mult.
=over 2

=item

n_val - the number of replicas for objects in this bucket

=item

allow_mult - whether to allow sibling objects to be created (concurrent updates)

=back

If successful, B<1> is returned, else B<0>.

    my $result = $riak->set_bucket('bucket')->recv;

=item B<fetch>($bucketname, $object, [parameters => { }, callback => sub { }])

Reads an object from a bucket.

=item B<store>($bucketname, $objectname, $objectdata, [parameters => { }, callback => sub { }]);

=item B<delete>($bucketname, $objectname, [parameters => { }, callback => sub { }]);

=back

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2009, 2010 by linkfluence.

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
