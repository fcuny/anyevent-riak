package AnyEvent::Riak;

# ABSTRACT: non-blocking Riak client

use JSON;
use AnyEvent;
use AnyEvent::HTTP;
use Moose;

with qw/AnyEvent::Riak::Role::HTTPUtils AnyEvent::Riak::Role::CVCB/;

our $VERSION = '0.02';

has host => (is => 'rw', isa => 'Str', default => 'http://127.0.0.1:8098');
has path        => (is => 'rw', isa => 'Str', default => 'riak');
has mapred_path => (is => 'rw', isa => 'Str', default => 'mapred');
has r           => (is => 'rw', isa => 'Int', default => 2);
has w           => (is => 'rw', isa => 'Int', default => 2);
has dw          => (is => 'rw', isa => 'Int', default => 2);

sub is_alive {
    my $self = shift;

    my ($cv, $cb) = $self->_cvcb(\@_);
    my $options = shift;

    http_request(
        GET     => $self->_build_uri([qw/ping/]),
        headers => $self->_build_headers(),
        sub {
            my ($body, $headers) = @_;
            if ($headers->{Status} == 200) {
                $cv->send($cb->(1));
            }
            else {
                $cv->send($cb->(0));
            }
        }
    );
    $cv;
}

sub list_bucket {
    my $self = shift;
    my $bucket_name = shift;

    my ($cv, $cb) = $self->_cvcb(\@_);
    my $options = shift;

    my $params = {
        props => delete $options->{props} || 'true',
        keys  => delete $options->{keys}  || 'true',
    };

    http_request(
        GET     => $self->_build_uri([$self->path, $bucket_name], $params),
        headers => $self->_build_headers(),
        sub {
            my ($body, $headers) = @_;
            if ($body && $headers->{Status} == 200) {
                my $res = JSON::decode_json($body);
                $cv->send($cb->($res));
            }
            else {
                $cv->send($cb->(undef));
            }
        }
    );
    $cv;
}

sub set_bucket {
    my $self        = shift;
    my $bucket_name = shift;
    my $schema      = shift;

    my ($cv, $cb) = $self->_cvcb(\@_);

    http_request(
        PUT     => $self->_build_uri([$self->path, $bucket_name]),
        headers => $self->_build_headers(),
        body => JSON::encode_json({props => $schema}),
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
    my $self        = shift;
    my $bucket_name = shift;
    my $key         = shift;

    my ($cv, $cb) = $self->_cvcb(\@_);
    my $options = shift;

    my $params = {r => $options->{params}->{r} || $self->r,};

    if ($options->{vtag}) {
        $params->{vtag} = delete $options->{vtag};
    }

    my $headers = {};
    foreach (qw/If-None-Match If-Modified-Since Accept/) {
        $headers->{$_} = delete $options->{headers}->{$_}
          if (exists $options->{headers}->{$_});
    }

    http_request(
        GET =>
          $self->_build_uri([$self->path, $bucket_name, $key], $params),
        headers => $self->_build_headers($headers),
        sub {
            my ($body, $headers) = @_;
            # XXX 300 && 304
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
    my $self        = shift;
    my $bucket_name = shift;
    my $object      = shift;

    my ($cv, $cb) = $self->_cvcb(\@_);
    my $options = shift;
    my $key = '';

    my $params = {
        w          => $options->{params}->{w}          || $self->w,
        dw         => $options->{params}->{dw}         || $self->dw,
        returnbody => $options->{params}->{returnbody} || 'true',
    };

    if ($options->{key}) {
        $key = delete $options->{key};
        $params->{r} = $options->{params}->{r} || $self->r;
    }

    # XXX headers

    my $json = JSON::encode_json($object);

    http_request(
        POST => $self->_build_uri([$self->path, $bucket_name, $key,], $params),
        headers => $self->_build_headers(),
        body    => $json,
        sub {
            my ($body, $headers) = @_;
            my $result;
            if ($body && ($headers->{Status} == 201 || $headers->{Status} == 200)) {
                $result = $body ? JSON::decode_json($body) : 1;
            }
            elsif ($headers->{Status} == 204) {
                $result = 1;
            }
            else {
                $result = 0;
            }
            $cv->send($cb->($result, $headers));
        }
    );
    $cv;
}

sub delete {
    my $self        = shift;
    my $bucket_name = shift;
    my $key         = shift;

    my ($cv, $cb) = $self->_cvcb(@_);

    http_request(
        DELETE  => $self->_build_uri([$self->path, $bucket_name, $key],),
        headers => $self->_build_headers(),
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

no Moose;

1;

__END__

=head1 SYNOPSIS

    use AnyEvent::Riak;

    my $riak = AnyEvent::Riak->new(
        host => 'http://127.0.0.1:8098',
        path => 'riak',
    );

This version is not compatible with the previous version (0.01) of this module and with Riak < 0.91.

For a complete description of the Riak REST API, please refer to L<https://wiki.basho.com/display/RIAK/REST+API>.

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using C<AnyEvent>. This client allows you to connect to a Riak instance, create, modify and delete Riak objects.

=head2 METHODS

=over 4

=item B<is_alive> ([$cv, $cb])

Check if the Riak server is alive. If the ping is successful, 1 is returned, else 0.

Options can be:

=over 4

=item B<headers>

A list of valid HTTP headers that will be send with the query

=back

=item B<list_bucket> ($bucket_name, [$options, $cv, $cb])

Reads the bucket properties and/or keys.

    $riak->list_bucket(
        'mybucket',
        {props => 'true', keys => 'false'},
        sub {
            my $res = shift;
            ...
        }
      );

=item B<set_bucket> ($bucket_name, $schema, [%options, $cv, $cb])

Sets bucket properties like n_val and allow_mult.

    $riak->set_bucket(
        'mybucket',
        {n_val => 5},
        sub {
            my $res = shift;
            ...;
        }
    );

=item B<fetch> ($bucket_name, $key, [$options, $cv, $cb])

Reads an object from a bucket.

    $riak->fetch(
        'mybucket', 'mykey',
        {params => {r = 2}, headers => {'If-Modified-Since' => $value}},
        sub {
            my $res = shift;
        }
    );

=item B<store> ($bucket_name, $key, $object, [$options, $cv, $cb])

Stores a new object in a bucket.

    $riak->store(
        'mybucket', $object,
        {key => 'mykey', headers => {''}, params => {w => 2}},
        sub {
            my $res = shift;
            ...
        }
    );

=item B<delete> ($bucket, $key, [$options, $cv, $cb])

Deletes an object from a bucket.

    $riak->delete('mybucket', 'mykey', sub { my $res = shift;... });

=back
