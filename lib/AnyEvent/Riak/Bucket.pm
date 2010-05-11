package AnyEvent::Riak::Bucket;

use Moose;
use AnyEvent::HTTP;

use AnyEvent::Riak::Object;

with qw/
  AnyEvent::Riak::Role::CVCB
  AnyEvent::Riak::Role::HTTPUtils
  AnyEvent::Riak::Role::Client
  /;

has name => (is => 'rw', isa => 'Str', required => 1);
has _properties =>
  (is => 'rw', isa => 'HashRef', predicate => '_has_properties');
has r => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { my $self = shift; $self->_client->r }
);
has w => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { my $self = shift; $self->_client->w }
);
has dw => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { my $self = shift; $self->_client->dw }
);

sub get_properties {
    my ($self, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    if ($self->_has_properties) {
        $cv->send($self->_properties);
    }
    else {
        http_request(
            GET => $self->_build_uri(
                [$self->_client->path, $self->name],
                $options{params}
            ),
            headers => $self->_build_headers($options{params}),
            sub {
                my ($body, $headers) = @_;
                if ($body && $headers->{Status} == 200) {
                    my $prop = JSON::decode_json($body);
                    $self->_properties($prop);
                    $cv->send($cb->($self->_properties));
                }
                else {
                    $cv->send(undef);
                }
            }
        );
    }
    return $cv;
}

sub set_properties {
    my ($self, $schema, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    http_request(
        PUT =>
          $self->_build_uri([$self->{path}, $self->name], $options{params}),
        headers => $self->_build_headers($options{params}),
        body    => JSON::encode_json({props => $schema}),
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
    return $cv;
}

sub create {
    my ($self, $key, $content) = @_;
    my $object = AnyEvent::Riak::Object->new(
        _client => $self->_client,
        key     => $key,
        content => $content,
        bucket  => $self,
    );
    return $object;
}

sub object {
    my ($self, $key, $r) = @_;
    my $obj = AnyEvent::Riak::Object->new(
        _client => $self->_client,
        key    => $key,
        r      => $r,
        bucket => $self,
    );
}

no Moose;

1;
