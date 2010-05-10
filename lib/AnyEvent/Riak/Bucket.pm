package AnyEvent::Riak::Bucket;

use Moose;
use AnyEvent::HTTP;

with qw/
  AnyEvent::Riak::Role::CVCB
  AnyEvent::Riak::Role::HTTPUtils
  /;

has _client => (is => 'rw', isa => 'AnyEvent::Riak', required => 1);
has name    => (is => 'rw', isa => 'Str',            required => 1);
has _properties =>
  (is => 'rw', isa => 'HashRef', predicate => '_has_properties');
has r       => (
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
                $self->_client->host, [$self->_client->path, $self->name],
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

sub get {
    my ($self, $key, $r) = @_;
    my $obj = AnyEvent::Riak::Object->new(
        client => $self->_client,
        key    => $key,
        r      => $r,
        bucket => $self,
    )->get;
}

no Moose;

1;
