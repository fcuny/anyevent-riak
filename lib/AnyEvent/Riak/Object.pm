package AnyEvent::Riak::Object;

use Moose;
use AnyEvent::HTTP;

with qw/
  AnyEvent::Riak::Role::Client
  AnyEvent::Riak::Role::HTTPUtils
  AnyEvent::Riak::Role::CVCB
  /;

has key     => (is => 'rw', isa => 'Str');
has _content => (is => 'rw', isa => 'HashRef', predicate => '_has_content');
has content_type => (is => 'rw', isa => 'Str', default => 'application/json');
has bucket => (is => 'rw', isa => 'AnyEvent::Riak::Bucket', required => 1);
has status => (is => 'rw', isa => 'Int');
has r      => (is => 'rw', isa => 'Int');

sub get {
    my ($self, %options) = @_;

    my ($cv, $cb) = $self->cvcb(\%options);

    if ($self->_has_content) {
        $cv->send($self->_content);
    }
    else {
        http_request(
            GET => $self->_build_uri(
                [$self->_client->path, $self->bucket->name, $self->key],
                $options{params}
            ),
            headers => $self->_build_headers($options{params}),
            sub {
                my ($body, $headers) = @_;
                if ($body && $headers->{Status} == 200) {
                    my $content = JSON::decode_json($body);
                    $self->_content($content);
                    $cv->send($cb->($self->_content));
                }
                else {
                    $cv->send(undef);
                }
            }
        );
    }
    return $cv;
}

no Moose;

1;
