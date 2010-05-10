package AnyEvent::Riak::Object;

use Moose;

has _client => (is => 'rw', isa => 'AnyEvent::Riak', requid => 1);
has key     => (is => 'rw', isa => 'Str');
has content => (is => 'rw', isa => 'HashRef');
has content_type => (is => 'rw', isa => 'Str', default => 'application/json');
has bucket => (is => 'rw', isa => 'AnyEvent::Riak::Bucket', required => 1);
has status => (is => 'rw', isa => 'Int');
has r      => (is => 'rw', isa => 'Int');

sub get {
    my ($self) = @_;
    $self->_client->http_get($self->bucket_name, $self->key, $self->r);
}

no Moose;

1;
