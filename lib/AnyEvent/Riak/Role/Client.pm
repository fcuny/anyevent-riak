package AnyEvent::Riak::Role::Client;

use Moose::Role;

has _client => (
    is       => 'rw',
    isa      => 'AnyEvent::Riak',
    required => 1,
    handles  => {host => 'host', client_id => 'client_id'}
);

1;
