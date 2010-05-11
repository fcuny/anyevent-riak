package AnyEvent::Riak::Role::CVCB;

# ABSTRACT: return a default condvar and callback if none defined

use Moose::Role;

sub _cvcb {
    my ($self, $options) = @_;

    my ($cv, $cb) = (AnyEvent->condvar, sub { return @_ });
    if ($options && @$options) {
        $cv = pop @$options if UNIVERSAL::isa($options->[-1], 'AnyEvent::CondVar');
        $cb = pop @$options if ref $options->[-1] eq 'CODE';
    }
    ($cv, $cb);
}

1;

