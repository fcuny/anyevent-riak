package AnyEvent::Riak::Role::CVCB;

use Moose::Role;

sub default_cb {
    my ($self, $options) = @_;
    return sub {
        my $res = shift;
        return $res;
    };
}

sub cvcb {
    my ($self, $options) = @_;

    my ($cv, $cb);
    $cv = AE::cv;
    if ($options->{callback}) {
        $cb = delete $options->{callback};
    }
    else {
        $cb = $self->default_cb();
    }
    ($cv, $cb);
}

1;
