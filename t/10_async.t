use strict;
use warnings;

use Test::More;
use JSON::XS;
use Test::Exception;
use AnyEvent::Riak;
use YAML::Syck;

plan tests => 5;

my ( $host, $path );

BEGIN {
    my $riak_test = $ENV{RIAK_TEST_SERVER};
    ($host, $path) = split ";", $riak_test if $riak_test;
    plan skip_all => 'set $ENV{RIAK_TEST_SERVER} if you want to run the tests'
      unless ($host && $path);
}

my $bucket = 'test';

ok my $riak = AnyEvent::Riak->new(
    host => $host,
    path => $path,
    w    => 1,
    dw   => 1
  ),
  'create riak object';

{
    my $cv = AnyEvent->condvar;
    $cv->begin(sub { $cv->send });
    $cv->begin;
    # ping
    $riak->is_alive(
        callback => sub {
            my $res = shift;
            pass "is alive in cb" if $res;
            $cv->end;
        }
    );
    $cv->end;
    $cv->recv;
}

{
    my $cv = AnyEvent->condvar;
    $cv->begin(sub { $cv->send });
    $cv->begin;
    # list bucket
    $riak->list_bucket(
        $bucket,
        parameters => {props => 'true', keys => 'true'},
        callback   => sub {
            my $res = shift;
            ok $res->{props}, 'got props';
            $cv->end;
        }
    );
    $cv->end;
    $cv->recv;
}

{
    my $value = {foo => 'bar',};
    my $cv = AnyEvent->condvar;
    $cv->begin(sub { $cv->send });
    $cv->begin;

    # store object
    $riak->store(
        $bucket, 'bar3', $value,
        callback => sub {
            pass "store value ok";
            $riak->fetch(
                'foo', 'bar3',
                callback => sub {
                    my $body = shift;
                    is_deeply($body, $value, 'value is ok in cb');
                    $cv->end;
                }
            );
        }
    );
    $cv->end;
    $cv->recv;
}
