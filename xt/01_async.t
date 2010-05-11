use strict;
use warnings;

use Test::More;
use JSON::XS;
use Test::Exception;
use AnyEvent::Riak;
use YAML::Syck;

#plan tests => 6;

my $host = 'http://10.0.0.42:8098';
my $path = 'riak';

ok my $riak = AnyEvent::Riak->new(
    host => $host,
    path => $path,
    w    => 1,
    dw   => 1
  ),
  'create riak object';

my $cv = AnyEvent->condvar;

$riak->is_alive(
    callback => sub {
        my $res = shift;
        ok $res, "is alive in cb";
    }
);

$riak->list_bucket(
    'blog_content_temp',
    {keys => 'false'},
    sub {
        my $res = shift;
        ok $res, "got result list_bucket";
    }
);

$riak->set_bucket(
    'blog_content_temp',
    {n_val => 5},
    sub {
        my $res = shift;
        ok $res, "got result in set_bucket"
    }
);

$riak->fetch(
    'blog_content_temp',
    '012853de99ce67c2f0f09c0c2ea28cbe5de8f653137d273803f85a398d1de840',
    sub {
        my $res = shift;
        ok $res, "got result in fetch"
    }
);

$riak->store(
    'blog_content_temp',
    {foo => 1},
    sub {
        my $res = shift;
        ok $res, "got result in store"
    }
);

$cv->recv;

# my ( $host, $path );

# BEGIN {
#     my $riak_test = $ENV{RIAK_TEST_SERVER};
#     ($host, $path) = split ";", $riak_test if $riak_test;
#     plan skip_all => 'set $ENV{RIAK_TEST_SERVER} if you want to run the tests'
#       unless ($host && $path);
# }

# my $bucket = 'test';

# ok my $riak = AnyEvent::Riak->new(
#     host => $host,
#     path => $path,
#     w    => 1,
#     dw   => 1
#   ),
#   'create riak object';

# {
    # my $cv = AnyEvent->condvar;
    # $cv->begin(sub { $cv->send });
    # $cv->begin;
    # # ping


# }

# # {
# #     my $cv = AnyEvent->condvar;
# #     $cv->begin(sub { $cv->send });
# #     $cv->begin;
# #     # list bucket
# #     $riak->list_bucket(
# #         $bucket,
# #         parameters => {props => 'true', keys => 'true'},
# #         callback   => sub {
# #             my $res = shift;
# #             ok $res->{props}, 'got props';
# #             $cv->end;
# #         }
# #     );
# #     $cv->end;
# #     $cv->recv;
# # }

# # {
# #     my $key   = 'bar';
# #     my $value = {foo => 'bar',};
# #     my $cv    = AnyEvent->condvar;
# #     $cv->begin(sub { $cv->send });
# #     $cv->begin;

# #     # store object
# #     $riak->store(
# #         $bucket, $key, $value,
# #         callback => sub {
# #             pass "store value ok";
# #             $riak->fetch(
# #                 $bucket, $key,
# #                 callback => sub {
# #                     my $body = shift;
# #                     is_deeply($body, $value, 'value is ok in cb');
# #                     $riak->delete(
# #                         $bucket, $key,
# #                         callback => sub {
# #                             my $res = shift;
# #                             is $res, 1, 'key deleted';
# #                             $cv->end;
# #                         }
# #                     );

# #                 }
# #             );
# #         }
# #     );
# #     $cv->end;
# #     $cv->recv;
# # }

done_testing();
