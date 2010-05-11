use strict;
use warnings;

use JSON;
use Test::More;
use AnyEvent::Riak;

#plan tests => 4;

my $host = 'http://10.0.0.42:8098';
my $path = 'riak';

ok my $riak = AnyEvent::Riak->new(
    host => $host,
    path => $path,
    w    => 1,
    dw   => 1
  ),
  'create riak object';

ok my $ping = $riak->is_alive()->recv, 'ping: host is alive';
ok my $buckets =
  $riak->list_bucket('blog_content_temp', {keys => 'false'})->recv,
  "fetch bucket list";

my $value = {foo => 1};

ok my ($res, $headers) = $riak->store('foo', $value)->recv,
  'set a new key';
($res, $headers) = $riak->store('foo', $value, {key => 'foo_test'})->recv;
ok $res, 'stored key foo_test';

ok $res = $riak->fetch('foo', 'foo_test')->recv, 'fetch our new key';

is_deeply $value, $res, 'got same data';

ok $res = $riak->delete( 'foo', 'foo_test' )->recv, 'delete our key';

done_testing;
