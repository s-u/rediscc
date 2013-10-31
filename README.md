## About rediscc

`rediscc` is an R package that provides a tiny client for Redis. It is
based on `hiredis` C library and was created as a minimalistic
replacement for `rredis` which did not scale well in our production
environment. So far only the absolutely bare minimum is implemented to
support RCloud RCS calls - many thanks go to the hiredis team for
creating such a nice library!

For compatibility with `rredis` it uses the same serialization wrapper
(values are serialized using binary R serialization) such that existing
databases can be re-used.

In addition, it supports auto-reconnect and timeouts on all operations.

`hiredis` is bundled with the R package, so it is not needed as a
compile-time dependency. This is for our deployment reasons, not
anything deep - if you remove everything but `credis.c` from `src` and
add `PKG_CPPFLAGS=-I/usr/local/include/hiredis PKG_LIBS=-lhiredis` or
similar then you can build it against external hiredis.

## Use

It supports parallel connections, so you get a connection handle.

    c <- redis.connect()
    redis.set(c, "foo", "bar")
    redis.get(c, "foo")
    ## it is vectorized
    redis.set(c, list(a=1, b="gee", c="duh"))
    redis.get(c, c("a", "b, "foo"), TRUE)
    redis.keys(c)
    ## clean up what we created
    redis.rm(c, c("a", "b", "c", "foo", "bar")
    redis.close(c)

