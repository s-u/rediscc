## About rediscc

`rediscc` is an R package that provides a tiny client for Redis. It is
based on `hiredis` C library and was created as a minimalistic
replacement for `rredis` which did not scale well in our production
environment. So far only the absolutely bare minimum is implemented to
support RCloud RCS calls (due to lack of time - done in two hours
thanks to the clean hiredis API - thanks hiredis team!). For
compatibility with `rredis` it uses the same serialization wrapper
such that existing databases can be re-used.
