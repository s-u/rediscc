redis.connect <- function(host="localhost", port=6379L, timeout=30, reconnect=FALSE, retry=FALSE) .Call(cr_connect, host, port, timeout, reconnect, retry)

redis.get <- function(rc, keys, list=FALSE) {
  r <- .Call(cr_get, rc, keys, list)
  if (is.list(r)) lapply(r, function(o) if (is.raw(o)) unserialize(o) else o) else if (is.raw(r)) unserialize(r) else r
}

redis.rm <- function(rc, keys) invisible(.Call(cr_del, rc, keys))

## FIXME: values must be a list of raw vectors -- the only reason is that this is a quick hack to replace rredis in RCS and that's all we need for now (since rredis was serializing everything)
redis.set <- function(rc, keys, values) invisible(.Call(cr_set, rc, keys, lapply(values, serialize, NULL)))

redis.close <- function(rc) invisible(.Call(cr_close, rc))

redis.keys <- function(rc, pattern=NULL) .Call(cr_keys, rc, pattern)
