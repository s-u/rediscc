redis.connect <- function(host="localhost", port=6379L, timeout=30, reconnect=FALSE, retry=FALSE, db=getOption("redis.default.db", 0L)) .Call(cr_connect, host, port, timeout, reconnect, retry, db)

redis.authenticate <- function(rc, password) .Call(cr_cmd, rc, c("AUTH", as.character(password)))

redis.get <- function(rc, keys, list=FALSE) {
  r <- .Call(cr_get, rc, keys, list)
  if (is.list(r)) lapply(r, function(o) .Call(raw_unpack, o)) else .Call(raw_unpack, r)
}

redis.inc <- function(rc, key) as.integer(.Call(cr_cmd, rc, c("INCR", as.character(key))))

redis.dec <- function(rc, key, N0=FALSE)
  if (N0) { ## FIXME: this is NOT atomic!
    i <- redis.dec(rc, key, FALSE)
    if (i < 0L) {
      redis.zero(rc, key)
      0L
    } else i
  } else as.integer(.Call(cr_cmd, rc, c("DECR", as.character(key))))

redis.zero <- function(rc, key) .Call(cr_cmd, rc, c("SET", as.character(key)[1L], "0"))

redis.rm <- function(rc, keys) invisible(.Call(cr_del, rc, keys))

## FIXME: values must be a list of raw vectors -- the only reason is that this is a quick hack to replace rredis in RCS and that's all we need for now (since rredis was serializing everything)
redis.set <- function(rc, keys, values) invisible(.Call(cr_set, rc, keys, if (is.raw(values)) list(values) else lapply(values, serialize, NULL)))

redis.close <- function(rc) invisible(.Call(cr_close, rc))

redis.keys <- function(rc, pattern=NULL) .Call(cr_keys, rc, pattern)
