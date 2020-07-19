redis.connect <- function(host="localhost", port=6379L, timeout=30, reconnect=FALSE, retry=FALSE, db=getOption("redis.default.db", 0L), password=NULL) .Call(cr_connect, host, port, timeout, reconnect, retry, db, password)

redis.clone <- function(rc, db=NA) .Call(cr_clone, rc, db)

redis.get <- function(rc, keys, list=FALSE, character=FALSE) {
  r <- .Call(cr_get, rc, keys, list, character)
  if (is.list(r)) lapply(r, function(o) .Call(raw_unpack, o)) else if (!is.character(r)) .Call(raw_unpack, r) else r
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

redis.pop <- function(rc, keys, timeout=0, r2l=TRUE) {
  r <- if (timeout <= 0) {
    if (length(keys) == 1) .Call(cr_cmd, rc, c(if (r2l) "LPOP" else "RPOP", as.character(keys))) else stop("Redis supports non-blocking pops only for single keys")
  } else {
    if (!is.finite(timeout)) timeout <- 0
    .Call(cr_cmd, rc, c(if (r2l) "BLPOP" else "BRPOP", keys, as.integer(timeout)))
  }
  if (is.list(r)) {
    l <- lapply(r, function(o) .Call(raw_unpack, o))
    ## for blocking pop we get a list of keys and values
    if (length(l) == 2 && length(l[[1]]) == length(l[[2]])) {
      r <- l[[2]]
      names(r) <- l[[1]]
      r
    } else l
  } else if (!is.character(r)) .Call(raw_unpack, r) else r
}

## FIXME: we only support as.is string values
redis.push <- function(rc, key, value, r2l=TRUE)
  .Call(cr_cmd, rc, c(if (r2l) "RPUSH" else "LPUSH", as.character(key), as.character(value)))

redis.rm <- function(rc, keys) invisible(.Call(cr_del, rc, keys))

## FIXME: values must be a list of raw vectors -- the only reason is that this is a quick hack to replace rredis in RCS and that's all we need for now (since rredis was serializing everything)
redis.set <- function(rc, keys, values, as.is=FALSE) invisible(.Call(cr_set, rc, keys, if (as.is) values else if (is.raw(values)) list(values) else lapply(values, serialize, NULL)))

redis.close <- function(rc) invisible(.Call(cr_close, rc))

redis.keys <- function(rc, pattern=NULL) .Call(cr_keys, rc, pattern)

redis.auth <- function(rc, password)
  invisible(if (!identical(res <- .Call(cr_cmd, rc, c("AUTH", as.character(password)[1L])), "OK")) stop("redis authentication failed with ", res) else TRUE)
