\name{redis.connect}
\alias{redis.connect}
\alias{redis.close}
\title{
  Manage connections to a Redis database
}
\description{
  \code{redis.connect} creates a new connection to a Redis database and
  returns the handle.

  \code{redis.close} closes a Redis database connection.
}
\usage{
redis.connect(host = "localhost", port = 6379L, timeout = 30,
              reconnect = FALSE, retry = FALSE, db = getOption("redis.default.db", 0L))
redis.close(rc)
}
\arguments{
  \item{host}{name of the host to connect to or a path to the socket (if
    \code{port} is 0)}
  \item{port}{numeric, TCP port to connectt to or 0 is a local (unix)
    socket is to be used instead (not supported on Windows, obvously).}
  \item{timeout}{numeric, timeout in seconds for requests (reals are
    supported for sub-second accuracy)}
  \item{reconnect}{logical, if \code{TRUE} then commands used on this
    connection will attempt to re-connect in case the connection is
    closed unexpectedly (e.g., due to a previous error).}
  \item{retry}{logical, if \code{TRUE} then commands will attempt to
    retry once on connection failure by closing the connection,
    re-connecting and re-trying. Only meaningful in conjunction with
    \code{reconnect=TRUE}.}
  \item{db}{integer, index of the database (keyspace) to use. The index
    \code{0} is the default and any other index will result in a
    \code{SELECT} command to be sent upon connection to select the
    desired database.}
  \item{rc}{Redis connection handle (as returned by \code{redis.connect})}
}
%\details{
%}
\value{
  \code{redis.connect}: an opaque handle to use for subsequent
  operations on the connection (object of the class \code{redisConnection})
  
  \code{redis.close}: \code{NULL} (invisibly)
}
%\references{
%}
\author{
  Simon Urbanek
}
%\note{
%}
%\seealso{
%}
\examples{
## try connecting - 
c <- tryCatch(redis.connect(),
              error = function(e) {
                cat("Cannot connect",e$message, " - please start Redis\n")
                NULL
              })
if (!is.null(c)) { ## go ahead only if Redis is up and we got a connection
  print(redis.get(c, "foo"))
  print(redis.set(c, "foo", "bar"))
  print(redis.get(c, "foo"))
  redis.rm(c, "foo")
  redis.close(c)
}
}
\keyword{database}
