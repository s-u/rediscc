\name{redis.expire}
\alias{redis.expire}
\title{
  Set key lifetime
}
\description{
  \code{redis.expire} set lifetime of key to expire in seconds
}
\usage{
redis.expire(rc, key, seconds)
}
\arguments{
  \item{rc}{Redis connection as returned by \code{\link{redis.connect}}}
  \item{key}{key associated with the counter}
  \item{seconds}{Number of seconds after which the key should expire}
}
\details{
	Set a timeout on key. After the timeout has expired,
	the key will automatically be deleted.
	A key with an associated timeout is often said to be volatile in Redis terminology.

	It is possible to call EXPIRE using as argument a key that already has an existing expire set. 
	In this case the time to live of a key is updated to the new value.
}
\value{
 integer, resulting 1 success 0 failure
}
%\references{
%}
\author{
Simon Urbanek
}
\keyword{database}
