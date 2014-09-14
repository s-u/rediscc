\name{redis.zero}
\alias{redis.zero}
\alias{redis.inc}
\alias{redis.dec}
\title{
  Counter key operations
}
\description{
  \code{redis.zero} resets a counter to zero

  \code{redis.inc} increments a counter

  \code{redis.dec} decrements a counter
}
\usage{
redis.zero(rc, key)
redis.inc(rc, key)
redis.dec(rc, key, N0 = FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{rc}{Redis connection as returned by \code{\link{redis.connect}}}
  \item{key}{key associated with the counter}
  \item{N0}{logical, if \code{TRUE} then counter will be reset to zero
  if decreasing it leads to negative number.}
}
\details{
  Counters are implemented directly in Redis and therefore do no
  behave like any other values set via \code{redis.set} since they are
  not serialized R objects. This is also why they have to be reset
  using \code{redis.zero} and cannot be set using
  \code{\link{redis.set}}.

  Note that \code{redis.dec} with \code{N0=TRUE} is currently
  implemented such that the counter is first decremented and only if it
  leads to a negative value it is reset to zero.
}
\value{
 integer, resulting counter value
}
%\references{
%}
\author{
Simon Urbanek
}
\keyword{database}
