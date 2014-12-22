\name{redis.auth}
\alias{redis.auth}
\title{
  Authenticate redis connection
}
\description{
  \code{redis.auth} attempts to authenticate a redis connection
}
\usage{
redis.auth(rc, password)
}
\arguments{
  \item{rc}{Redis connection as returned by \code{\link{redis.connect}}}
  \item{password}{string, password to provide}
}
\details{
  Issues a redis \code{AUTH} command with the given password. Will
  fail if redis was not configured with \code{requirepass}
  configuration (since redis issues an error in that case).

  Note that \code{redis.connect(..., password)} is preferrable for
  persistent authentication since it performs \code{AUTH} before
  selecting the database and remembers the password for implicit
  re-connects - neither of which is possible with \code{redis.auth}.
}
\value{
  \code{TRUE} if successful, raises an error on failure
}
%\references{
%}
\author{
Simon Urbanek
}
\keyword{database}
