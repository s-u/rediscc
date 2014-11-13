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
  fail if redis was not configured with \code{requirepass} configuration.
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
