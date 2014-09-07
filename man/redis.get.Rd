\name{redis.get}
\alias{redis.get}
\alias{redis.set}
\alias{redis.rm}
\alias{redis.keys}
\title{
  Get/set/query database content
}
\description{
  \code{redis.get} retrieves values for given keys

  \code{redis.set} sets (stores/replaces) values for given keys

  \code{redis.rm} removes values for given keys

  \code{redis.keys} lists all keys matching a pattern
}
\usage{
redis.get(rc, keys, list = FALSE)
redis.set(rc, keys, values)
redis.rm (rc, keys)
redis.keys(rc, pattern = NULL)
}
\arguments{
  \item{rc}{redis connection handle as returned by
  \code{\link{redis.connect}}}
  \item{keys}{character vector of keys. For \code{redis.set} this can
  also be a named list in which case the names are used as keys and
  elements as values.}
  \item{list}{logical, if \code{TRUE} then \code{keys} is expected to
  be a single key and the return will be the value. If \code{FALSE}
  then the result is always a list with one value for each key.}
  \item{values}{only used if \code{keys} is a character vector. If
  \code{keys} is a single string then the content of \code{values}
  will be stored under that key. Otherwise it must be a list of the
  same length as \code{keys} and each element will be stored under the
  corresponding key.}
  \item{pattern}{string with a glob-style pattern as suppored by Redis
    or \code{NULL} to retrieve all keys}
}
\details{
  \code{redis.get} retrieves values associated with the speicifed
  keys. If \code{list} is \code{FALSE} then \code{keys} is expected to
  be a single string and the value is returned directly. If
  \code{list} is \code{TRUE} then a list is returned with one result
  element for each key element. If a key doesn't exist \code{NULL} is
  returned for that key.

  \code{redis.set} sets one or more key/value pairs. If \code{keys} is
  of length one then \code{values} is interpreted directly as a single
  value. If \code{keys} is a character vector of length of more than
  one, then \code{values} must be a list of the same length. Finally,
  \code{keys} can be a named list in which case \code{values} are
  ignored and names of the list are taken as keys and elements as
  values.

  \code{redis.rm} removes a key/value pair from the database. See the
  note below for the difference of \code{redis.rm} and setting a value
  of \code{NULL}.

  \code{redis.keys} list all matching keys present in the database.
}
%\value{
%}
%\references{
%}
\author{
 Simon Urbanek
}
\note{
\code{rediscc} was originally written as a replacement of
\code{rredis} so it shares the serialized value format which has a few
odd implications. \code{rredis} doesn't store values directly, but
uses R serialization to store the serialized content in the
value. On the plus side it means that arbitrary values can be stored,
but it also means that storing an integer into a key will result in the
storage of a binary blob such that seemingly intuitive actions like
incrementing it don't work since Redis doesn't recognize it as a
number. Also it means that it is possible to store the value
\code{NULL} in the key but this is not distinguishable from the
deleted key since \code{redis.get} will return \code{NULL} in both
cases.
}
\seealso{
\code{\link{redis.connect}}
}
%\examples{
%}
\keyword{database}
