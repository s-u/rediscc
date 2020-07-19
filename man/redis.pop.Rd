\name{redis.pop}
\alias{redis.pop}
\alias{redis.push}
\title{
  Redis queues
}
\description{
  \code{redis.push} pushes an element to the list

  \code{redis.pop} pops and element from the list
}
\usage{
redis.pop(rc, keys, timeout=0, r2l=TRUE)
redis.push(rc, key, value, r2l=TRUE)
}
\arguments{
  \item{rc}{redis connection handle as returned by
    \code{\link{redis.connect}}}
  \item{keys}{character vector of keys. Note that Redis only supports
    non-blocking POP for singe key, so must be a string if
    \code{timeout} is greater than 0, see Details.}
  \item{timeout}{integer >= 0 or \code{Inf}. Time out (in seconds)
    for the POP operation. If 0 then non-blocking POP is performed.}
  \item{r2l}{logical, if \code{TRUE} then the operation is
    right-to-left, i.e. right push, left pop. If \code{FALSE} then the
    operation is left-to-roght (left push, right pop).}
  \item{key}{string, key}
  \item{value}{value to push. Currently, only string values are
    supported adn they are stored as-is. Multiple values are allowed
    which corresponds to multiple operations.}    
}
\details{
  \code{redis.push} pushes an element into a list and \code{redis.pop}
  retrieves elements from the list. This pair of operations is commonly
  used to implement queues. If \code{timeout} is 0 then only a single key
  can be provided and the operation will return \code{NULL} if the list
  is empty, otherwise one element is popped. If \code{timeout} is
  \code{> 0} then a list of keys can be provided and it will return the
  moment an element is available in any of the lists (see Redis
  documentation on \code{BLPOP} for exact details). If the timeout
  value is not finite, then it blocks forever.

  If you think of a FIFO queue, \code{r2l} determines the direction in
  which the queue will work. If \code{TRUE} then elements are pushed
  on the right and popped on the left (most common). You can implement a
  LIFO queue by using the inverse value for pop vs push (e.g.,
  \code{r2l=FALSE} for push and \code{r2l=TRUE} for pop which will make
  the queue entry and exit on the left).

  Note that the current implementation only supports direct string values
  in \code{redis.push}. Also 
}
\value{
  \code{redis.pop} non-blocking version returns the obtained object or
  \code{NULL} if the list is empty. Blocking operation returns named
  vector with the obtained element bearing the name of the key from
  which it was obtained.

  \code{redis.push} returns the number of items in the list
}
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
