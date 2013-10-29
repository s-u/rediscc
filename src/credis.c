#include "hiredis.h"

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

typedef struct rconn_s {
    redisContext *rc;
} rconn_t;

static void rc_close(rconn_t *c) {
    if (c) {
	if (c->rc) redisFree(c->rc);
	c->rc = 0;
    }
}

static void rconn_fin(SEXP what) {
    rconn_t *c = (rconn_t*) EXTPTR_PTR(what);
    if (c) {
	rc_close(c);
	free(c);
    }    
}

SEXP cr_connect(SEXP sHost, SEXP sPort, SEXP sTimeout) {
    const char *host = "localhost";
    double tout = Rf_asReal(sTimeout);
    int port = Rf_asInteger(sPort);
    redisContext *ctx;
    rconn_t *c;
    SEXP res;
    struct timeval tv;

    if (TYPEOF(sHost) == STRSXP && LENGTH(sHost) > 0)
	host = CHAR(STRING_ELT(sHost, 0));

    tv.tv_sec = (int) tout;
    tv.tv_usec = (tout - (double)tv.tv_sec) * 1000000.0;
    if (port < 1)
	ctx = redisConnectUnixWithTimeout(host, tv);
    else
	ctx = redisConnectWithTimeout(host, port, tv);
    if (!ctx) Rf_error("connect to redis failed (NULL context)");
    if (ctx->err){
	SEXP es = Rf_mkChar(ctx->errstr);
	redisFree(ctx);
	Rf_error("connect to redis failed: %s", CHAR(es));
    }
    c = malloc(sizeof(rconn_t));
    if (!c) {
	redisFree(ctx);
	Rf_error("unable to allocate connection context");
    }
    c->rc = ctx;
    redisSetTimeout(ctx, tv);
    res = PROTECT(R_MakeExternalPtr(c, R_NilValue, R_NilValue));
    Rf_setAttrib(res, R_ClassSymbol, Rf_mkString("redisConnection"));
    R_RegisterCFinalizer(res, rconn_fin);
    UNPROTECT(1);
    return res;
}

SEXP cr_close(SEXP sc) {
    rconn_t *c;
    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    rc_close(c);
    return R_NilValue;
}

static SEXP rc_reply2R(redisReply *reply) {
    SEXP res;
    int i, n;
    /* Rprintf("rc_reply2R, type=%d\n", reply->type); */
    switch (reply->type) {
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_ERROR:
	res = PROTECT(Rf_allocVector(STRSXP, 1));
	SET_STRING_ELT(res, 0, Rf_mkCharLenCE(reply->str, reply->len, CE_UTF8));
	UNPROTECT(1);
	return res;
    case REDIS_REPLY_STRING:
	res = Rf_allocVector(RAWSXP, reply->len);
	memcpy(RAW(res), reply->str, reply->len);
	return res;
    case REDIS_REPLY_NIL:
	return R_NilValue;
    case REDIS_REPLY_INTEGER:
	if (reply->integer > INT_MAX || reply->integer < INT_MIN)
	    return Rf_ScalarReal((double) reply->integer);
	return Rf_ScalarInteger((int) reply->integer);
    case REDIS_REPLY_ARRAY:
	res = PROTECT(Rf_allocVector(VECSXP, reply->elements));
	n = reply->elements;
	for (i = 0; i < n; i++)
	    SET_VECTOR_ELT(res, i, rc_reply2R(reply->element[i]));
	UNPROTECT(1);
	return res;
    default:
	Rf_error("unknown redis reply type %d", reply->type);
    }
    return R_NilValue;
}

#define NARGBUF 128
static const char *argbuf[NARGBUF];

SEXP cr_get(SEXP sc, SEXP keys, SEXP asList) {
    rconn_t *c;
    int n, i, use_list = Rf_asInteger(asList);
    const char **argv = argbuf;
    redisReply *reply;
    SEXP res;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    if (!c->rc) Rf_error("disconnected connection");
    if (TYPEOF(keys) != STRSXP)
	Rf_error("invalid keys");
    n = LENGTH(keys);
    if (use_list < 0) /* asList == NA -> list for non scalar results only */
	use_list = (n == 1) ? 0 : 1;
    if (n != 1 && !use_list) Rf_error("exaclty one key must be specified with list=FALSE");
    if (n + 1 > NARGBUF) {
	argv = malloc(sizeof(const char*) * (n + 2));
	if (!argv)
	    Rf_error("out of memory");
    }
    argv[0] = "MGET";
    for (i = 0; i < n; i++)
	argv[i + 1] = CHAR(STRING_ELT(keys, i));
    /* we use strings only, so no need to supply argvlen */
    reply = redisCommandArgv(c->rc, n + 1, argv, 0);
    if (argv != argbuf)
	free(argv);
    if (!reply) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	Rf_error("MGET error: %s", CHAR(es));
    }
    /* Rprintf("reply, type=%d\n", reply->type); */
    if (reply->type != REDIS_REPLY_ARRAY) {
	freeReplyObject(reply);
	Rf_error("unexpected result type");
    }
    if (reply->elements != n) {
	freeReplyObject(reply);
	Rf_error("unexpected result length - should be %d but is %d", n, (int) reply->elements);
    }
    if (use_list) {
	int n = reply->elements;
	res = PROTECT(Rf_allocVector(VECSXP, n));
	Rf_setAttrib(res, R_NamesSymbol, keys);
	for (i = 0; i < n; i++)
	    SET_VECTOR_ELT(res, i, rc_reply2R(reply->element[i]));
	UNPROTECT(1);
    } else
	res = rc_reply2R(reply->element[0]);
    freeReplyObject(reply);
    return res;
}

SEXP cr_set(SEXP sc, SEXP keys, SEXP values) {
    rconn_t *c;
    int n, i;
    const char **argv = argbuf;
    size_t argsz[8];
    redisReply *reply;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    if (!c->rc) Rf_error("disconnected connection");
    if (TYPEOF(keys) != STRSXP)
	Rf_error("invalid keys");
    n = LENGTH(keys);
    if (n < 1) return R_NilValue;
    /* FIXME: we check only the first ... in the hope that we support more formats later */
    if (TYPEOF(values) != VECSXP || TYPEOF(VECTOR_ELT(values, 0)) != RAWSXP)
	Rf_error ("Sorry, values can only be a list of raw vectors for now");
    if (LENGTH(values) != n) Rf_error("keys/values length mismatch");
    if (2 * n + 1 > NARGBUF) {
	argv = malloc(sizeof(const char*) * (2 * n + 2));
	if (!argv)
	    Rf_error("out of memory");
    }
    argv[0] = "MSET"; argsz[0] = strlen(argv[0]);
    for (i = 0; i < n; i++) {
	argv [2 * i + 1] = CHAR(STRING_ELT(keys, i));
	argsz[2 * i + 1] = strlen(argv[2 * i + 1]);
	argv [2 * i + 2] = (char*) RAW(VECTOR_ELT(values, i));
	argsz[2 * i + 2] = LENGTH(VECTOR_ELT(values, i));
    }
    reply = redisCommandArgv(c->rc, 2 * n + 1, argv, argsz);
    if (argv != argbuf)
	free(argv);
    if (!reply) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	Rf_error("MSET error: %s", CHAR(es));
    }
    /* Rprintf("reply, type=%d\n", reply->type); */
    /* Note: the result is normally "status" - probably nothing useful we can do with that? */
    freeReplyObject(reply);
    return R_NilValue;
}

SEXP cr_del(SEXP sc, SEXP keys) {
    rconn_t *c;
    int n, i;
    const char **argv = argbuf;
    redisReply *reply;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    if (!c->rc) Rf_error("disconnected connection");
    if (TYPEOF(keys) != STRSXP)
	Rf_error("invalid keys");
    n = LENGTH(keys);
    if (n + 1 > NARGBUF) {
	argv = malloc(sizeof(const char*) * (n + 2));
	if (!argv)
	    Rf_error("out of memory");
    }
    argv[0] = "DEL";
    for (i = 0; i < n; i++)
	argv[i + 1] = CHAR(STRING_ELT(keys, i));
    /* we use strings only, so no need to supply argvlen */
    reply = redisCommandArgv(c->rc, n + 1, argv, 0);
    if (argv != argbuf)
	free(argv);
    if (!reply) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	Rf_error("DEL error: %s", CHAR(es));
    }
    /* Rprintf("reply, type=%d\n", reply->type); */
    freeReplyObject(reply);
    return R_NilValue;
}

SEXP cr_keys(SEXP sc, SEXP sPattern) {
    rconn_t *c;
    int n, i;
    const char *pattern = "*";
    redisReply *reply;
    SEXP res;
    
    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    if (!c->rc) Rf_error("disconnected connection");
    if (TYPEOF(sPattern) == STRSXP && LENGTH(sPattern) > 0)
	pattern = CHAR(STRING_ELT(sPattern, 0));
    reply = redisCommand(c->rc, "KEYS %s", pattern);
    if (!reply) {
        SEXP es = Rf_mkChar(c->rc->errstr);
        rc_close(c);
        Rf_error("KEYS error: %s", CHAR(es));
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        freeReplyObject(reply);
        Rf_error("unexpected result type");
    }
    res = PROTECT(Rf_allocVector(STRSXP, (n = reply->elements)));
    for (i = 0; i < n; i++) {
	if (reply->element[i]->type == REDIS_REPLY_STRING)
	    SET_STRING_ELT(res, i, Rf_mkCharLenCE(reply->element[i]->str, reply->element[i]->len, CE_UTF8));
	else if (reply->element[i]->type == REDIS_REPLY_NIL)
	    SET_STRING_ELT(res, i, R_NaString);
	else {
	    freeReplyObject(reply);
	    Rf_error("invalid element (non-string) in the keys array");
	}
    }
    freeReplyObject(reply);
    UNPROTECT(1);
    return res;
}
