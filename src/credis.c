#include "hiredis.h"

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#define RCF_RECONNECT   1  /* auto-reconnect closed connection */
#define RCF_RETRY       2  /* on error, try to re-connect and re-run the same command */

typedef struct rconn_s {
    redisContext *rc;
    int    flags;
    char  *host;
    char  *pwd;
    int    port;
    int    db;
    double timeout;
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
	if (c->host) {
	    free(c->host);
	    c->host = 0;
	}
	if (c->pwd) {
	    free(c->pwd);
	    c->pwd = 0;
	}
	free(c);
    }
}

static void cr_conn_init(rconn_t *c) {
    char cmd[20];
    struct timeval tv;
    redisReply *reply;
    tv.tv_sec = (int) c->timeout;
    tv.tv_usec = (c->timeout - (double)tv.tv_sec) * 1000000.0;
    redisSetTimeout(c->rc, tv);

    if (c->pwd) { /* AUTH first */
	int pwd_len = strlen(c->pwd);
	char *auth = (char*) malloc(pwd_len + 8);
	if (!auth)
	    Rf_error("unable to allocate buffer for AUTH");
	strcpy(auth, "AUTH ");
	strcpy(auth + 5, c->pwd);
	reply = redisCommand(c->rc, auth);
	free(auth);
	if (!reply) {
	    SEXP es = Rf_mkChar(c->rc->errstr);
	    freeReplyObject(reply);
	    rc_close(c);
	    Rf_error("AUTH failed: %s", CHAR(es));
	} else if (reply->type == REDIS_REPLY_ERROR) {
	    SEXP es = Rf_mkChar(reply->str);
	    freeReplyObject(reply);
	    rc_close(c);
	    Rf_error("AUTH error response: %s", CHAR(es));
	}
	freeReplyObject(reply);
    }
    if (c->db) {
#ifdef WIN32
	sprintf(cmd, "SELECT %d", c->db);
#else
	snprintf(cmd, sizeof(cmd), "SELECT %d", c->db);
#endif
	reply = redisCommand(c->rc, cmd);
	if (!reply) {
	    SEXP es = Rf_mkChar(c->rc->errstr);
	    freeReplyObject(reply);
	    rc_close(c);
	    Rf_error("SELECT failed: %s", CHAR(es));
	} else if (reply->type == REDIS_REPLY_ERROR) {
	    SEXP es = Rf_mkChar(reply->str);
	    freeReplyObject(reply);
	    rc_close(c);
	    Rf_error("SELECT error response: %s", CHAR(es));
	}
	freeReplyObject(reply);
    }
}

SEXP cr_connect(SEXP sHost, SEXP sPort, SEXP sTimeout, SEXP sReconnect, SEXP sRetry, SEXP sDB, SEXP sPwd) {
    const char *host = "localhost", *pwd = 0;
    double tout = Rf_asReal(sTimeout);
    int port = Rf_asInteger(sPort), reconnect = (Rf_asInteger(sReconnect) > 0),
	retry = (Rf_asInteger(sRetry) > 0), db = Rf_asInteger(sDB);
    redisContext *ctx;
    rconn_t *c;
    SEXP res;
    struct timeval tv;

    if (TYPEOF(sHost) == STRSXP && LENGTH(sHost) > 0)
	host = CHAR(STRING_ELT(sHost, 0));
    if (TYPEOF(sPwd) == STRSXP && LENGTH(sPwd) > 0)
	pwd = CHAR(STRING_ELT(sPwd, 0));

    if (db < 0 || db > 65535)
	Rf_error("invalid DB number");

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
    c->flags = (reconnect ? RCF_RECONNECT : 0) | (retry ? RCF_RETRY : 0);
    c->host  = strdup(host);
    c->port  = port;
    c->timeout = tout;
    c->db    = db;
    c->pwd   = pwd ? strdup(pwd) : 0;
    res = PROTECT(R_MakeExternalPtr(c, R_NilValue, R_NilValue));
    Rf_setAttrib(res, R_ClassSymbol, Rf_mkString("redisConnection"));
    R_RegisterCFinalizer(res, rconn_fin);
    cr_conn_init(c);
    UNPROTECT(1);
    return res;
}

SEXP cr_clone(SEXP sc, SEXP sDb) {
    SEXP res;
    rconn_t *c, *nc;
    redisContext *ctx;
    int db = Rf_asInteger(sDb);
    struct timeval tv;

    if (!Rf_inherits(sc, "redisConnection"))
	Rf_error("invalid connection");
    if (db > 65535)
	Rf_error("invalid DB number");

    c = (rconn_t*) EXTPTR_PTR(sc);
    nc= (rconn_t*) malloc(sizeof(rconn_t));
    if (!nc)
	Rf_error("cannot allocate connection context");

    tv.tv_sec = (int) c->timeout;
    tv.tv_usec = (c->timeout - (double)tv.tv_sec) * 1000000.0;
    if (c->port < 1)
	ctx = redisConnectUnixWithTimeout(c->host, tv);
    else
	ctx = redisConnectWithTimeout(c->host, c->port, tv);
    if (!ctx) {
	free(nc);
	Rf_error("connect to redis failed (NULL context)");
    }
    if (ctx->err){
	SEXP es = Rf_mkChar(ctx->errstr);
	redisFree(ctx);
	Rf_error("connect to redis failed: %s", CHAR(es));
    }
    nc->rc      = ctx;
    nc->flags   = c->flags;
    nc->host    = strdup(c->host);
    nc->port    = c->port;
    nc->timeout = c->timeout;
    nc->db      = (db < 0) ? c->db : db;
    nc->pwd     = c->pwd ? strdup(c->pwd) : 0;
    res = PROTECT(R_MakeExternalPtr(nc, R_NilValue, R_NilValue));
    Rf_setAttrib(res, R_ClassSymbol, Rf_mkString("redisConnection"));
    R_RegisterCFinalizer(res, rconn_fin);
    cr_conn_init(nc);
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

static void rc_validate_connection(rconn_t *c, int optional) {
    if (!c->rc && (c->flags & RCF_RECONNECT)) {
	struct timeval tv;
	tv.tv_sec = (int) c->timeout;
	tv.tv_usec = (c->timeout - (double)tv.tv_sec) * 1000000.0;

	if (c->port < 1)
	    c->rc = redisConnectUnixWithTimeout(c->host, tv);
	else
	    c->rc = redisConnectWithTimeout(c->host, c->port, tv);
	if (!c->rc) {
	    if (optional) return;
	    Rf_error("disconnected connection and re-connect to redis failed (NULL context)");
	}
	if (c->rc->err){
	    SEXP es = Rf_mkChar(c->rc->errstr);
	    redisFree(c->rc);
	    c->rc = 0;
	    if (optional) return;
	    Rf_error("disconnected connection and re-connect to redis failed: %s", CHAR(es));
	}
	cr_conn_init(c);
	/* re-connect succeeded */
    }
    if (!c->rc && !optional)
	Rf_error("disconnected redis connection");
}

#define NARGBUF 128
static const char *argbuf[NARGBUF];
static size_t argszbuf[NARGBUF];

SEXP cr_get(SEXP sc, SEXP keys, SEXP asList) {
    rconn_t *c;
    int n, i, use_list = Rf_asInteger(asList);
    const char **argv = argbuf;
    redisReply *reply;
    SEXP res;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    rc_validate_connection(c, 0);
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
    if (!reply && (c->flags & RCF_RETRY)) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	rc_validate_connection(c, 1);
	if (c->rc)
	    reply = redisCommandArgv(c->rc, n + 1, argv, 0);
	else {
	    if (argv != argbuf)
		free(argv);
	    Rf_error("MGET error: %s and re-connect failed", CHAR(es));
	}
    }
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
    size_t *argsz = argszbuf;
    redisReply *reply;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    rc_validate_connection(c, 0);
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
	argsz = malloc(sizeof(size_t) * (2 * n + 2));
	if (!argsz) {
	    free(argv);
	    Rf_error("out of memory");
	}
    }
    argv[0] = "MSET"; argsz[0] = strlen(argv[0]);
    for (i = 0; i < n; i++) {
	argv [2 * i + 1] = CHAR(STRING_ELT(keys, i));
	argsz[2 * i + 1] = strlen(argv[2 * i + 1]);
	argv [2 * i + 2] = (char*) RAW(VECTOR_ELT(values, i));
	argsz[2 * i + 2] = LENGTH(VECTOR_ELT(values, i));
    }
    reply = redisCommandArgv(c->rc, 2 * n + 1, argv, argsz);
    if (!reply && (c->flags & RCF_RETRY)) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	rc_validate_connection(c, 1);
	if (c->rc)
	    reply = redisCommandArgv(c->rc, 2 * n + 1, argv, argsz);
	else {
	    if (argv != argbuf) {
		free(argv);
		free(argsz);
	    }
	    Rf_error("MGET error: %s and re-connect failed", CHAR(es));
	}
    }
    if (argv != argbuf) {
	free(argv);
	free(argsz);
    }
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
    rc_validate_connection(c, 0);
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
    if (!reply && (c->flags & RCF_RETRY)) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	rc_validate_connection(c, 1);
	if (c->rc)
	    reply = redisCommandArgv(c->rc, n + 1, argv, 0);
	else {
	    if (argv != argbuf)
		free(argv);
	    Rf_error("DEL error: %s and re-connect failed", CHAR(es));
	}
    }
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
    rc_validate_connection(c, 0);
    if (TYPEOF(sPattern) == STRSXP && LENGTH(sPattern) > 0)
	pattern = CHAR(STRING_ELT(sPattern, 0));
    reply = redisCommand(c->rc, "KEYS %s", pattern);
    if (!reply && (c->flags & RCF_RETRY)) {
	rc_close(c);
	rc_validate_connection(c, 0);
	reply = redisCommand(c->rc, "KEYS %s", pattern);
    }
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

/* issuue one command with one key paratemer and return the result */
SEXP cr_cmd(SEXP sc, SEXP sArgs) {
    rconn_t *c;
    const char **argv = argbuf;
    int n, i;
    redisReply *reply;
    SEXP res;

    if (!Rf_inherits(sc, "redisConnection")) Rf_error("invalid connection");
    c = (rconn_t*) EXTPTR_PTR(sc);
    if (!c) Rf_error("invalid connection (NULL)");
    rc_validate_connection(c, 0);
    if (TYPEOF(sArgs) != STRSXP || LENGTH(sArgs) < 1)
	Rf_error("invalid command - must be a string");
    n = LENGTH(sArgs);
    if (n + 1 > NARGBUF) {
	argv = malloc(sizeof(const char*) * (n + 2));
	if (!argv)
	    Rf_error("out of memory");
    }
    for (i = 0; i < n; i++)
	argv[i] = CHAR(STRING_ELT(sArgs, i));
    /* we use strings only, so no need to supply argvlen */
    reply = redisCommandArgv(c->rc, n, argv, 0);
    if (!reply && (c->flags & RCF_RETRY)) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	rc_validate_connection(c, 1);
	if (c->rc)
	    reply = redisCommandArgv(c->rc, 2, argv, 0);
	else {
	    if (argv != argbuf)
		free(argv);
	    Rf_error("%s error: %s and re-connect failed", argv[0], CHAR(es));
	}
    }
    if (argv != argbuf)
	free(argv);
    if (!reply) {
	SEXP es = Rf_mkChar(c->rc->errstr);
	rc_close(c);
	Rf_error("%s error: %s", argv[0], CHAR(es));
    }
    /* Rprintf("reply, type=%d\n", reply->type); */
    res = rc_reply2R(reply);
    freeReplyObject(reply);
    return res;
}

/* this is a work-around our compatibility layer for rredis -
   it tries to detect values that are serialized and unserializes them.
   It also converts RAWs to strings, assuming UTF8 */
SEXP raw_unpack(SEXP sWhat) {
    SEXP r;
    if (TYPEOF(sWhat) == RAWSXP && LENGTH(sWhat) >= 10) {
	unsigned char *a = (unsigned char*) RAW(sWhat);
	/* we check for "X\n\0\0" since the foramt is "X\n" <bigendian int version = 2> */
	if (a[0] == 'X' && a[1] == '\n' && !a[2] && !a[3])
	    return Rf_eval(Rf_lang2(Rf_install("unserialize"), sWhat), R_BaseEnv);
    }
    if (TYPEOF(sWhat) == RAWSXP) { /* we do encode strings as RAW so let's reverse that */
	r = PROTECT(Rf_allocVector(STRSXP, 1));
	SET_STRING_ELT(r, 0, Rf_mkCharLenCE((const char*)RAW(sWhat), LENGTH(sWhat), CE_UTF8));
	UNPROTECT(1);
	return r;
    }
    /* everything else is pass-through */
    return sWhat;
}
