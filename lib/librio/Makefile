.include <src.opts.mk>

LIB=	rio
PACKAGE=lib${LIB}
SHLIB_MAJOR= 0

SRCS=	rio.c
INCS=	librio.h
#MAN+=	rio.3

CFLAGS+= -I${.CURDIR} \
	 -I/home/ryan/ck/include \
	 -I/home/ryan/ck/include/x86_64

# XXX: Temporary workaround while prototyping RIO with CK
.PATH:	/home/ryan/ck/src
SRCS+=	ck_ec.c

# TODO: manpages
#MLINKS=	rio.3 rio_create.3

HAS_TESTS=
SUBDIR.${MK_TESTS}+= tests

.include <bsd.lib.mk>
