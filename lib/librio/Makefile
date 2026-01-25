.include <src.opts.mk>

LIB=	rio
PACKAGE=lib${LIB}
SHLIB_MAJOR= 0

SRCS=	rio.c
INCS=	librio.h
#MAN+=	rio.3

CKTOP=	${SRCTOP}/sys/contrib/ck

CFLAGS+= \
	-I${SRCTOP}/sys \
	-I${CKTOP}/include \
	-I${CKTOP}/include/${MACHINE_CPUARCH} \

.PATH:	${CKTOP}/src
SRCS+=	ck_ec.c

# TODO: manpages
#MLINKS=	rio.3 rio_create.3

HAS_TESTS=
SUBDIR.${MK_TESTS}+= tests

.include <bsd.lib.mk>
