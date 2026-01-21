/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <sys/param.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <librio.h>

#include <atf-c.h>

#define TESTCONFIG 32, 32, 32, 0

ATF_TC(create_destroy);
ATF_TC_HEAD(create_destroy, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests rio_create and rio_destroy");
}

ATF_TC_BODY(create_destroy, tc)
{
	rio_t rio = NULL;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) == 0);
	ATF_CHECK(rio != NULL);
	rio_destroy(rio);
}

ATF_TC(polling);
ATF_TC_HEAD(polling, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests polling various IO commands");
}

ATF_TC_BODY(polling, tc)
{
	char buf[5] = {'\0'};
	struct riocb iocb = {0};
	rio_t rio = NULL;
	int fd;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) == 0);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write a string to the file. */
	iocb.rio_ident = fd;
	iocb.rio_buf = __DECONST(char *, "test");
	iocb.rio_length = 4;
	ATF_CHECK(rio_write(rio, &iocb, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) == 0);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_WRITE);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 4);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);

	/* Read back the string we wrote. */
	iocb.rio_buf = buf;
	ATF_CHECK(rio_read(rio, &iocb, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) == 0);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_READ);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 4);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);
	ATF_CHECK_STREQ(buf, "test");

	ATF_CHECK(close(fd) == 0);
	rio_destroy(rio);
}

ATF_TC(vectors);
ATF_TC_HEAD(vectors, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests polling vectored IO commands");
}

static inline void
fill(struct iovec *iov, const char *s)
{
	iov->iov_base = __DECONST(char *, s);
	iov->iov_len = strlen(s);
}

ATF_TC_BODY(vectors, tc)
{
	char buf1[5] = {'\0'};
	char buf2[5] = {'\0'};
	struct iovec iov[2];
	struct riocb iocb = {0};
	rio_t rio = NULL;
	int fd;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) == 0);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write strings to the file using an iovec. */
	fill(&iov[0], "hello");
	fill(&iov[1], " kyua");
	iocb.rio_ident = fd;
	iocb.rio_iov = iov;
	iocb.rio_length = nitems(iov);
	ATF_CHECK(rio_writev(rio, &iocb, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) == 0);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_WRITEV);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 10);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);

	/* Read back the string we wrote. */
	iov[0].iov_base = buf1;
	iov[0].iov_len = sizeof(buf1);
	iov[1].iov_base = buf2;
	iov[1].iov_len = sizeof(buf2);
	ATF_CHECK(rio_readv(rio, &iocb, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) == 0);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_READV);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 10);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);
	ATF_CHECK_STREQ(buf1, "hello");
	ATF_CHECK_STREQ(buf2, " kyua");

	ATF_CHECK(close(fd) == 0);
	rio_destroy(rio);
}

ATF_TP_ADD_TCS(tp)
{
	ATF_TP_ADD_TC(tp, create_destroy);
	ATF_TP_ADD_TC(tp, polling);
	ATF_TP_ADD_TC(tp, vectors);
	return (atf_no_error());
}
