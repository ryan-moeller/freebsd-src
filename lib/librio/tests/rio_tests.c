/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <sys/param.h>
#include <sys/uio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <librio.h>

#include <atf-c.h>

/* ring sizes (must be power of 2) */
#define SQLEN	32
#define CQLEN	32
#define NCB	128	/* > SQLEN + CQLEN */
#define POLICY	0
#define TESTCONFIG	SQLEN, CQLEN, NCB, POLICY

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

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write a string to the file. */
	iocb.rio_ident = fd;
	iocb.rio_buf = __DECONST(char *, "test");
	iocb.rio_length = 4;
	ATF_CHECK(rio_write(rio, &iocb, NULL) != -1);
	ATF_CHECK(rio_submit(rio) != -1);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_WRITE);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 4);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);

	/* Read back the string we wrote. */
	iocb.rio_buf = buf;
	ATF_CHECK(rio_read(rio, &iocb, NULL) != -1);
	ATF_CHECK(rio_submit(rio) != -1);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_READ);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 4);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);
	ATF_CHECK_STREQ(buf, "test");

	ATF_CHECK(close(fd) != -1);
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

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write strings to the file using an iovec. */
	fill(&iov[0], "hello");
	fill(&iov[1], " kyua");
	iocb.rio_ident = fd;
	iocb.rio_iov = iov;
	iocb.rio_length = nitems(iov);
	ATF_CHECK(rio_writev(rio, &iocb, NULL) != -1);
	ATF_CHECK(rio_submit(rio) != -1);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_WRITEV);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 10);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);

	/* Read back the string we wrote. */
	iov[0].iov_base = buf1;
	iov[0].iov_len = sizeof(buf1);
	iov[1].iov_base = buf2;
	iov[1].iov_len = sizeof(buf2);
	ATF_CHECK(rio_readv(rio, &iocb, NULL) != -1);
	ATF_CHECK(rio_submit(rio) != -1);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_cmd, RIO_READV);
	ATF_CHECK_INTEQ(iocb.rio_ident, fd);
	ATF_CHECK_INTEQ(iocb.rio_status, 10);
	ATF_CHECK_INTEQ(iocb.rio_error, 0);
	ATF_CHECK_STREQ(buf1, "hello");
	ATF_CHECK_STREQ(buf2, " kyua");

	ATF_CHECK(close(fd) != -1);
	rio_destroy(rio);
}

ATF_TC(errors);
ATF_TC_HEAD(errors, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests error completion");
}

ATF_TC_BODY(errors, tc)
{
	const struct timespec deadline = {0};
	struct riocb iocb = {0};
	rio_t rio = NULL;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);

	/* Try reading an invalid file into an invalid buffer. */
	iocb.rio_ident = -1;
	iocb.rio_buf = NULL;
	iocb.rio_length = 0;
	/* SQLEN - 1 because the ring always has an empty marker slot. */
	for (int i = 0; i < SQLEN - 1; i++) {
		/* Non-blocking submissions succeed with space available. */
		ATF_CHECK(rio_read(rio, &iocb, &deadline) != -1);
	}
	/* Non-blocking submission fails ETIMEDOUT when out of space. */
	ATF_CHECK_ERRNO(ETIMEDOUT, rio_read(rio, &iocb, &deadline));
	/* Now we must submit. */
	ATF_CHECK(rio_submit(rio) != -1);
	for (int i = 0; i < SQLEN - 1; i++) {
		/* Blocking submissions succeed until full again. */
		ATF_CHECK(rio_read(rio, &iocb, NULL) != -1);
	}
	ATF_CHECK_ERRNO(ETIMEDOUT, rio_read(rio, &iocb, &deadline));
	/*
	 * Submit the next batch.  This succeeds, but we get kicked out of the
	 * issuer queue because the completion queue is not being drained.
	 */
	ATF_CHECK(rio_submit(rio) != -1);
	/* Now the completion queue is full and the issuer stopped issuing. */
	for (int i = 0; i < CQLEN - 1; i++) {
		/* Blocking dequeues succeed while completions are available. */
		ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
		ATF_CHECK_INTEQ(iocb.rio_ident, -1);
		ATF_CHECK_INTEQ(iocb.rio_status, -1);
		ATF_CHECK_INTEQ(iocb.rio_error, EBADF);
	}
	/* Non-blocking dequeue fails, nothing left. */
	ATF_CHECK_ERRNO(ETIMEDOUT, rio_poll(rio, &iocb, &deadline));
	/* Submit again to issue the remaining submissions. */
	ATF_CHECK(rio_submit(rio) != -1);
	for (int i = 0; i < CQLEN - 1; i++) {
		/* Blocking dequeues succeed while completions are available. */
		ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
		ATF_CHECK_INTEQ(iocb.rio_ident, -1);
		ATF_CHECK_INTEQ(iocb.rio_status, -1);
		ATF_CHECK_INTEQ(iocb.rio_error, EBADF);
	}
	ATF_CHECK_ERRNO(ETIMEDOUT, rio_poll(rio, &iocb, &deadline));

	rio_destroy(rio);
}

ATF_TC(saturation);
ATF_TC_HEAD(saturation, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests full ring blocking");
}

ATF_TC_BODY(saturation, tc)
{
	const int limit = 1000;
	const struct timespec deadline = {0};
	struct riocb iocb = {0};
	rio_t rio = NULL;
	int fd, s, c;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write a string to the file. */
	iocb.rio_ident = fd;
	iocb.rio_buf = __DECONST(char *, "test");
	iocb.rio_length = 4;
	/* TODO: Test with multiple threads and different qlens/ncb. */
	s = c = 0;
	while (s < limit) {
		/* Non-blocking enqueue, submit when full. */
		while (rio_write(rio, &iocb, &deadline) != -1) {
			s++;
		}
		ATF_CHECK_INTEQ(ETIMEDOUT, errno);
		/* Submission queue full. */
		ATF_CHECK(rio_submit(rio) != -1);
		/* Blocking enqueue, submit when full again. */
		for (int i = 0; i < SQLEN - 1; i++) {
			ATF_CHECK(rio_fsync(rio, &iocb, NULL) != -1);
			s++;
		}
		/* Submissions full. */
		ATF_CHECK_ERRNO(ETIMEDOUT, rio_fsync(rio, &iocb, &deadline));
		/* Can submit, but we're immediately kicked off the issuer. */
		ATF_CHECK(rio_submit(rio) != -1);
		/*
		 * The submissions being full is our hint to drain completions.
		 */
		while (s - c > CQLEN - 1) {
			ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
			ATF_CHECK_INTEQ(iocb.rio_ident, fd);
			ATF_CHECK_INTEQ(iocb.rio_error, 0);
			c++;
		}
		/*
		 * We were kicked off the issuer for not draining completions
		 * earlier, so the queue must be resubmit before it starts
		 * being issued.
		 */
		ATF_CHECK_ERRNO(ETIMEDOUT, rio_read(rio, &iocb, &deadline));
		ATF_CHECK(rio_submit(rio) != -1);
		/* We have to keep polling to make progress. */
		while (s - c > 0) {
			ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
			ATF_CHECK_INTEQ(iocb.rio_ident, fd);
			ATF_CHECK_INTEQ(iocb.rio_error, 0);
			c++;
		}
	}
	/* Drain remaining completions. */
	while (s > c) {
		ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
		ATF_CHECK_INTEQ(iocb.rio_ident, fd);
		ATF_CHECK_INTEQ(iocb.rio_error, 0);
		c++;
	}

	ATF_CHECK(close(fd) != -1);
	rio_destroy(rio);
}

ATF_TC(cancel);
ATF_TC_HEAD(cancel, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests cancellation");
}

ATF_TC_BODY(cancel, tc)
{
	struct riocb iocb = {0};
	rio_t rio = NULL;
	int cookie;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);

	/* We're going to mlock NULL... */
	ATF_CHECK((cookie = rio_mlock(rio, &iocb, NULL)) != -1);
	/* On second thought, let's not. */
	ATF_CHECK(rio_cancel(rio, cookie) != -1);
	/* Have to go through the motions to flush out the iocb. */
	ATF_CHECK(rio_submit(rio) != -1);
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_error, ECANCELED);

	rio_destroy(rio);
}

ATF_TP_ADD_TCS(tp)
{
	ATF_TP_ADD_TC(tp, create_destroy);
	ATF_TP_ADD_TC(tp, polling);
	ATF_TP_ADD_TC(tp, vectors);
	ATF_TP_ADD_TC(tp, errors);
	ATF_TP_ADD_TC(tp, saturation);
	ATF_TP_ADD_TC(tp, cancel);
	return (atf_no_error());
}
