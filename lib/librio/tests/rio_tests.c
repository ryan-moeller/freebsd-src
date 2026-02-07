/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <sys/param.h>
#include <sys/mdioctl.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>
#include <fcntl.h>
#include <paths.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <librio.h>

#include <atf-c.h>

/* ring sizes (must be power of 2) */
#define SQLEN	32
#define CQLEN	32
#define NCB	128	/* > SQLEN + CQLEN */
#define POLICY	RIO_POLICY_SOFT_AFFINITY
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
	int fd, q, s, c;

	ATF_REQUIRE(CQLEN <= SQLEN);
	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write a string to the file. */
	iocb.rio_ident = fd;
	iocb.rio_buf = __DECONST(char *, "test");
	iocb.rio_length = 4;
	/* TODO: Test with multiple threads and different qlens/ncb. */
	q = s = c = 0;
	while (s < limit) {
		ATF_REQUIRE(q == s);
		ATF_REQUIRE(s == c);
		/* Non-blocking enqueue, submit when full. */
		for (; q - s < SQLEN - 1; q++) {
			ATF_CHECK(rio_write(rio, &iocb, &deadline) != -1);
		}
		if (s == 0) {
			/* Submission queue full on first iteration. */
			ATF_CHECK(rio_write(rio, &iocb, &deadline) == -1);
			ATF_CHECK_INTEQ(errno, ETIMEDOUT);
		}
		ATF_CHECK(rio_submit(rio) != -1);
		s = q;
		/*
		 * Blocking enqueue, submit when full again.  The completion
		 * ring size will make space available in the submission ring.
		 */
		for (; q - s < CQLEN - 1; q++) {
			ATF_CHECK(rio_fsync(rio, &iocb, NULL) != -1);
		}
		/* Submissions full. */
		ATF_CHECK(rio_fsync(rio, &iocb, &deadline) == -1);
		ATF_CHECK_INTEQ(errno, ETIMEDOUT);
		/* Can submit, but we're immediately kicked off the issuer. */
		ATF_CHECK(rio_submit(rio) != -1);
		s = q;
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
		ATF_CHECK(rio_read(rio, &iocb, &deadline) == -1);
		ATF_CHECK_INTEQ(errno, ETIMEDOUT);
		ATF_CHECK(rio_submit(rio) != -1);
		s = q;
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
	ATF_CHECK(q == s);
	ATF_CHECK(s == c);

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

ATF_TC(error_status);
ATF_TC_HEAD(error_status, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests error status changes");
}

ATF_TC_BODY(error_status, tc)
{
	char buf;
	struct riocb iocb = {0};
	int sd[2];
	rio_t rio = NULL;
	int cookie;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	ATF_REQUIRE(socketpair(PF_LOCAL, SOCK_STREAM, 0, sd) != -1);

	iocb.rio_ident = sd[0];
	iocb.rio_buf = &buf;
	iocb.rio_length = sizeof(buf);
	ATF_CHECK((cookie = rio_read(rio, &iocb, NULL)) != -1);
	/* Initially the error stays 0 while sitting in queues. */
	ATF_CHECK(rio_error(rio, cookie) == 0);
	ATF_CHECK(rio_submit(rio) != -1);
	/*
	 * The error will still be 0 until a worker is about to do the I/O.
	 * For a socket, the control block remains queued on the socket buffer
	 * until woken.  Only then will it be issued to a worker.
	 */
	ATF_CHECK_INTEQ(rio_error(rio, cookie), 0);
#if 0 /* XXX: Can't reliably test this with a socketpair, it's non-blocking. */
	/* Now we're blocked in the kernel waiting on the socket buffer. */
	ATF_CHECK_INTEQ(rio_error(rio, cookie), EINPROGRESS);
	/* Too late to cancel. */
	ATF_CHECK_ERRNO(EINPROGRESS, rio_cancel(rio, cookie));
#endif
	/* Let it go. */
	buf = 'R';
	ATF_CHECK_INTEQ(write(sd[1], &buf, sizeof(buf)), sizeof(buf));
	ATF_CHECK(rio_poll(rio, &iocb, NULL) != -1);
	ATF_CHECK_INTEQ(iocb.rio_ident, sd[0]);
	ATF_CHECK_INTEQ(iocb.rio_status, sizeof(buf));
	ATF_CHECK_INTEQ(iocb.rio_error, 0);
	ATF_CHECK_INTEQ(buf, 'R');

	ATF_CHECK(close(sd[0]) != -1);
	ATF_CHECK(close(sd[1]) != -1);
	rio_destroy(rio);
}

#define MDUNIT_LINK	"mdunit_link"

static int
md_setup(void)
{
	char buf[PATH_MAX];
	struct md_ioctl mdio;
	int fd;

	fd = open(_PATH_DEV MDCTL_NAME, O_RDWR, 0);
	ATF_REQUIRE_MSG(fd != -1,
	    "opening %s%s failed: %s", _PATH_DEV, MDCTL_NAME,
	    strerror(errno));

	memset(&mdio, 0, sizeof(mdio));
	mdio.md_version = MDIOVERSION;
	mdio.md_type = MD_MALLOC;
	mdio.md_options = MD_AUTOUNIT | MD_COMPRESS;
	mdio.md_mediasize = 1 << 20; /* 1 MiB */
	mdio.md_sectorsize = 512;
	strlcpy(buf, __func__, sizeof(buf));
	mdio.md_label = buf;

	ATF_REQUIRE_MSG(ioctl(fd, MDIOCATTACH, &mdio) != -1,
	    "ioctl MDIOCATTACH failed: %s", strerror(errno));
	close(fd);

	snprintf(buf, sizeof(buf), "%d", mdio.md_unit);
	ATF_REQUIRE_MSG(symlink(buf, MDUNIT_LINK) != -1,
	    "symlink %s failed: %s", buf, strerror(errno));
	snprintf(buf, sizeof(buf), _PATH_DEV MD_NAME "%d", mdio.md_unit);
	fd = open(buf, O_RDWR);
	ATF_REQUIRE_MSG(fd != -1,
	    "opening %s failed: %s", buf, strerror(errno));

	return (fd);
}

static void
md_cleanup(void)
{
	char buf[PATH_MAX];
	struct md_ioctl mdio;
	int fd, n;

	fd = open(_PATH_DEV MDCTL_NAME, O_RDWR, 0);
	if (fd == -1) {
		fprintf(stderr, "opening %s%s failed: %s\n", _PATH_DEV,
		    MDCTL_NAME, strerror(errno));
		return;
	}
	n = readlink(MDUNIT_LINK, buf, sizeof(buf) - 1);
	if (n > 0) {
		buf[n] = '\0';
		memset(&mdio, 0, sizeof(mdio));
		if (sscanf(buf, "%d", &mdio.md_unit) > 0 && mdio.md_unit >= 0) {
			mdio.md_version = MDIOVERSION;
			if (ioctl(fd, MDIOCDETACH, &mdio) == -1) {
				fprintf(stderr,
				    "ioctl MDIOCDETACH unit %d failed: %s\n",
				    mdio.md_unit, strerror(errno));
			}
		}
	}
	close(fd);
}

static inline void
fill_buffer(char *buf, size_t len, long seed)
{
	srandom(seed);
	for (u_int i = 0; i < len; i++) {
		buf[i] = random() & 0xff;
	}
}

static inline bool
test_buffer(char *buf, size_t len, long seed)
{
	srandom(seed);
	for (u_int i = 0; i < len; i++) {
		char c = random() & 0xff;

		if (buf[i] != c) {
			fprintf(stderr, "%c != %c\n", buf[i], c);
			return (false);
		}
	}
	return (true);
}

ATF_TC_WITH_CLEANUP(cdev);
ATF_TC_HEAD(cdev, tc)
{
	atf_tc_set_md_var(tc, "descr", "Tests cdev bio issuing");
	atf_tc_set_md_var(tc, "require.user", "root");
	atf_tc_set_md_var(tc, "require.kmods", "g_md");
}

ATF_TC_BODY(cdev, tc)
{
	char buf[16384];
	rio_t rio;
	struct riocb iocb;
	struct iovec iov;
	long seed;

	srandomdev();
	seed = random();
	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) != -1);
	iocb.rio_ident = md_setup();
	iocb.rio_offset = 0;
	iocb.rio_buf = buf;
	iocb.rio_length = sizeof(buf);
	fill_buffer(buf, sizeof(buf), seed);
	ATF_REQUIRE_MSG(rio_write(rio, &iocb, NULL) != -1,
	    "rio_write failed: %s", strerror(errno));
	ATF_REQUIRE_MSG(rio_submit(rio) != -1,
	    "rio_submit failed: %s", strerror(errno));
	ATF_REQUIRE_MSG(rio_poll(rio, &iocb, NULL) != -1,
	    "rio_poll failed: %s", strerror(errno));
	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	iocb.rio_iov = &iov;
	iocb.rio_length = 1;
	ATF_REQUIRE_MSG(rio_readv(rio, &iocb, NULL) != -1,
	    "rio_readv failed: %s", strerror(errno));
	ATF_REQUIRE_MSG(rio_submit(rio) != -1,
	    "rio_submit failed: %s", strerror(errno));
	ATF_REQUIRE_MSG(rio_poll(rio, &iocb, NULL) != -1,
	    "rio_poll failed: %s", strerror(errno));
	ATF_REQUIRE_INTEQ(iocb.rio_error, 0);
	ATF_REQUIRE_INTEQ(iocb.rio_status, sizeof(buf));
	/* buffer check */
	ATF_REQUIRE(test_buffer(buf, sizeof(buf), seed));

	close(iocb.rio_ident);
	rio_destroy(rio);
}

ATF_TC_CLEANUP(cdev, tc)
{
	md_cleanup();
}

/* TODO: test different policies */

ATF_TP_ADD_TCS(tp)
{
	ATF_TP_ADD_TC(tp, create_destroy);
	ATF_TP_ADD_TC(tp, polling);
	ATF_TP_ADD_TC(tp, vectors);
	ATF_TP_ADD_TC(tp, errors);
	ATF_TP_ADD_TC(tp, saturation);
	ATF_TP_ADD_TC(tp, cancel);
	ATF_TP_ADD_TC(tp, error_status);
	ATF_TP_ADD_TC(tp, cdev);
	return (atf_no_error());
}
