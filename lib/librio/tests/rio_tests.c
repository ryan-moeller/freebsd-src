/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright (c) 2026 Ryan Moeller
 */

#include <fcntl.h>
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
	rio_t rio = NULL;
	struct riocb *iocb;
	int fd;

	ATF_REQUIRE(rio_create(&rio, TESTCONFIG) == 0);
	ATF_REQUIRE((fd = open("testfile", O_CREAT | O_RDWR, 0444)) != -1);

	/* Write a string to the file. */
	ATF_CHECK(rio_write(rio, fd, "test", 4, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK((iocb = rio_poll(rio, NULL)) != NULL);
	ATF_CHECK_INTEQ(iocb->rio_cmd, RIO_WRITE);
	ATF_CHECK_INTEQ(iocb->rio_ident, fd);
	ATF_CHECK_INTEQ(iocb->rio_status, 4);
	ATF_CHECK_INTEQ(iocb->rio_error, 0);
	rio_return(rio, iocb);

	/* Read back the string we wrote. */
	ATF_CHECK(rio_read(rio, fd, buf, 4, NULL) == 0);
	ATF_CHECK(rio_submit(rio) == 0);
	ATF_CHECK((iocb = rio_poll(rio, NULL)) != NULL);
	ATF_CHECK_INTEQ(iocb->rio_cmd, RIO_READ);
	ATF_CHECK_INTEQ(iocb->rio_ident, fd);
	ATF_CHECK_INTEQ(iocb->rio_status, 4);
	ATF_CHECK_INTEQ(iocb->rio_error, 0);
	rio_return(rio, iocb);
	ATF_CHECK_STREQ(buf, "test");

	ATF_CHECK(close(fd) == 0);
	rio_destroy(rio);
}

ATF_TP_ADD_TCS(tp)
{
	ATF_TP_ADD_TC(tp, create_destroy);
	ATF_TP_ADD_TC(tp, polling);
	return (atf_no_error());
}
