bin_PROGRAMS = hedwigtest
hedwigtest_SOURCES = main.cpp utiltest.cpp pubsubdatatest.cpp publishtest.cpp subscribetest.cpp servercontrol.cpp pubsubtest.cpp
hedwigtest_CPPFLAGS = -I$(top_srcdir)/inc $(DEPS_CFLAGS) $(BOOST_CPPFLAGS) 
hedwigtest_LDADD = $(DEPS_LIBS) -L$(top_builddir)/lib -lhedwig01 
hedwigtest_LDFLAGS = -no-undefined $(BOOST_ASIO_LIB) $(BOOST_LDFLAGS) $(BOOST_THREAD_LIB)

check: hedwigtest
	sh ../scripts/run-server-control.sh start
	sleep 5
	./hedwigtest
	sh ../scripts/run-server-control.sh stop