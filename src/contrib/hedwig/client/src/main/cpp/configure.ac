AC_INIT([Hedwig C++ Client], [0.1], [zookeeper-dev@hadoop.apache.org], [hedwig-cpp], [http://hadoop.apache.org/zookeeper//])

AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.9 no-define foreign])
AC_CONFIG_HEADERS([config.h])
AC_PROG_CXX
AC_LANG([C++])
AC_CONFIG_FILES([Makefile lib/Makefile test/Makefile hedwig-0.1.pc])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
PKG_CHECK_MODULES([DEPS], [log4cpp protobuf cppunit])
AX_BOOST_BASE
AX_BOOST_ASIO	  
AX_BOOST_THREAD

DX_HTML_FEATURE(ON)
DX_INIT_DOXYGEN(hedwig-c++, c-doc.Doxyfile, doc)

CXXFLAGS="$CXXFLAGS -Wall"

AC_OUTPUT

