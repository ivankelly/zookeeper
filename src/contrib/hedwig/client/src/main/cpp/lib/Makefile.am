PROTODEF = ../../../../../protocol/src/main/protobuf/PubSubProtocol.proto

lib_LTLIBRARIES = libhedwig01.la
libhedwig01_la_SOURCES = protocol.cpp channel.cpp client.cpp util.cpp clientimpl.cpp publisherimpl.cpp subscriberimpl.cpp eventdispatcher.cpp data.cpp
libhedwig01_la_CPPFLAGS = -I$(top_srcdir)/inc $(DEPS_CFLAGS)
libhedwig01_la_LIBADD = $(DEPS_LIBS) $(BOOST_CPPFLAGS) 
libhedwig01_la_LDFLAGS = -no-undefined $(BOOST_ASIO_LIB) $(BOOST_LDFLAGS) $(BOOST_THREAD_LIB)

protocol.cpp: $(PROTODEF)
	protoc --cpp_out=. -I`dirname $(PROTODEF)` $(PROTODEF)
	sed "s/PubSubProtocol.pb.h/hedwig\/protocol.h/" PubSubProtocol.pb.cc > protocol.cpp
	rm PubSubProtocol.pb.cc
	mv PubSubProtocol.pb.h $(top_srcdir)/inc/hedwig/protocol.h

