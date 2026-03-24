.SUFFIXES:

SHELL=/bin/bash
TOPDIR=$(dir $(firstword $(MAKEFILE_LIST)))
INCDIR=$(TOPDIR)inc
SRCDIR=$(TOPDIR)src
EXPDIR=$(TOPDIR)examples
TSTDIR=$(TOPDIR)test
BLDDIR=$(TOPDIR)bld
DISTDIR=$(TOPDIR)dist

ASIO_TAG=asio-1-36-0
SML_VERSION=1.1.13
SPDLOG_VERSION=1.17.0
CXXOPTS_VERSION=3.3.1
HTTPLIB_VERSION=0.34.0
MSGPACK_VERSION=7.0.0
MSGPACK_TAG=cpp-$(MSGPACK_VERSION)
NANOBENCH_VERSION=4.3.11
DOCTEST_VERSION=2.4.11

GITHUB=https://github.com
RAW=https://raw.githubusercontent.com

ASIO_URL=$(GITHUB)/chriskohlhoff/asio/archive/refs/tags/$(ASIO_TAG).tar.gz
SML_URL=$(RAW)/boost-ext/sml/v$(SML_VERSION)/include/boost/sml.hpp
SPDLOG_URL=$(GITHUB)/gabime/spdlog/archive/refs/tags/v$(SPDLOG_VERSION).tar.gz
CXXOPTS_URL=$(RAW)/jarro2783/cxxopts/v$(CXXOPTS_VERSION)/include/cxxopts.hpp
HTTPLIB_URL=$(RAW)/yhirose/cpp-httplib/v$(HTTPLIB_VERSION)/httplib.h
MSGPACK_URL=$(GITHUB)/msgpack/msgpack-c/archive/refs/tags/$(MSGPACK_TAG).tar.gz
NANOBENCH_URL=$(RAW)/martinus/nanobench/v$(NANOBENCH_VERSION)/src/include/nanobench.h
DOCTEST_URL=$(RAW)/doctest/doctest/v$(DOCTEST_VERSION)/doctest/doctest.h

DEFINITIONS=
DEFINITIONS+=SKIFFY_ENABLE_SPDLOG
DEFINITIONS+=SKIFFY_ENABLE_ASIO
DEFINITIONS+=ASIO_STANDALONE
DEFINITIONS+=FMT_HEADER_ONLY

INCLUDES=
INCLUDES+=$(INCDIR)

CXX=g++
CXXFLAGS=
CXXFLAGS+=-std=c++17
CXXFLAGS+=-Wall
CXXFLAGS+=-Wextra
CXXFLAGS+=-pthread
CXXFLAGS+=-g
CXXFLAGS+=$(DEFINITIONS:%=-D%)
CXXFLAGS+=$(INCLUDES:%=-I%)

FORMAT=clang-format
TIDY=clang-tidy
GCOVR=gcovr

SOURCES=$(wildcard $(EXPDIR)/*.cpp)
EXAMPLES=$(SOURCES:$(EXPDIR)/%.cpp=$(BLDDIR)/%)

TESTS=$(wildcard $(TSTDIR)/*.cpp)
TEST_SUITE=$(BLDDIR)/run_tests

ALL_SOURCES=$(wildcard $(SRCDIR)/*.h) \
			$(wildcard $(TSTDIR)/*.h) $(TESTS) $(SOURCES)

.PHONY: all test clean lint format tidy coverage dist variants deps
all: $(TEST_SUITE) $(EXAMPLES)

test: $(TEST_SUITE)
	$(MEMCHECK) $(TEST_SUITE)

coverage: test
	$(GCOVR) \
		--root $(TOPDIR) \
		--object-directory $(BLDDIR) \
		--filter $(subst ./,,$(SRCDIR)) \
		--fail-under-line 80 \
		--html-details $(BLDDIR)/coverage.html

clean:
	rm -rf $(BLDDIR) $(DISTDIR)

deps: $(INCDIR)/asio.hpp \
	$(INCDIR)/boost/sml.hpp \
	$(INCDIR)/spdlog/spdlog.h \
	$(INCDIR)/cxxopts.hpp \
	$(INCDIR)/httplib.h \
	$(INCDIR)/msgpack.hpp \
	$(INCDIR)/nanobench.h \
	$(INCDIR)/doctest/doctest.h

$(INCDIR) $(INCDIR)/boost $(INCDIR)/doctest:
	mkdir -p $@

$(INCDIR)/boost/sml.hpp: URL=$(SML_URL)
$(INCDIR)/boost/sml.hpp: | $(INCDIR)/boost
$(INCDIR)/doctest/doctest.h: URL=$(DOCTEST_URL)
$(INCDIR)/doctest/doctest.h: | $(INCDIR)/doctest
$(INCDIR)/cxxopts.hpp: URL=$(CXXOPTS_URL)
$(INCDIR)/httplib.h: URL=$(HTTPLIB_URL)
$(INCDIR)/nanobench.h: URL=$(NANOBENCH_URL)

$(INCDIR)/cxxopts.hpp $(INCDIR)/boost/sml.hpp \
$(INCDIR)/httplib.h $(INCDIR)/nanobench.h \
$(INCDIR)/doctest/doctest.h: | $(INCDIR)
	curl -fsSL $(URL) -o $@

$(INCDIR)/asio.hpp: | $(INCDIR)
	curl -fsSL $(ASIO_URL) \
		| tar -xz -C $(INCDIR) --strip-components=3 \
		asio-$(ASIO_TAG)/asio/include/asio.hpp \
		asio-$(ASIO_TAG)/asio/include/asio

$(INCDIR)/spdlog/spdlog.h: | $(INCDIR)
	curl -fsSL $(SPDLOG_URL) \
		| tar -xz -C $(INCDIR) --strip-components=2 \
		spdlog-$(SPDLOG_VERSION)/include/spdlog

$(INCDIR)/msgpack.hpp: | $(INCDIR)
	curl -fsSL $(MSGPACK_URL) \
		| tar -xz -C $(INCDIR) --strip-components=2 \
		msgpack-c-$(MSGPACK_TAG)/include/msgpack.hpp \
		msgpack-c-$(MSGPACK_TAG)/include/msgpack

variants: DEFINITIONS=
variants:
	$(COMPILE.cc) -fsyntax-only $(SRCDIR)/skiffy.hpp \
		-DSKIFFY_ENABLE_SPDLOG -DFMT_HEADER_ONLY \
		-DSKIFFY_ENABLE_ASIO -DASIO_STANDALONE
	$(COMPILE.cc) -fsyntax-only $(SRCDIR)/skiffy.hpp \
		-DSKIFFY_ENABLE_ASIO -DASIO_STANDALONE
	$(COMPILE.cc) -fsyntax-only $(SRCDIR)/skiffy.hpp \
		-DSKIFFY_ENABLE_SPDLOG -DFMT_HEADER_ONLY
	$(COMPILE.cc) -fsyntax-only $(SRCDIR)/skiffy.hpp
	@echo "all variants ok"

lint: CXXFLAGS+=-I$(SRCDIR)
lint:
	$(CXX) $(CXXFLAGS) -fsyntax-only $(ALL_SOURCES)

format:
	$(FORMAT) -i $(TOPDIR)src/skiffy.hpp $(ALL_SOURCES)

tidy:
	$(TIDY) -fix $(TOPDIR)src/skiffy.hpp $(ALL_SOURCES)

dist: $(DISTDIR)/skiffy.hpp

$(DISTDIR)/skiffy.hpp: $(SRCDIR)/skiffy.hpp \
		$(INCDIR)/boost/sml.hpp $(INCDIR)/msgpack.hpp
	@mkdir -p $(DISTDIR)
	awk -v d=$(INCDIR) \
		'/^#include "boost\/sml\.hpp"/ \
		 { while ((getline l < (d"/boost/sml.hpp")) > 0) print l; next } \
		 /^#include "msgpack\.hpp"/ \
		 { while ((getline l < (d"/msgpack.hpp")) > 0) print l; next } \
		 { print }' $< > $@

run-%: $(BLDDIR)/%
	( ./$< --port 9001 --timeout 10 & \
		./$< --port 9002 --timeout 15 --bootstrap localhost:9001 & \
		./$< --port 9003 --timeout 20 --bootstrap localhost:9001 & \
		wait )

run-http: $(BLDDIR)/http_server $(BLDDIR)/http_client
	( ./$(BLDDIR)/http_server --port 9001 --timeout 35 \
			--log-dir $(BLDDIR)/data & \
		sleep 0.3 ; \
		./$(BLDDIR)/http_server --port 9002 --timeout 35 \
			--log-dir $(BLDDIR)/data --bootstrap localhost:9001 & \
		./$(BLDDIR)/http_server --port 9003 --timeout 35 \
			--log-dir $(BLDDIR)/data --bootstrap localhost:9001 & \
		sleep 2 ; \
		NANOBENCH_SUPPRESS_WARNINGS=1 \
		./$(BLDDIR)/http_client \
			--servers localhost:10001,localhost:10002,localhost:10003 \
			--connections 50 --payload-size 256 ; \
		wait )

$(TEST_SUITE): CXXFLAGS+=--coverage -I$(SRCDIR)
$(TEST_SUITE): $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.o)
	$(LINK.cpp) $(OUTPUT_OPTION) $^

$(BLDDIR)/%.o: $(TSTDIR)/%.cpp | $(BLDDIR)
	$(COMPILE.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(*F).d $<

$(EXAMPLES): CXXFLAGS+=-I$(DISTDIR)
$(EXAMPLES): $(DISTDIR)/skiffy.hpp
$(BLDDIR)/%: $(EXPDIR)/%.cpp | $(BLDDIR)
	$(LINK.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(*F).d $<

$(BLDDIR):
	mkdir -p $(BLDDIR)

-include $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.d)
-include $(EXAMPLES:%=%.d)
