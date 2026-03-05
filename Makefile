.SUFFIXES:

SHELL=/bin/bash
TOPDIR=$(dir $(firstword $(MAKEFILE_LIST)))
INCDIR=$(TOPDIR)inc
SRCDIR=$(TOPDIR)src
EXPDIR=$(TOPDIR)examples
TSTDIR=$(TOPDIR)test
BLDDIR=$(TOPDIR)bld

DEFINITIONS=
DEFINITIONS+=ASIO_STANDALONE
DEFINITIONS+=FMT_HEADER_ONLY

INCLUDES=
INCLUDES+=$(SRCDIR)
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

.PHONY: all test clean lint format tidy coverage
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
	rm -rf $(BLDDIR)

lint:
	$(CXX) $(CXXFLAGS) -fsyntax-only $(ALL_SOURCES)

format:
	$(FORMAT) -i $(TOPDIR)src/raftpp.h $(ALL_SOURCES)

tidy:
	$(TIDY) -fix $(TOPDIR)src/raftpp.h $(ALL_SOURCES)

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

$(TEST_SUITE): CXXFLAGS+=--coverage
$(TEST_SUITE): $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.o)
	$(LINK.cpp) $(OUTPUT_OPTION) $^

$(BLDDIR)/%.o: $(TSTDIR)/%.cpp | $(BLDDIR)
	$(COMPILE.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(*F).d $<

$(BLDDIR)/%: $(EXPDIR)/%.cpp | $(BLDDIR)
	$(LINK.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(*F).d $<

$(BLDDIR):
	mkdir -p $(BLDDIR)

-include $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.d)
-include $(EXAMPLES:%=%.d)
