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

.PHONY: all test clean lint format tidy coverage
all: $(TEST_SUITE) $(EXAMPLES)

test: $(TEST_SUITE)
	$(TEST_SUITE)

coverage:
	$(GCOVR) \
		--root $(TOPDIR) \
		--object-directory $(BLDDIR) \
		--filter $(subst ./,,$(SRCDIR)) \
		--fail-under-line 90 \
		--html-details $(BLDDIR)/coverage.html

clean:
	rm -rf $(BLDDIR)

lint:
	$(CXX) $(CXXFLAGS) -fsyntax-only $(TESTS) $(SOURCES)

format:
	$(FORMAT) -i $(TOPDIR)src/raftpp.h $(TESTS) $(SOURCES)

tidy:
	$(TIDY) $(TOPDIR)src/raftpp.h $(TESTS) $(SOURCES)

run-%: $(BLDDIR)/%
	( ./$< --id 1 --timeout 10 --expected 3 & \
		./$< --id 2 --timeout 15 --bootstrap localhost:9001 & \
		./$< --id 3 --timeout 20 --bootstrap localhost:9001 & \
		wait )

$(TEST_SUITE): CXXFLAGS+=--coverage
$(TEST_SUITE): $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.o)
	$(LINK.cpp) $(OUTPUT_OPTION) $^

$(BLDDIR)/%.o: $(TSTDIR)/%.cpp | $(BLDDIR)
	$(COMPILE.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(<F).d $<

$(BLDDIR)/%: $(EXPDIR)/%.cpp | $(BLDDIR)
	$(LINK.cpp) $(OUTPUT_OPTION) -MMD -MP -MF $(BLDDIR)/$(<F).d $<

$(BLDDIR):
	mkdir -p $(BLDDIR)

-include $(TESTS:$(TSTDIR)/%.cpp=$(BLDDIR)/%.d)
-include $(EXAMPLES:%=%.d)
