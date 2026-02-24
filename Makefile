CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I src -I inc \
           -DASIO_STANDALONE

BLD_DIR    = bld
TEST_SRCS  = $(wildcard test/*.cpp)
TEST_OBJS  = $(patsubst test/%.cpp,$(BLD_DIR)/%.o,\
             $(TEST_SRCS))
TEST_DEPS  = $(TEST_OBJS:.o=.d)
TEST_BIN   = $(BLD_DIR)/run_tests

CLUSTER_SRC   = examples/cluster.cpp
CLUSTER_BIN   = $(BLD_DIR)/cluster
CLUSTER_DEP   = $(CLUSTER_BIN).d

WORKQUEUE_SRC = examples/workqueue.cpp
WORKQUEUE_BIN = $(BLD_DIR)/workqueue
WORKQUEUE_DEP = $(WORKQUEUE_BIN).d

.PHONY: all test clean lint

all: $(TEST_BIN) $(CLUSTER_BIN) $(WORKQUEUE_BIN)

$(BLD_DIR):
	mkdir -p $(BLD_DIR)

$(BLD_DIR)/%.o: test/%.cpp | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP -MF $(@:.o=.d) -c -o $@ $<

$(TEST_BIN): $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

test: $(TEST_BIN)
	./$(TEST_BIN)

$(CLUSTER_BIN): $(CLUSTER_SRC) | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP \
	    -MF $(CLUSTER_DEP) -o $@ $< -lpthread

$(WORKQUEUE_BIN): $(WORKQUEUE_SRC) | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP \
	    -MF $(WORKQUEUE_DEP) -o $@ $< -lpthread

lint:
	@echo "lint: checking compilation..."
	$(CXX) $(CXXFLAGS) -fsyntax-only \
	    $(TEST_SRCS) $(CLUSTER_SRC) \
	    $(WORKQUEUE_SRC)

clean:
	rm -rf $(BLD_DIR)

-include $(TEST_DEPS) $(CLUSTER_DEP) \
         $(WORKQUEUE_DEP)
