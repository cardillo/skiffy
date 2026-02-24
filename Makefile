CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I src -I inc \
           -DASIO_STANDALONE

BLD_DIR    = bld
TEST_SRCS  = $(wildcard test/*.cpp)
TEST_OBJS  = $(patsubst test/%.cpp,$(BLD_DIR)/%.o,\
             $(TEST_SRCS))
TEST_DEPS  = $(TEST_OBJS:.o=.d)
TEST_BIN   = $(BLD_DIR)/run_tests

KV_SRC    = examples/kv.cpp
KV_BIN    = $(BLD_DIR)/kv
KV_DEP    = $(KV_BIN).d

QUEUE_SRC = examples/queue.cpp
QUEUE_BIN = $(BLD_DIR)/queue
QUEUE_DEP = $(QUEUE_BIN).d

.PHONY: all test clean lint

all: $(TEST_BIN) $(KV_BIN) $(QUEUE_BIN)

$(BLD_DIR):
	mkdir -p $(BLD_DIR)

$(BLD_DIR)/%.o: test/%.cpp | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP -MF $(@:.o=.d) -c -o $@ $<

$(TEST_BIN): $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

test: $(TEST_BIN)
	./$(TEST_BIN)

$(KV_BIN): $(KV_SRC) | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP \
	    -MF $(KV_DEP) -o $@ $< -lpthread

$(QUEUE_BIN): $(QUEUE_SRC) | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP \
	    -MF $(QUEUE_DEP) -o $@ $< -lpthread

lint:
	@echo "lint: checking compilation..."
	$(CXX) $(CXXFLAGS) -fsyntax-only \
	    $(TEST_SRCS) $(KV_SRC) \
	    $(QUEUE_SRC)

clean:
	rm -rf $(BLD_DIR)

-include $(TEST_DEPS) $(KV_DEP) \
         $(QUEUE_DEP)
