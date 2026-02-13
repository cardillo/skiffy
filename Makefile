CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I src -I inc

BLD_DIR   = bld
TEST_SRCS = $(wildcard test/*.cpp)
TEST_OBJS = $(patsubst test/%.cpp,$(BLD_DIR)/%.o,$(TEST_SRCS))
TEST_BIN  = $(BLD_DIR)/run_tests

.PHONY: all test clean lint

all: test

$(BLD_DIR):
	mkdir -p $(BLD_DIR)

$(BLD_DIR)/%.o: test/%.cpp | $(BLD_DIR)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(TEST_BIN): $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

test: $(TEST_BIN)
	./$(TEST_BIN)

lint:
	@echo "lint: checking compilation..."
	$(CXX) $(CXXFLAGS) -fsyntax-only $(TEST_SRCS)

clean:
	rm -rf $(BLD_DIR)
