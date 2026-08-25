# Plain-make build for anyone without CMake: `make` then `./measles_abm`.
CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic -Isrc
SRC := src/config.cpp src/types.cpp src/params.cpp src/population.cpp src/model.cpp src/reporter.cpp

all: measles_abm measles_tests

measles_abm: $(SRC) src/main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC) src/main.cpp

measles_tests: $(SRC) tests/test_abm.cpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC) tests/test_abm.cpp

test: measles_tests
	./measles_tests

clean:
	rm -f measles_abm measles_tests

.PHONY: all test clean
