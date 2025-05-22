CXX = g++
CXXFLAGS = -std=c++20 -Wall -Wextra -O2 -pthread
AR = ar
ARFLAGS = rcs

# Source files for the library
SOURCES = MapReduceFramework.cpp Barrier.cpp
OBJECTS = $(SOURCES:.cpp=.o)

# Target library
TARGET = libMapReduceFramework.a

# Default target
all: $(TARGET)

# Build the static library
$(TARGET): $(OBJECTS)
	$(AR) $(ARFLAGS) $@ $^

# Compile source files to object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(OBJECTS) $(TARGET)

# Dependencies (add more as needed)
MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h Barrier.h
Barrier.o: Barrier.cpp Barrier.h

.PHONY: all clean