CXX					= g++
RM					= rm -f

INCLUDE_FLAGS	   := -Iinclude
CXXFLAGS		   := -Wall -std=c++17 $(INCLUDE_FLAGS) -g -lz -lexpat -lbz2

main:
	$(CXX) main.cpp $(CXXFLAGS) -o main

clean: 
	$(RM) main	
