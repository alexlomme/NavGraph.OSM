CXX					= g++
RM					= rm -f

INCLUDE_FLAGS	   := -Iinclude
CXXFLAGS		   := -Wall -std=c++17 $(INCLUDE_FLAGS) -g -lz -lprotobuf
SRC				   := ./include/osmpbf/osmformat.pb.cc ./include/osmpbf/fileformat.pb.cc
# -lz -lexpat -lbz2

main:
	$(CXX) main.cpp $(SRC) $(CXXFLAGS) -o main

clean: 
	$(RM) main	
