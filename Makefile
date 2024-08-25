CXX					= g++
RM					= rm -f
PKG_CONFIG := pkg-congig

ABSL_CFLAGS := $(shell $(PKG_CONFIG) --cflags absl)
ABSL_LIBS := $(shell $(PKG_CONFIG) --libs absl)

INCLUDE_FLAGS	   := -Iinclude -I/opt/homebrew/include -L/opt/homebrew/lib
CXXFLAGS		   := -Wall -std=c++20 $(INCLUDE_FLAGS) -g -lz -ldeflate -lprotobuf -O3 \
    -labsl_log_internal_check_op \
    -labsl_log_internal_conditions \
    -labsl_log_internal_fnmatch \
    -labsl_log_internal_format \
    -labsl_log_internal_globals \
    -labsl_log_internal_log_sink_set \
    -labsl_log_internal_message \
    -labsl_log_internal_nullguard \
    -labsl_log_internal_proto 
SRC				   := ./include/osmpbf/osmformat.pb.cc ./include/osmpbf/fileformat.pb.cc ./include/parsing/primitive-block-parser.cpp \
                        ./include/utils/geomath.cpp ./include/graph/graph.cpp ./include/graph/ways-to-edges.cpp ./include/processing.cpp
# -lz -lexpat -lbz2

main:
	$(CXX) -g main.cpp $(SRC) $(CXXFLAGS) -o main

clean: 
	$(RM) main	
