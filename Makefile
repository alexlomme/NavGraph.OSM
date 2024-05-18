CXX					= g++
# RM					= rm -f

INCLUDE_FLAGS	   := -Iinclude
# DEBUG_FLAGS		   := -fsanitize=undefined
# SANITIZE_FLAGS	   := -fno-omit-frame-pointer -fsanitize=address
# RELEASE_FLAGS	   := -DNDEBUG
CXXFLAGS		   := -Wall -std=c++20 -I/usr/local/include -L/usr/local/lib

# BINDIR				= bin
# SRCDIR				= tasks
# RSRCDIR				= resources

# ## rebuild w/o file changes if target changed
# TARGET_MARKER      := $(BINDIR)/.$(target).target
# $(TARGET_MARKER):
# 	mkdir -p $(BINDIR)
# 	$(RM) $(BINDIR)/.*.target
# 	touch $(TARGET_MARKER)

# ## rebuild w/o file changes if variant changed
# VARIANT_MARKER      := $(BINDIR)/.$(variant).variant
# $(VARIANT_MARKER):
# 	mkdir -p $(BINDIR)
# 	$(RM) $(BINDIR)/.*.variant
# 	touch $(VARIANT_MARKER)

# ## build targets
# $(BINDIR)/%.out: $(TARGET_MARKER) $(SRCDIR)/%.cpp
# 	$(CXX) $(patsubst $(BINDIR)/%.out, $(SRCDIR)/%.cpp, $@) $(CXXFLAGS) -o $@


# ## if quiet flag is set, don't ouptput generated code
# CODEGEN_COMMAND := $(if $(quiet),clang-format --style=WebKit >, clang-format --style=WebKit | tee)

# get_task = $(basename $(word 3,$(subst -, ,$1)))

# VARIANT_FLAG = -DVARIANT_$(variant) -DVARIANT_NAME=$(variant)

# ## special rules for query compiler
# $(BINDIR)/p2c-task-%.out: $(TARGET_MARKER) $(VARIANT_MARKER) $(SRCDIR)/p2c-task-%.cpp $(RSRCDIR)/queryFrame.cpp $(SRCDIR)/additionaloperators.hpp
# 	$(CXX) -std=c++20 $(patsubst $(BINDIR)/p2c-task-%.out, $(SRCDIR)/p2c-task-%.cpp, $@) $(CXXFLAGS) $(VARIANT_FLAG) -O0 -g -o $(subst task,tmp,$@)
# 	cp $(RSRCDIR)/queryFrame.cpp $(BINDIR)/queryFrame.cpp
# 	$(subst task,tmp,$@) | $(CODEGEN_COMMAND) $(BINDIR)/p2c-query.cpp
# 	$(CXX) -std=c++20 $(BINDIR)/queryFrame.cpp $(CXXFLAGS) -DTASK_NAME="$(call get_task,$@)" $(VARIANT_FLAG) -o $@
# clean:
# 	$(RM) -r $(BINDIR)/*

# .PHONY: clean

# $(CXX) $(CXXFLAGS) -o main main.cpp -losmium

main:
	g++ -std=c++17 -o main main.cpp -lz -lexpat -lbz2
