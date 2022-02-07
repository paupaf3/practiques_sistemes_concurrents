CC=gcc
CXX=g++
RM=rm -f
CPPFLAGS= -g -std=c++11 -lpthread
LDFLAGS= -g -std=c++11 -lpthread

src = $(wildcard *.c)
obj = $(src:.c=.o)
dep = $(obj:.o=.d)

SRCS= $(wildcard *.cpp)
SRCS= Map.cpp MapReduce.cpp Reduce.cpp Types.cpp WordCount.cpp
#WordCount.cpp  MapReduce.cpp Map.cpp rutines.cpp 
OBJS=$(subst .cpp,.o,$(SRCS))

all: WordCount

WordCount: $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp %.h
	$(CXX) $(CPPFLAGS) -o $@ -c $< 

depend: .depend

.depend: $(SRCS)
	rm -f ./.depend
	$(CXX) $(CPPFLAGS) -MM $^>>./.depend;

clean:
	$(RM) $(OBJS)

dist-clean: clean
	$(RM) *~ .depend

include .depend
