csrc = $(wildcard ../utility/fnv/*.c)
cppsrc = $(wildcard ../utility/encoding/*.cpp) $(wildcard ../utility/*.cpp) $(wildcard ../cal/*.cpp) $(wildcard ../config/*.cpp) $(wildcard ../log/*.cpp) $(wildcard ../worker/*.cpp)
obj = $(csrc:.c=.o) $(cppsrc:.cpp=.o) 
dep = $(obj:.o=.d)

DEBUGFLAGS=-g
ORACLE_HOME=/opt/instantclient_11_2/
CPPFLAGS=-std=c++11 -I../ -I$(ORACLE_HOME)/sdk/include $(DEBUGFLAGS)
CFLAGS=$(DEBUGFLAGS)

%.d: %.c
	gcc $(CFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.d: %.cpp
	g++ $(CPPFLAGS) $< -MM -MT $(@:.d=.o) >$@

%.o: %.c
	gcc $(CFLAGS) -o $@ -c $<

%.o: %.cpp
	g++ $(CPPFLAGS) -o $@ -c $<


LDFLAGS = $(DEBUGFLAGS) -L$(ORACLE_HOME) -lpthread /usr/lib/x86_64-linux-gnu/libboost_regex.a -lnnz11 -lclntsh

oracleworker: $(obj)
	echo Linking ...
	g++ -o $@ $^ $(LDFLAGS)

-include $(dep)

.PHONY: clean
clean:
	rm -f $(obj) oracleworker
.PHONY: cleandep
cleandep:
	rm -f $(dep)
