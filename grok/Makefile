
.SUFFIXES:
.SUFFIXES:  .o .s .c .y .pp

CC      = gcc
GROKFILES =  nfa.o ccl.o parse.o sym.o misc.o scan.o


grok:	$(GROKFILES)
	$(CC) $(LDFLAGS) -o grok $(GROKFILES) -lfl
	./grok ncnb.def < test.in > test.out.tmp
	if cmp test.out test.out.tmp ; \
	then \
		echo Test Passed ; \
		rm test.out.tmp ; \
	else \
		echo Test Failed ; \
		mv grok grok.bogus ; \
	fi

.c.o:	; 	$(CC) -I. $(CFLAGS) -c $*.c

# correction

scan.c:	scan.l
	flex -I scan.l
	mv lex.yy.c scan.c

parse.h parse.c : parse.y
	yacc -d parse.y
	@mv y.tab.c parse.c
	@mv y.tab.h parse.h

clean:
	rm -f *.o *.log scan.c parse.c test.out.tmp grok

