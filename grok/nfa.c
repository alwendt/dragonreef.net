/* nfa - NFA construction routines */

/* Construct a state-labelled epsilon nfa for a regular expression.
 * Then read input and output a word from the language defined
 * by the nfa that is as close as possible to the input.
 */

/*
 * Copyright (c) 1987, the University of California
 * 
 * The United States Government has rights in this work pursuant to
 * contract no. DE-AC03-76SF00098 between the United States Department of
 * Energy and the University of California.
 * 
 * This program may be redistributed.  Enhancements and derivative works
 * may be created provided the new works, if made available to the general
 * public, are made available for use by anyone.
 */

/*  Changes copyright (c) 1991,  Alan Wendt.  This work is hereby made
 *  available to the general public, for use by anyone.
 */


#include "flexdef.h"

#define FIRSTST(x)   states[x]->firstst
#define LASTST(x)    states[x]->lastst
#define FINALST(x)   states[x]->finalst
#define EDGEPTR(x)   states[x]->edgeptr
#define NOUTEDGES(x) states[x]->noutedges
#define ACCPTNUM(x)  ((x) == finalst)
#define NINEDGES(x)  states[x]->ninedges
#define TRANSCHAR(x) states[x]->transchar
#define TEMP(x)      states[x]->temp

#define EDGE_FROM(x)  (x)->from
#define EDGE_TO(x)    (x)->to


#define DELETE_COST(x) delete_cost[x]
#define INSERT_COST(x) insert_cost[x]
#define CORRECT_COST(x,y) correct_cost[x][y]

#define MAXCOST 31

int lastnfa, finalst;
int tolerance = 15;		/* maximum tolerable error */

#define MAXMEM 32000L
char		*banks[40];
int		nbanks = 0;
long		memx;			/* memory allocation counter	*/
int		globcost;		/* cost of repair	*/
struct BackEdge	*globword;		/* the repaired word	*/

static char *getword();		/* reconstruct the repaired word */
static void advance();

char	*ralloc(n)			/* allocate memory */
    int		n;
    {
    long	result;

    retry:

    memx += sizeof(int) - 1;		/* align	*/
    memx &= ~(sizeof(int) - 1);
    result = memx;
    memx += n;
    if (nbanks == 0 || memx > MAXMEM) {
	nbanks++;
	if (nbanks > 40)
	    {
	    fprintf(stderr, "no memory in ralloc\n");
	    exit(1);
	    }
	memx = 0;
	if (banks[nbanks - 1] == NULL)
	    {
	    banks[nbanks - 1] = sbrk((int)MAXMEM);
	    }
	goto retry;
	}
    return banks[nbanks - 1]+result;
    }


/* add_accept - add an accepting state to a machine
 *
 * synopsis
 *
 *   add_accept( mach );
 *
 */

add_accept( mach )
int mach;
    {
    finalst = FINALST(mach);
    /*	fprintf(stderr, "final state = %d\n", finalst);	*/
    }

/* copysingl - make a given number of copies of a singleton machine
 *
 * synopsis
 *
 *   newsng = copysingl( singl, num );
 *
 *     newsng - a new singleton composed of num copies of singl
 *     singl  - a singleton machine
 *     num    - the number of copies of singl to be present in newsng
 */

int copysingl( singl, num )
int singl, num;

    {
    int copy, i;

    copy = mkstate( SYM_EPSILON );

    for ( i = 1; i <= num; ++i )
	copy = link_machines( copy, dupmachine( singl ) );

    return ( copy );
    }


/* dumpnfa - debugging routine to write out an nfa
 *
 * synopsis
 *    int state1;
 *    dumpnfa( state1 );
 */

dumpnfa( state1 )
int state1;

    {
    int ns, j;
    struct Edge *e;

    fprintf( stderr, "\n\n********** beginning dump of state-labelled epsilon nfa with start state %d\n",
	     state1 );

    /* we probably should loop starting at FIRSTST(state1) and going to
     * LASTST(state1), but they're not maintained properly when we "or"
     * all of the rules together.  So we use our knowledge that the machine
     * starts at state 1 and ends at lastnfa.
     */

    /* for ( ns = FIRSTST(state1); ns <= LASTST(state1); ++ns ) */
    for ( ns = 1; ns <= lastnfa; ++ns )
	{
	fprintf( stderr, "state # %4d:\t  %4d", ns, TRANSCHAR(ns));
	if (' ' <= TRANSCHAR(ns) && TRANSCHAR(ns) <= '~')
	    fprintf(stderr, "'%c'", TRANSCHAR(ns));
	else
	    fprintf(stderr, "   ");

	fprintf(stderr, ", %d nedges %3d\n", ns == finalst, NOUTEDGES(ns) );

	for (e = EDGEPTR(ns), j = 0; j < NOUTEDGES(ns); e = e->next, j++)
	    fprintf( stderr, "from %4d to %4d\n", EDGE_FROM(e), EDGE_TO(e));
	}

    fprintf( stderr, "********** end of dump\n" );
    }


/* dupmachine - make a duplicate of a given machine
 *
 * synopsis
 *
 *   copy = dupmachine( mach );
 *
 *     copy - holds duplicate of mach
 *     mach - machine to be duplicated
 *
 * note that the copy of mach is NOT an exact duplicate; rather, all the
 * transition states values are adjusted so that the copy is self-contained,
 * as the original should have been.
 *
 * also note that the original MUST be contiguous, with its low and high
 * states accessible by the arrays firstst and lastst
 */

int dupmachine( mach )
int mach;

    {
    int i, j, state, init, last = LASTST(mach), state_offset;
    struct Edge *e;

    for ( i = FIRSTST(mach); i <= last; ++i )
	{
	state = mkstate( TRANSCHAR(i) );
	}

    state_offset = state - i + 1;
    init = mach + state_offset;

    for ( i = FIRSTST(mach); i <= last; ++i )
	for (j = 0, e = EDGEPTR(i); j < NOUTEDGES(i); j++, e = e->next)
	    mkxtion(e->from + state_offset, e->to + state_offset, __LINE__);

    FIRSTST(init) = FIRSTST(mach) + state_offset;
    FINALST(init) = FINALST(mach) + state_offset;
    LASTST(init) = LASTST(mach) + state_offset;

    return ( init );
    }


/* link_machines - connect two machines together
 *
 * synopsis
 *
 *   new = link_machines( first, last );
 *
 *     new    - a machine constructed by connecting first to last
 *     first  - the machine whose successor is to be last
 *     last   - the machine whose predecessor is to be first
 *
 * note: this routine concatenates the machine first with the machine
 *  last to produce a machine new which will pattern-match first first
 *  and then last, and will fail if either of the sub-patterns fails.
 *  FIRST is set to new by the operation.  last is unmolested.
 */

int link_machines( first, last )
int first, last;

    {
#if 0
    fprintf(stderr, "link_machines(%d,%d)\n", first, last);
#endif
    if ( first == NIL )
	{
	return ( last );
	}

    else if ( last == NIL )
	{
	return ( first );
	}

    else
	{
	mkxtion( FINALST(first), last, __LINE__ );
	FINALST(first) = FINALST(last);
	LASTST(first) = max( LASTST(first), LASTST(last) );
	FIRSTST(first) = min( FIRSTST(first), FIRSTST(last) );

	return ( first );
	}
    }


/* mkclos - convert a machine into a closure
 *
 * synopsis
 *   new = mkclos( state );
 *
 *     new - a new state which matches the closure of "state"
 */

int mkclos( state )
int state;

    {
    return ( mkopt( mkposcl( state ) ) );
    }


/* mkopt - make a machine optional
 *
 * synopsis
 *
 *   new = mkopt( mach );
 *
 *     new  - a machine which optionally matches whatever mach matched
 *     mach - the machine to make optional
 *
 * notes:
 *     1. mach must be the last machine created
 *     2. mach is destroyed by the call
 */

int mkopt( mach )
int mach;

    {
    return mkor( mach, mkstate( SYM_EPSILON ) );
    }


/* mkor - make a machine that matches either one of two machines
 *
 * synopsis
 *
 *   new = mkor( first, second );
 *
 *     new - a machine which matches either first's pattern or second's
 *     first, second - machines whose patterns are to be or'ed (the | operator)
 *
 * note that first and second are both destroyed by the operation
 * the code is rather convoluted because an attempt is made to minimize
 * the number of epsilon states needed
 */

int mkor( first, second )
int first, second;

    {
    int eps, result, newedges = 0;

#if 0
    fprintf(stderr, "mkor(%d,%d)", first, second);
#endif
    if ( first == NIL )
	return ( second );

    else if ( second == NIL )
	return ( first );

    eps = mkstate( SYM_EPSILON );

    mkxtion( FINALST(first), eps, __LINE__ );
    mkxtion( FINALST(second), eps, __LINE__ );
    FINALST(second) = FINALST(first) = eps;
    LASTST(first) = LASTST(second) = eps;
    newedges += 2;
    eps = mkstate( SYM_EPSILON );
    mkxtion( eps, first, __LINE__ );
    mkxtion( eps, second, __LINE__ );
    LASTST(first) = LASTST(second) = eps;
    newedges += 2;
    result = eps;

    FINALST(result) = FINALST(first);
    if ( FIRSTST(first) < FIRSTST(result) )
	FIRSTST(result) = FIRSTST(first);
    if ( FIRSTST(second) < FIRSTST(result) )
	FIRSTST(result) = FIRSTST(second);
    if ( LASTST(first) > LASTST(result) )
	LASTST(result) = LASTST(first);
    if ( LASTST(second) > LASTST(result) )
	LASTST(result) = LASTST(second);

#if 0
    fprintf(stderr, "mkor returns %d\n", result);
#endif
    return result;
    }


/* mkposcl - convert a machine into a positive closure
 *
 * synopsis
 *   new = mkposcl( state );
 *
 *    new - a machine matching the positive closure of "state"
 */

int mkposcl( state )
int state;

    {
    int		eps;
    if (TRANSCHAR(state) == SYM_EPSILON)
	{
	mkxtion( FINALST(state), state, __LINE__ );
	return ( state );
	}

    eps = mkstate( SYM_EPSILON );
    mkxtion( FINALST(state), eps, __LINE__ );
    mkxtion( eps, state, __LINE__ );
    FIRSTST(eps) = FIRSTST(state);
    LASTST(eps) = eps;
    FINALST(eps) = FINALST(state);
    return eps;
    }


/* mkrep - make a replicated machine
 *
 * synopsis
 *   new = mkrep( mach, lb, ub );
 *
 *    new - a machine that matches whatever "mach" matched from "lb"
 *          number of times to "ub" number of times
 *
 * note
 *   if "ub" is INFINITY then "new" matches "lb" or more occurrences of "mach"
 */

int mkrep( mach, lb, ub )
int mach, lb, ub;

    {
    int base, tail, copy, i;

    base = copysingl( mach, lb - 1 );

    if ( ub == INFINITY )
	{
	copy = dupmachine( mach );
	mach = link_machines( mach, link_machines( base, mkclos( copy ) ) );
	}

    else
	{
	tail = mkstate( SYM_EPSILON );

	for ( i = lb; i < ub; ++i )
	    {
	    copy = dupmachine( mach );
	    tail = mkopt( link_machines( copy, tail ) );
	    }

	mach = link_machines( mach, link_machines( base, tail ) );
	}

    return ( mach );
    }


/* mkstate - create a state with a transition on a given symbol
 *
 * synopsis
 *
 *   state = mkstate( sym );
 *
 *     state - a new state matching sym
 *     sym   - the symbol the new state is to have its in-transitions on
 *             (state-labelled epsilon-nfa)
 *
 * note that this routine makes new states in ascending order through the
 * state array (and increments LASTNFA accordingly).  The routine DUPMACHINE
 * relies on machines being made in ascending order and that they are
 * CONTIGUOUS.  Change it and you will have to rewrite DUPMACHINE (kludge
 * that it admittedly is)
 */

int mkstate( sym )
int sym;

    {
    if (++lastnfa >= MAXIMUM_MNS)
	{
	lerrif( "input rules are too complicated (>= %d NFA states)",
		MAXIMUM_MNS );
	}

    states[lastnfa] = (struct state *)malloc(sizeof(struct state));

    if (!states[lastnfa])
	    {
	    lerrif("out of memory at line %d\n", __LINE__);
	    }

    TRANSCHAR(lastnfa) = sym;
    FIRSTST(lastnfa) = lastnfa;
    FINALST(lastnfa) = lastnfa;
    LASTST(lastnfa) = lastnfa;
    EDGEPTR(lastnfa) = NULL;
    NOUTEDGES(lastnfa) = 0;
    NINEDGES(lastnfa) = 0;

    return ( lastnfa );
    }

#define INSERT_LIST(head, new, next, prior)  { \
    if (head == NULL) { \
	head = new->next = new->prior = new; \
	} \
    else { \
	new->next = head; \
	new->prior = head->prior; \
	new->prior->next = new; \
	new->next->prior = new; \
	head = new; \
	} \
    }

#define UNLINK(x, type) { \
	type *next, *prior;  \
	next = x->next; \
	prior = x->prior; \
	prior->next = next; \
	next->prior = prior; \
	}

/* delxtion - delete a transition
 *
 * synopsis
 *
 *   delxtion( edgeptr )
 *
 *       edgeptr - a pointer to the struct Edge that is to be deleted
 */
delxtion( edgeptr )
    struct Edge *edgeptr;
    {
    NOUTEDGES(EDGE_FROM(edgeptr))--;
    NINEDGES(EDGE_TO(edgeptr))--;
    UNLINK(edgeptr, struct Edge);
    if (EDGEPTR(EDGE_FROM(edgeptr)) == edgeptr)
	EDGEPTR(EDGE_FROM(edgeptr)) = edgeptr->next;
    if (NOUTEDGES(EDGE_FROM(edgeptr)) == 0)
	EDGEPTR(EDGE_FROM(edgeptr)) = NULL;
    free(edgeptr);
    }

/* mkxtion - make a transition from one state to another
 *
 * synopsis
 *
 *   mkxtion( statefrom, stateto );
 *
 *     statefrom - the state from which the transition is to be made
 *     stateto   - the state to which the transition is to be made
 */

/* SUPPRESS 590 */
mkxtion( statefrom, stateto, fromline )
int statefrom, stateto;
    {
    struct Edge	*newedge;

    /*
    fprintf(stderr, "mkxtion(%d,%d,%d)\n", statefrom, stateto, fromline);
    */

    newedge = (struct Edge *)malloc(sizeof(struct Edge));
    if (!newedge)
	{
	fprintf(stderr, "no memory in mkxtion\n");
	exit(1);
	}

    NOUTEDGES(statefrom)++;
    NINEDGES(stateto)++;
    newedge->from = statefrom;
    newedge->to = stateto;
    INSERT_LIST(EDGEPTR(statefrom), newedge, next, prior)
    }

static int	bflen;		/* length of the buffer */
static char	bf[512];	/* the buffer */
extern FILE	*yyin;

/* v[j][q] records the cost of the cheapest way to get to state q
 * having consumed j characters.
 */
static int maxj = 0;		/* # of allocated vectors in v */
static unsigned char *v[MAXLINE + 1];

/* Back-edge is used to record the history of edits made by the system
 * as it finds the cost of edits to get the cheapest match.  It is used
 * to reconstruct a word that is a closest match.
 */
struct BackEdge {
    struct BackEdge *prior;
    char	c;
    };

struct Work
    {
    struct Work *next;
    int		q;			/* a state number */
    int		j;			/* a number of characters consumed */
    struct BackEdge *backedge;
    } *worklist[MAXCOST+1], *workend[MAXCOST+1];

#define Q_PUT(type, head, tail, state, chars, word)  { \
    type  *newptr; \
    newptr = (type *)ralloc(sizeof(type)); \
    newptr->q = state; \
    newptr->j = chars; \
    newptr->backedge = word; \
    if (head == NULL)  head = newptr;  \
    else  tail->next = newptr;  \
    newptr->next = NULL; \
    tail = newptr; \
    }

#define Q_EMPTY(head) (head == NULL)

/*  Get the next element off the queue.  Make sure the queue is not
 *  empty before you do this.
 */
#define Q_GET(type, head, new) { \
    type *next; \
    new = *(head); \
    next = (head)->next; \
    (head) = next; \
    }

 /*  This constructs a state-labelled epsilon-nfa from a regular expression.
  *  Then it constructs a two-dimensional array v[j][n].  j corresponds
  *  to the number of characters that have been read from the string, so
  *  0 <= j <= strlen(input).  n corresponds to the states of the nfa.
  *  Entries in v represent the cheapest cost of driving the nfa to state
  *  n, having read j characters from the input.  Initially the nfa is
  *  in state 0, having read 0 characters from the input, so v[0][0] = 0.
  *  The algorithm uses a dynamic programming algorithm based upon Dikstra's
  *  shortest-path (with discrete bucketing) to find the cheapest path to
  *  v[strlen(input)][nf] where nf is the nfa final state.  If the cost
  *  exceeds the tolerance, the search is abandoned.
  */
main(argc, argv)
    int		argc;
    char	**argv;
    {
    int		i, j;

    if (argc == 2)
	{
	yyin = fopen(argv[1], "r");
	if (!yyin) {
	    fprintf(stderr, "cannot open %s\n", argv[1]);
	    exit(1);
	    }
	}

    for (i = 0; i < 128; i++)
	{
	delete_cost[i] = insert_cost[i] = 1;
	for (j = 0; j < 128; j++)
	    correct_cost[i][j] = 1;
	}

    yyparse();


    firstnfa = link_machines( mkstate( SYM_EPSILON ), firstnfa );

    optimize_nfa();

    checkedges(__LINE__, __FILE__);

    /*
    fprintf(stderr, "firstnfa %d last %d\n", firstnfa, lastnfa);
    dumpnfa( firstnfa );
    fprintf(stderr, "\n\n");
    */

    while (gets( bf ))
	{
	/*	fprintf(stderr, "bf   = %s\n", bf);	*/
	bflen = strlen( bf );
	while (maxj <= bflen)
	    {
	    v[maxj] = (unsigned char *)
		allocate_array( lastnfa + 1, sizeof(*v[0]) );
	    maxj++;
	    }

	for (j = 0; j <= bflen; j++)
	    memset((char *)(v[j]), '\377', (unsigned)(lastnfa + 1));

	nbanks = memx = 0;
	for (i = 0; i <= MAXCOST; i++)
	    {
	    worklist[i] = NULL;
	    workend[i] = NULL;
	    }
	getcosts(tolerance);
	if (globword)
	    {
	    /*
	    fprintf(stderr, "cost = %d\nword = '%s'\n",
		globcost, getword(globword));
	    */
	    puts(getword(globword));
	    }
	}
    /* fprintf(stderr, "done!\n");	*/
    }

getcosts(maxcost)
    {
    int		cost = 0, modcost;
    struct Work new;

    /*  We can get to the first nfa state, scanning zero characters,
     *	for a cost of 0.  Start off by sticking this fact into the queue.
     */
    globword = NULL;
    record(firstnfa, 0, 0, (struct BackEdge *)0);

    /*  allow increasing costs and see how far we can get */
    for (cost = 0; cost <= maxcost; cost++)
	{
	modcost = cost % (MAXCOST + 1);

	/*  process until we exhaust the work queue */
	while (!Q_EMPTY(worklist[modcost]))
	    {
	    Q_GET(struct Work, worklist[modcost], new);

	    /*
	    fprintf(stderr, "Pull chars=%d state=%d cost=%d from queue\n",
		new.j, new.q, cost);
	    */
	    if ( v[new.j][new.q] < cost )
		{
		/*
		fprintf(stderr, "discard chars=%d state=%d cost=%d\n",
			new.j, new.q, cost);
		*/
		continue;
		}

	    advance(new.q, new.j, cost, new.backedge);
	    }

	if (globword)
	    {
	    return;
	    }
	}
    }

static char word[MAXLINE];

static char *getword(edge)
    struct BackEdge *edge;
    {
    int		first = 0, last = 0;
    int		t;

    /*  trace back and collect the corrected word into a buffer */
    for (; edge; edge = edge->prior)
	{
	word[last++] = edge->c;
	}

    word[last--] = 0;

    /*  reverse the buffer */
    for (; last > first; last--, first++)
	{
	t = word[last]; word[last] = word[first]; word[first] = t;
	}

    return word;
    }

/*  A call to record says that we can get to a given state for a
 *  given cost k.  If this is further than we have gotten yet for
 *  that much cost we put this info on the end of the worklist[k]
 *  and also into v[cost][state].
 */
record(state, chars, cost, word)
    int		state;		/* state we can get to for given cost */
    int		chars;		/* having consumed this many characters */
    int		cost;		/* cost */
    struct BackEdge *word;	/* last node in a word that works */
    {
    int		modcost;

    /*
    fprintf(stderr, "record state=%d chars=%d cost=%d word='%s'\n",
	state, chars, cost, getword(word));
    */

    if (state == finalst && chars == bflen && (!globword || cost < globcost))
	{
	globcost = cost;
	globword = word;
	}

    /*  Is this a newly-discovered cheaper way to get here? */
    if (cost < v[chars][state])
	{
	v[chars][state] = cost;
	modcost = cost % (MAXCOST + 1);
	Q_PUT(struct Work, worklist[modcost], workend[modcost],
		state, chars, word);
	/*
	fprintf(stderr, "new v[%d][%d]=%d\n\n", chars, state, cost);
	*/
	}
    }

#define TRACE_ADVANCE 0

/*  A call to advance(state, chars, cost, word) asserts that cost is the
 *  cheapest way we know to get to the given state after consuming chars
 *  of the input, and demands that the closure of this assertion be
 *  taken.
 *
 *  Advance goes one more step and enters the new values in v.  Advance will
 *  eventually get called for everything that it puts in v, so recursive
 *  calls are not necessary.
 */
static void advance(state, chars, cost, word)
    int		state;		/* we can get to state p */
    int		chars;		/* after consuming so many characters */
    int		cost;		/* for a given cost */
    struct BackEdge *word;	/* last char in a word that works */
    {
    register	q;		/* another state */
    struct Edge *e;
    struct BackEdge *new;

#if TRACE_ADVANCE
    fprintf(stderr,
	"got to state %d at cost %d using %d input chars\n",
	state, cost, chars);
#endif

    /*  we can always consume one character and stay in the same state,
     *  by incurring a "deletion charge".
     */
    if (chars < bflen)
	{
#if TRACE_ADVANCE
	fprintf(stderr,
	    "get to state %d at cost %d using %d chars by deleting 1 char\n",
	    state, cost + DELETE_COST(bf[chars]), chars + 1);
#endif
	record(state, chars + 1, cost + DELETE_COST(bf[chars]), word);
	}

    /*  for each edge out of state p */
    for (e = EDGEPTR(state); e;)
	{
	q = EDGE_TO(e);

	/*  If we have an epsilon edge to a new state, we can get
	 *  there for no additional charge and without consuming
	 *  any characters.
	 */
	if (TRANSCHAR(q) == SYM_EPSILON)
	    {
#if TRACE_ADVANCE
	    fprintf(stderr,
		"get to state %d at cost %d using %d chars with e-edge\n",
		q, cost, chars);
#endif
	    record(q, chars, cost, word);
	    }

	else {
	    /*  We can incur an "insertion charge" and get to the
	     *  next state without using up any input characters.
	     */
	    new = (struct BackEdge *)ralloc(sizeof(struct BackEdge));
	    new->c = TRANSCHAR(q);
	    new->prior = word;

#if TRACE_ADVANCE
	    fprintf(stderr,
		"get to state %d at cost %d using %d chars by insert %c\n",
		q, cost + INSERT_COST(new->c), chars, new->c);
#endif

	    record( q, chars, cost + INSERT_COST(new->c), new );

	    /*  If we have a transition to a new state that is labelled
	     *  correctly, we can get there for no additional charge.
	     */
	    if (chars < bflen)
		{
		if (new->c == bf[chars])
		    {
#if TRACE_ADVANCE
		    fprintf(stderr, 
		    "get to state %d cost %d using %d chars following %c edge\n",
			    q, cost, chars + 1, new->c);
#endif
		    record(q, chars + 1, cost, new);
		    }

		else {
		    /*  If we have a transition to a new state that is not
		     *  labelled correctly, we can get there by incurring a
		     *  correction cost.
		     */
#if TRACE_ADVANCE
		    fprintf(stderr, 
			"get to state %d cost %d using %d chars changing %c to %c\n",
			    q, cost + CORRECT_COST(bf[chars], new->c),
			    chars + 1, bf[chars], new->c);
#endif
		    record( q, chars + 1,
			   cost + CORRECT_COST(bf[chars], new->c), new );
		    }
		}
	    }


	e = e->next;
	if (e == EDGEPTR(state))
	    break;
	}
    }

/*  nfa branch chaining.  Find a state A with an out-edge that points
 *  to an epsilon state B.  Remove the edge and duplicate the out-edges
 *  of B into A.  When you remove all of the in-edges of some state,
 *  you can remove the state unless it is the start state.
 *
 *  Straightfoward application of this principal makes the nfa 
 *  slower and larger.  What happens is on something like
 *  [0-9][0-9] the original machine had an e-state in between 10
 *  left-hand states and 10 right-hand states, and this replaced
 *  20 edges with 100 in order to eliminate the e-state.  This caused
 *  5 times more calls to record.  Now we do not perform this opt
 *  unless the product of B's inedges and B's outedges is <= their
 *  sum.  Optimize_nfa still usually eliminates half of the input
 *  states.
 */
optimize_nfa()
    {
    int		A, B, C, k, kk, changes;
    struct Edge *e, *nexte, *ee, *nextee;


    for (changes = 1; changes;)
	{
	changes = 0;


	changes = 0;
	for (A = 1; A <= lastnfa; A++)
	    {


	    /*  If an edge from this state goes to an epsilon state */
	    for (e = EDGEPTR(A); EDGEPTR(A); e = nexte) {

		nexte = e->next;
		B = EDGE_TO(e);
		if (TRANSCHAR(B) == SYM_EPSILON && B != finalst &&
		    NINEDGES(B) * NOUTEDGES(B) <= NINEDGES(B) + NOUTEDGES(B)) {

		    /*  delete A->B edge */
		    delxtion(e);

		    /*  copy B's out-edges to A */
		    for (kk = 0, ee = EDGEPTR(B); kk < NOUTEDGES(B);
							ee = ee->next, kk++)
			{
			mkxtion(A, EDGE_TO(ee), __LINE__);
			/*  Fix the case in which there was only one out edge
			 *  which is being replaced by a different one.
			 *  If we don't do this, e will point to the deleted 
			 *  edge on the next loop iteration.
			 */
			if (e == nexte)
			    nexte = EDGEPTR(A);
			}

		    /*  delete B if it has no incoming edges */
		    if (NINEDGES(B) == 0)
			{
			delete_state(B);
			A--;
			changes = 1;
			goto nextA;	/* avoid second loop in case A=0 */
			}
		    changes = 1;
		    }
		if (nexte == EDGEPTR(A)) break;
		}

	    for (k = 0, e = EDGEPTR(A); k < NOUTEDGES(A); k++, e = nexte) {

		nexte = e->next;
		B = EDGE_TO(e);

		if (TRANSCHAR(B) == SYM_EPSILON
		    && NOUTEDGES(B) == 1
		    && TRANSCHAR(C = EDGE_TO(EDGEPTR(B))) == SYM_EPSILON
		    && ACCPTNUM(B) <= ACCPTNUM(C)) {
		    /*
		    fprintf(stderr, "%d -> eps %d -> eps %d\n", A, B, C);
		    */

		    delxtion(e);
		    mkxtion(A, C, __LINE__);
		    if (NINEDGES(B) == 0) {
			delete_state(B);
			A--;
			}
		    changes = 1;
		    }
		}
	    nextA:;
	    }
	}

    }

/*  delete an nfa state which has lost all of its in-edges */
delete_state(d)
    int		d;
    {
    int		dd;	
    struct Edge *e, *nexte;

    retry:
#if 0
    fprintf(stderr, "delete_state(%d)\n", d);
#endif

    if (firstnfa == lastnfa)
	firstnfa = d;
    if (finalst == lastnfa)
	finalst = d;

    /*  Delete all of the edges emanating from the losing state.
     *  Uncount the in-edge count of each state pointed to.
     */
    /* SUPPRESS 560 */
    while (e = EDGEPTR(d))
	delxtion(e);

    states[d] = states[lastnfa--];		/* overwrite the losing state */

    /*  fix up the edges out of the state that moved */
    for (e = EDGEPTR(d); e; e = nexte)
	{
	nexte = e->next;
	if (EDGE_TO(e) == lastnfa + 1)
	    {
	    fprintf(stderr, "can't happen at line %d\n", __LINE__, __FILE__);
	    exit(1);
	    }
	EDGE_FROM(e) = d;
	if (nexte == EDGEPTR(d))
	    break;
	}

    /*  fix up the edges into the state that moved */
    for (dd = 1; dd <= lastnfa; dd++) 
	{
	for (e = EDGEPTR(dd); e; e = nexte)
	    {
	    nexte = e->next;
	    if (EDGE_TO(e) == lastnfa + 1)
		{
		EDGE_TO(e) = d;
		}
	    if (nexte == EDGEPTR(dd))
		break;
	    }
	}


    /*  delete any states (except the start state) which have
     *  lost all of their incoming edges.
     */
    for (d = 1; d <= lastnfa ; d++)
	if (NINEDGES(d) == 0 && d != firstnfa)
	    goto retry;

    }

/*  Check the nfa to make sure the in-edge and out-edge counts are
 *  correct.  If you suspect nfa structure problems, call this from
 *  all over the place.
 */

/* SUPPRESS 590 */
checkedges(fromline, fromfile)
    int		fromline;
    char	*fromfile;
    {

    int		i;
    int		k;
    struct Edge *e, *nexte;
    for (i = 1; i <= lastnfa; i++)
	TEMP(i) = 0;

    /*  count the edges into and from each state */
    for (i = 1; i <= lastnfa; i++) 
	{
	k = 0;
	for (e = EDGEPTR(i); e; e = nexte)
	    {
	    k++;
	    nexte = e->next;
	    TEMP(EDGE_TO(e))++;
	    if (EDGE_FROM(e) != i)
		{
		fprintf(stderr, "EDGE_FROM(%d) is %d should be %d\n",
			e, EDGE_FROM(e), i);	
		}
	    if (nexte == EDGEPTR(i))
		break;
	    }
	if (NOUTEDGES(i) != k)
	    {
	    fprintf(stderr, "noutedges[%d] is %d should be %d line %d %s\n",
		i, NOUTEDGES(i), k, fromline, fromfile);
	    }
	}

    for (i = 1; i <= lastnfa; i++)
	if (TEMP(i) != NINEDGES(i))
	    {
	    fprintf(stderr, "ninedges[%d] is %d should be %d line %d %s\n",
		i, NINEDGES(i), TEMP(i), fromline, fromfile);
	    }
    }


