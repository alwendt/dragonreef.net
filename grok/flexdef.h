#include <stdio.h>

#ifdef HAS_STRINGS
#include <strings.h>
#else
#ifdef HAS_STRING
#include <string.h>
#define bzero(s, n) memset((char *)(s), '\000', (unsigned)(n))
#endif
#endif

#define min(x,y) (x < y ? x : y)
#define max(x,y) (x > y ? x : y)

#define true 1
#define false 0

/* NIL must be 0.  If not, its special meaning when making equivalence classes
 * (it marks the representative of a given e.c.) will be unidentifiable
 */
#define NIL 0

#define NO_TRANSITION NIL
#define INFINITY -1	/* for x{5,} constructions */

/* variables used in the flex input routines:
 * datapos - characters on current output line
 * dataline - number of contiguous lines of data in current data
 *    statement.  Used to generate readable -f output
 * skelfile - fd of the skeleton file
 * yyin - input file
 * temp_action_file - temporary file to hold actions
 * action_file_name - name of the temporary file
 * infilename - name of input file
 * linenum - current input line number
 */

extern int linenum;

/* size of input alphabet - should be size of ASCII set */
#define CSIZE 128

struct Ccl
	{
	char	c[CSIZE/8];
	};


/* variables for flags:
 * syntaxerror - true if a syntax error has been found
 */

extern int syntaxerror;

#define SYM_EPSILON 0	/* to mark transitions on the symbol epsilon */

/* variables for nfa machine data:
 * current_mns - current maximum on number of NFA states
 * firstst - first physical state of a fragment
 * lastst - last physical state of fragment
 * finalst - last logical state of fragment
 * transchar - transition character that got us into this state
 * accptnum - accepting number
 * lastnfa - last nfa state number created
 * edgeptr - pointer to first Edge out of this state
 * first logical state of a fragment is its machine number
 */

#define MAXIMUM_MNS 30000

struct state {
    struct Edge	*edgeptr;
    int		firstst;
    int		lastst;
    int		finalst;
    int		ninedges;
    int		noutedges;
    int		temp;
    unsigned char transchar;
    } *states[15000];

extern int current_mns;
extern int lastnfa, firstnfa;


extern int ccl2nfa();

#if 0
/*
 *  transfrom - from edge state number
 *  transto   - to state number
 */
#define INITIAL_MNE 2000	/* default maximum number of nfa edges */
#define MNE_INCREMENT 1000	/* amount to bump above by if it's not enough */
#define MAXIMUM_MNE 31999
extern int current_mne;		/* current max # of edges */
extern int current_ne;		/* current # of edges */
#endif

struct Edge {
    struct Edge *next, *prior;	/* links all edges from a given state */
    int	from, to;		/* from & to state numbers */
    };

/*
 * num_reallocs - number of times it was necessary to realloc() a group
 *		  of arrays
 */
extern int num_reallocs;

extern void *malloc(), *realloc(), *sbrk();

#define allocate_integer_array(size) \
	(int *) allocate_array( size, sizeof( int ) )

#define reallocate_integer_array(array,size) \
	(int *) reallocate_array( (char *) array, size, sizeof( int ) )
char *allocate_array(), *reallocate_array();

struct hash_entry
    {
    struct hash_entry *prev, *next;
    char *name;
    char *str_val;
    int int_val;
    } ;

typedef struct hash_entry *hash_table[];

#define NAME_TABLE_HASH_SIZE 101
#define START_COND_HASH_SIZE 101

/* maximum line length we'll have to deal with */
#define MAXLINE BUFSIZ

#define allocate_character_array(size) allocate_array( size, sizeof( char ) )

#define reallocate_character_array(array,size) \
	reallocate_array( array, size, sizeof( char ) )
/*
 * nmstr - last NAME scanned by the scanner
 */

extern char nmstr[MAXLINE];

/* used to communicate between scanner and parser.  The type should really
 * be YYSTYPE, but we can't easily get our hands on it.
 */
extern int yylval;

char	delete_cost[128];
char	insert_cost[128];
char	correct_cost[128][128];
int	tolerance;			/* maximum tolerable error */
