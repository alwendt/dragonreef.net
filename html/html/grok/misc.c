/* misc - miscellaneous flex routines */

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

#include <ctype.h>
#include "flexdef.h"


/* allocate_array - allocate memory for an integer array of the given size */

char *allocate_array( size, element_size )
int size, element_size;

    {
    register char *mem = malloc( (unsigned) (element_size * size) );

    if ( mem == NULL )
	flexfatal( "memory allocation failed in allocate_array()" );

    return ( mem );
    }


/* clower - replace upper-case letter to lower-case
 *
 * synopsis:
 *    char clower(), c;
 *    c = clower( c );
 */

char clower( c )
register char c;

    {
    return ( isupper(c) ? tolower(c) : c );
    }


/* copy_string - returns a dynamically allocated copy of a string
 *
 * synopsis
 *    char *str, *copy, *copy_string();
 *    copy = copy_string( str );
 */

char *copy_string( str )
register char *str;

    {
    register char *c;
    char *copy;

    /* find length */
    for ( c = str; *c; ++c )
	continue;

    copy = malloc( (unsigned) ((c - str + 1) * sizeof( char )) );

    if ( copy == NULL )
	flexfatal( "dynamic memory failure in copy_string()" );

    /* SUPPRESS 560 */
    for ( c = copy; (*c++ = *str++); )
   	continue;

    return ( copy );
    }


/* lerrif - report an error message formatted with one integer argument
 *
 * synopsis
 *    char msg[];
 *    int arg;
 *    lerrif( msg, arg );
 */

lerrif( msg, arg )
char msg[];
int arg;

    {
    char errmsg[MAXLINE];
    (void) sprintf( errmsg, msg, arg );
    flexerror( errmsg );
    }


/* lerrsf - report an error message formatted with one string argument
 *
 * synopsis
 *    char msg[], arg[];
 *    lerrsf( msg, arg );
 */

lerrsf( msg, arg )
char msg[], arg[];

    {
    char errmsg[MAXLINE];

    (void) sprintf( errmsg, msg, arg );
    flexerror( errmsg );
    }


/* flexerror - report an error message and terminate
 *
 * synopsis
 *    char msg[];
 *    flexerror( msg );
 */

flexerror( msg )
char msg[];

    {
    fprintf( stderr, "flex: %s\n", msg );
    exit( 1 );
    }


/* flexfatal - report a fatal error message and terminate
 *
 * synopsis
 *    char msg[];
 *    flexfatal( msg );
 */

flexfatal( msg )
char msg[];

    {
    fprintf( stderr, "flex: fatal internal error %s\n", msg );
    exit( 1 );
    }






/* myctoi - return the integer represented by a string of digits
 *
 * synopsis
 *    char array[];
 *    int val, myctoi();
 *    val = myctoi( array );
 *
 */

int myctoi( array )
char array[];

    {
    int val = 0;

    (void) sscanf( array, "%d", &val );

    return ( val );
    }


/* myesc - return character corresponding to escape sequence
 *
 * synopsis
 *    char array[], c, myesc();
 *    c = myesc( array );
 *
 */

char myesc( array )
char array[];

    {
    switch ( array[1] )
	{
	case 'n': return ( '\n' );
	case 't': return ( '\t' );
	case 'f': return ( '\f' );
	case 'r': return ( '\r' );
	case 'b': return ( '\b' );

	case '0':
	    if ( isdigit(array[2]) )
		{ /* \0<octal> */
		char c, esc_char;
		register int sptr = 2;

		while ( isdigit(array[sptr]) )
		    /* don't increment inside loop control because the
		     * macro will expand it to two increments!  (Not a
		     * problem with the C version of the macro)
		     */
		    ++sptr;

		c = array[sptr];
		array[sptr] = '\0';

		esc_char = otoi( array + 2 );
		array[sptr] = c;

		if ( esc_char == '\0' )
		    {
		    synerr( "escape sequence for null not allowed" );
		    return ( 1 );
		    }

		return ( esc_char );
		}

	    else
		{
		synerr( "escape sequence for null not allowed" );
		return ( 1 );
		}

#ifdef NOTDEF
	case '^':
	    {
	    register char next_char = array[2];

	    if ( next_char == '?' )
		return ( 0x7f );
	    
	    else if ( next_char >= 'A' && next_char <= 'Z' )
		return ( next_char - 'A' + 1 );
    
	    else if ( next_char >= 'a' && next_char <= 'z' )
		return ( next_char - 'z' + 1 );
    
	    synerr( "illegal \\^ escape sequence" );

	    return ( 1 );
	    }
#endif
	}
    
    return ( array[1] );
    }


/* otoi - convert an octal digit string to an integer value
 *
 * synopsis:
 *    int val, otoi();
 *    char str[];
 *    val = otoi( str );
 */

int otoi( str )
char str[];

    {
#ifdef FTLSOURCE
    fortran int gctoi()
    int dummy = 1;

    return ( gctoi( str, dummy, 8 ) );
#else
    int result;

    (void) sscanf( str, "%o", &result );

    return ( result );
#endif
    }



#if 0
/* reallocate_array - increase the size of a dynamic array */

char *reallocate_array( array, size, element_size )
char *array;
int size, element_size;

    {
    register char *new_array = realloc( array,
					(unsigned) (size * element_size ));

    if ( new_array == NULL )
	flexfatal( "attempt to increase array size failed" );
    
    return ( new_array );
    }


#endif
