/* ccl - routines for character classes */

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

#include "flexdef.h"

/* ccladd - add a single character to a ccl
 *
 * synopsis
 *    int cclp;
 *    char ch;
 *    ccladd( cclp, ch );
 */

ccltest( cclp, ch )
    struct Ccl *cclp;
    char ch;
    {
    /* SUPPRESS 53 */
    return cclp->c[(ch & (CSIZE - 1)) >> 3] & (1 << (ch & 7));
    }

ccladd( cclp, ch )
    struct Ccl *cclp;
    char ch;
    {
    /* SUPPRESS 53 */
    cclp->c[(ch & (CSIZE - 1)) >> 3] |= 1 << (ch & 7);
    }


/* cclinit - make an empty ccl
 *
 * synopsis
 *    int cclinit();
 *    cclinit( &newccl );
 */

void cclinit( ccl )
    struct Ccl *ccl;
    {
    bzero(ccl->c, sizeof(*ccl));
    }


/* cclnegate - negate a ccl
 *
 * synopsis
 *    int cclp;
 *    cclnegate( ccl );
 */

cclnegate( cclp )
    struct Ccl *cclp;
    {
    int		i;
    for (i = 0; i < sizeof(*cclp); i++)
	{
	cclp->c[i] ^= -1;
	}
    }


	
/*  BUGGY IF CCL IS EMPTY!!! */
int ccl2nfa( cclp )
    struct Ccl *cclp;
    {
    int		i, j;
    int			result = 0;

    for (i = 0; i < CSIZE; i++)
	{
	j = cclp->c[i >> 3];
	if (j & (1 << (i & 7)))  
	    {
	    if (result == 0)
		{
		result = mkstate( i );
		}
	    else
		{
		result = mkor( result, mkstate( i ) );
		}
	    }
	}
    return result;
    }
