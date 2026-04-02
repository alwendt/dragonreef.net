/* sym - symbol table routines */

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

struct hash_entry *ndtbl[NAME_TABLE_HASH_SIZE];
struct hash_entry *sctbl[START_COND_HASH_SIZE];

struct hash_entry *findsym();


/* addsym - add symbol and definitions to symbol table
 *
 * synopsis
 *    char sym[], *str_def;
 *    int int_def;
 *    hash_table table;
 *    int table_size;
 *    0 / -1 = addsym( sym, def, int_def, table, table_size );
 *
 * -1 is returned if the symbol already exists, and the change not made.
 */

int addsym( sym, str_def, int_def, table, table_size )
register char sym[];
char *str_def;
int int_def;
hash_table table;
int table_size;

    {
    int hash_val = hashfunct( sym, table_size );
    register struct hash_entry *entry = table[hash_val];
    register struct hash_entry *new_entry;
    register struct hash_entry *successor;

    while ( entry )
	{
	if ( ! strcmp( sym, entry->name ) )
	    { /* entry already exists */
	    return ( -1 );
	    }
	
	entry = entry->next;
	}

    /* create new entry */
    new_entry = (struct hash_entry *) malloc( sizeof( struct hash_entry ) );

    if ( new_entry == NULL )
	flexfatal( "symbol table memory allocation failed" );

    if ( (successor = table[hash_val]) )
	{
	new_entry->next = successor;
	successor->prev = new_entry;
	}
    else
	new_entry->next = NULL;

    new_entry->prev = NULL;
    new_entry->name = sym;
    new_entry->str_val = str_def;
    new_entry->int_val = int_def;

    table[hash_val] = new_entry;

    return ( 0 );
    }


/* findsym - find symbol in symbol table
 *
 * synopsis
 *    char sym[];
 *    hash_table table;
 *    int table_size;
 *    struct hash_entry *entry, *findsym();
 *    entry = findsym( sym, table, table_size );
 */

struct hash_entry *findsym( sym, table, table_size )
register char sym[];
hash_table table;
int table_size;

    {
    register struct hash_entry *entry = table[hashfunct( sym, table_size )];
    static struct hash_entry empty_entry =
	{
	(struct hash_entry *) 0, (struct hash_entry *) 0, NULL, NULL, 0,
	} ;

    while ( entry )
	{
	if ( ! strcmp( sym, entry->name ) )
	    return ( entry );
	entry = entry->next;
	}

    return ( &empty_entry );
    }

    
/* hashfunct - compute the hash value for "str" and hash size "hash_size"
 *
 * synopsis
 *    char str[];
 *    int hash_size, hash_val;
 *    hash_val = hashfunct( str, hash_size );
 */

int hashfunct( str, hash_size )
register char str[];
int hash_size;

    {
    register int hashval;
    register int locstr;

    hashval = 0;
    locstr = 0;

    while ( str[locstr] )
	hashval = ((hashval << 1) + str[locstr++]) % hash_size;

    return ( hashval );
    }


/* ndinstal - install a name definition
 *
 * synopsis
 *    char nd[], def[];
 *    ndinstal( nd, def );
 */

ndinstal( nd, def )
char nd[], def[];

    {
    char *copy_string();

    if ( addsym( copy_string( nd ), copy_string( def ), 0,
		 ndtbl, NAME_TABLE_HASH_SIZE ) )
	synerr( "name defined twice" );
    }


/* ndlookup - lookup a name definition
 *
 * synopsis
 *    char nd[], *def;
 *    char *ndlookup();
 *    def/NULL = ndlookup( nd );
 */

char *ndlookup( nd )
char nd[];

    {
    return ( findsym( nd, ndtbl, NAME_TABLE_HASH_SIZE )->str_val );
    }


