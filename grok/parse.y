/* parse.y - parser for flex input */

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

%token CHAR NUMBER SECTEND SCDECL XSCDECL WHITESPACE NAME CORRECT
%token STRING DELETE INSERT TOLERANCE

%{
#include "flexdef.h"

struct Ccl ccl, anyccl, ccl0;

int pat, eps, lastchar, i, firstnfa, finalst;
char clower();

int linenum, syntaxerror;


static void makeany()
    {
    static int madeany = false;
    if ( ! madeany )
	{
	/* create the '.' character class */
	checkedges(__LINE__, __FILE__);
	cclinit( &anyccl );
	ccladd( &anyccl, '\n' );
	checkedges(__LINE__, __FILE__);
	cclnegate( &anyccl );
	checkedges(__LINE__, __FILE__);

	madeany = true;
	}
    }


%}

%%
goal            :  initlex correctionrules sect2
		;

initlex         :
			{
			/* initialize for processing rules */
			}
		;

correctionrules	:	correctionrules onerule
		|
		;

onerule		:	CORRECT item
			{
			ccl0 = ccl;
			}
			item NUMBER '\n'
			{
			int	i, j;
			for (i = 0; i < 128; i++)
			    {
			    for (j = 0; j < 128; j++)
				{
				if (ccltest( &ccl0, i) && ccltest( &ccl, j))
				    {
				    correct_cost[i][j] = $5;
				    }
				}
			    }
			}

		|       DELETE item NUMBER '\n'
			{
			int	i, j;
			for (i = 0; i < 128; i++)
			    {
			    if (ccltest( &ccl, i))
				{
				delete_cost[i] = $3;
				}
			    }
			}

		|	'\n'
			{
			}

		|	INSERT item NUMBER '\n'
			{
			int	i;
			for (i = 0; i < 128; i++)
			    {
			    if (ccltest( &ccl, i))
				{
				insert_cost[i] = $3;
				}
			    }
			}

		|	TOLERANCE NUMBER '\n'
			{
			tolerance = $2;
			}
		;

item		:	'.'
			{
			makeany();
			ccl = anyccl;
			}

		|	fullccl
			{
			}

		|	CHAR
			{
			cclinit( &ccl );
			ccladd( &ccl, $1 );
			}

		|	'"' CHAR '"'
			{
			cclinit( &ccl );
			ccladd( &ccl, $2 );
			}

sect2           :  sect2 initforrule flexrule '\n'
		|
		;

initforrule     :
			{
			/* initialize for a parse of one rule */
			}
		;

flexrule        : '^' re eol 
			{
			checkedges(__LINE__, __FILE__);
			pat = link_machines( $2, $3 );
			checkedges(__LINE__, __FILE__);
			add_accept( pat );

			checkedges(__LINE__, __FILE__);
			firstnfa = pat;
			}

                |  re  eol 
			{
			checkedges(__LINE__, __FILE__);
			pat = link_machines( $1, $2 );
			checkedges(__LINE__, __FILE__);
			add_accept( pat );
			checkedges(__LINE__, __FILE__);
			firstnfa = pat;
			}

                |  error
			{ synerr( "unrecognized rule" ); }
		;

eol             :  '$'
                        {
			checkedges(__LINE__, __FILE__);
			eps = mkstate( SYM_EPSILON );
			checkedges(__LINE__, __FILE__);
			$$ = link_machines( eps, mkstate( '\n' ) );
			checkedges(__LINE__, __FILE__);
			}

		|
		        {
			checkedges(__LINE__, __FILE__);
		        $$ = mkstate( SYM_EPSILON );
			checkedges(__LINE__, __FILE__);
		        }
		;

re              :  re '|' series
                        {
			checkedges(__LINE__, __FILE__);
			$$ = mkor( $1, $3 );
			checkedges(__LINE__, __FILE__);
			}

		|  series
			{
			$$ = $1;
			}
		;


series          :  series singleton
                        {
			/* this is where concatenation of adjacent patterns
			 * gets done
			 */
			checkedges(__LINE__, __FILE__);
			$$ = link_machines( $1, $2 );
			checkedges(__LINE__, __FILE__);
			}

		|  singleton
			{ $$ = $1; }
		;

singleton       :  singleton '*'
                        {
			checkedges(__LINE__, __FILE__);
			$$ = mkclos( $1 );
			checkedges(__LINE__, __FILE__);
			}
			
		|  singleton '+'
			{
			checkedges(__LINE__, __FILE__);
			$$ = mkposcl( $1 );
			checkedges(__LINE__, __FILE__);
			}

		|  singleton '?'
			{
			checkedges(__LINE__, __FILE__);
			$$ = mkopt( $1 );
			checkedges(__LINE__, __FILE__);
			}

		|  singleton '{' NUMBER ',' NUMBER '}'
			{
			if ( $3 > $5 || $3 < 0 )
			    {
			    synerr( "bad iteration values" );
			    $$ = $1;
			    }
			else {
			    if ( $3 == 0 )
				{
				checkedges(__LINE__, __FILE__);
				$$ = mkrep( $1, 1, $5 );
				checkedges(__LINE__, __FILE__);
				$$ = mkopt( $$ );
				checkedges(__LINE__, __FILE__);
				}
			    else {
				checkedges(__LINE__, __FILE__);
				$$ = mkrep( $1, $3, $5 );
				checkedges(__LINE__, __FILE__);
				}
			    }
			}
				
		|  singleton '{' NUMBER ',' '}'
			{
			if ( $3 <= 0 )
			    {
			    synerr( "iteration value must be positive" );
			    $$ = $1;
			    }

			else
			    $$ = mkrep( $1, $3, INFINITY );
			}

		|  singleton '{' NUMBER '}'
			{
			if ( $3 <= 0 )
			    {
			    synerr( "iteration value must be positive" );
			    $$ = $1;
			    }

			else {
			    checkedges(__LINE__, __FILE__);
			    $$ = link_machines( $1, copysingl( $1, $3 - 1 ) );
			    checkedges(__LINE__, __FILE__);
			    }
			}

		|  '.'
			{

			makeany();
			$$ = ccl2nfa( &anyccl );
			checkedges(__LINE__, __FILE__);
			}

		|  fullccl
			{
			$$ = ccl2nfa( &ccl );
			}


		|  '"' string '"'
			{ $$ = $2; }

		|  '(' re ')'
			{ $$ = $2; }

		|  CHAR
			{
			if ( $1 == '\0' )
			    synerr( "null in rule" );

			checkedges(__LINE__, __FILE__);
			$$ = mkstate( $1 );
			checkedges(__LINE__, __FILE__);
			}
		;

fullccl		:  '[' ccl ']'

		|  '[' '^' ccl ']'
			{
			checkedges(__LINE__, __FILE__);
			cclnegate( &ccl );
			checkedges(__LINE__, __FILE__);
			}
		;

ccl             :  ccl CHAR '-' CHAR
                        {
			if ( $2 > $4 )
			    synerr( "negative range in character class" );

			else
			    {
			    for ( i = $2; i <= $4; ++i )
				{
				checkedges(__LINE__, __FILE__);
			        ccladd( &ccl, i );
				checkedges(__LINE__, __FILE__);
				}

			    checkedges(__LINE__, __FILE__);
			    lastchar = $4;
			    }
			
			$$ = $1;
			}

		|  ccl CHAR
		        {
			checkedges(__LINE__, __FILE__);
			ccladd( &ccl, $2 );
			checkedges(__LINE__, __FILE__);
			lastchar = $2;
			checkedges(__LINE__, __FILE__);
			$$ = $1;
			}

		|
			{
			checkedges(__LINE__, __FILE__);
			lastchar = 0;
			checkedges(__LINE__, __FILE__);
			cclinit( &ccl );
			checkedges(__LINE__, __FILE__);
			}
		;

string		:  string CHAR
                        {
			checkedges(__LINE__, __FILE__);
			$$ = link_machines( $1, mkstate( $2 ) );
			checkedges(__LINE__, __FILE__);
			}

		|
			{
			checkedges(__LINE__, __FILE__);
			$$ = mkstate( SYM_EPSILON );
			checkedges(__LINE__, __FILE__);
			}
		;

%%

/* synerr - report a syntax error
 *
 * synopsis
 *    char str[];
 *    synerr( str );
 */

synerr( str )
char str[];

    {
    syntaxerror = true;
    fprintf( stderr, "Syntax error at line %d:  %s\n", linenum, str );
    }


/* yyerror - eat up an error message from the parser
 *
 * synopsis
 *    char msg[];
 *    yyerror( msg );
 */

yyerror( msg )
char msg[];

    {
    }

