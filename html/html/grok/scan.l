/* scan.l - scanner for flex input */

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

%{
#include "flexdef.h"
#include "parse.h"

char nmstr[MAXLINE];

#define RETURNCHAR \
	yylval = yytext[0]; \
	return ( CHAR );

#define RETURNNAME \
	(void) strcpy( nmstr, yytext ); \
	return ( NAME );

#define RETURNSTRING \
	(void) strcpy( nmstr, yytext ); \
	return ( STRING );

#define PUT_BACK_STRING(str, start) \
	for ( i = strlen( str ) - 1; i >= start; --i ) \
	    unput(str[i])
%}

%x MAIN NUM QUOTE BRACEERROR FIRSTCCL CCL COR QUOTECOR
%x FIRSTCCLCOR CCLCOR

C	([a-zA-Z0-9_()%<>[\]{},.;&!~*/+\-=^|?:@`$# ])
cchar	((\\?)({C}|\"))|(\\\')|(\\\\)|(\\([0-7]{1,3}))
schar	((\\?)({C}|\'))|(\\\")|(\\\\)|(\\([0-7]{1,3}))
WS		[ \t]+

NAME		[a-z_][a-z_0-9]*

ESCSEQ		\\([^^\n]|"^".|0[0-9]{1,3})

%%
    static int bracelevel, didadef;
    int i;
    char myesc();


<INITIAL,COR>^%correct	{
			BEGIN(COR);
			return CORRECT;
			}

<INITIAL,COR>^"%"{WS}.*"\n"	{
			/* I'm a comment */
			}

<INITIAL,COR>^%tolerance  {
			BEGIN(COR);
			return TOLERANCE;
			}

<INITIAL,COR>^%insert  {
			BEGIN(COR);
			return INSERT;
			}

<INITIAL,COR>^%delete 	{
			BEGIN(COR);
			return DELETE;
			}

<COR>{WS}		{
			}

<COR>[0-9]+		{
			yylval = myctoi( yytext );
			return ( NUMBER );
			}

<COR>\"			{
			BEGIN(QUOTECOR);
			return '"';
			}

<QUOTECOR>\"		{
			BEGIN(COR);
			return '"';
			}

<QUOTECOR>.		{
			RETURNCHAR;
			}

<COR>\.			{
			return '.';
			}

<COR>"\n"		{
			return '\n';
			}

<COR>.			{
			RETURNCHAR;
			}

<INITIAL,COR>"["([^\\\]\n]|{ESCSEQ})+"]"	{
			(void) strcpy( nmstr, yytext );

			/* push back everything but the leading bracket
			 * so the ccl can be rescanned
			 */
			PUT_BACK_STRING(nmstr, 1);

			BEGIN(FIRSTCCLCOR);
			return ( '[' );
			}

<FIRSTCCLCOR>"^"/[^-\n]	BEGIN(CCLCOR); return ( '^' );
<FIRSTCCLCOR>"^"/-		return ( '^' );
<FIRSTCCLCOR>-		BEGIN(CCLCOR); yylval = '-'; return ( CHAR );
<FIRSTCCLCOR>.		BEGIN(CCLCOR); RETURNCHAR;
<FIRSTCCLCOR>{ESCSEQ}	{
			yylval = myesc( yytext );
			BEGIN(CCLCOR);
			return ( CHAR );
			}

<CCLCOR>-/[^\]\n]		return ( '-' );
<CCLCOR>[^\]\n]		RETURNCHAR;
<CCLCOR>"]"		BEGIN(COR); return ( ']' );


<INITIAL,COR>^"%%".*\n	{
			BEGIN(MAIN);
			}


<MAIN>^"^"		return ( '^' );
<MAIN>\"		BEGIN(QUOTE); return ( '"' );
<MAIN>"{"/[0-9]		BEGIN(NUM); return ( '{' );
<MAIN>"{"[^0-9\n][^}\n]*	BEGIN(BRACEERROR);
<MAIN>"$"/[ \t\n]	return ( '$' );

<MAIN>{WS}		{ /* needs to be separate from following rule due to
			   * bug with trailing context
			   */
			bracelevel = 0;
			return ( '\n' );
			}

<MAIN>"["([^\\\]\n]|{ESCSEQ})+"]"	{
			(void) strcpy( nmstr, yytext );

			/* push back everything but the leading bracket
			 * so the ccl can be rescanned
			 */
			PUT_BACK_STRING(nmstr, 1);

			BEGIN(FIRSTCCL);
			return ( '[' );
			}

<MAIN>"{"{NAME}"}"	{
			register char *nmdefptr;
			char *ndlookup();

			(void) strcpy( nmstr, yytext );
			nmstr[yyleng - 1] = '\0';  /* chop trailing brace */

			/* lookup from "nmstr + 1" to chop leading brace */
			if ( ! (nmdefptr = ndlookup( nmstr + 1 )) )
			    synerr( "undefined {name}" );

			else
			    { /* push back name surrounded by ()'s */
			    unput(')');
			    PUT_BACK_STRING(nmdefptr, 0);
			    unput('(');
			    }
			}

<MAIN>[/|*+?.()]	return ( yytext[0] );
<MAIN>.			RETURNCHAR;
<MAIN>\n		++linenum; return ( '\n' );


<QUOTE>[^"\n]		RETURNCHAR;
<QUOTE>\"		BEGIN(MAIN); return ( '"' );

<QUOTE>\n		{
			synerr( "missing quote" );
			BEGIN(MAIN);
			++linenum;
			return ( '"' );
			}


<FIRSTCCL>"^"/[^-\n]	BEGIN(CCL); return ( '^' );
<FIRSTCCL>"^"/-		return ( '^' );
<FIRSTCCL>-		BEGIN(CCL); yylval = '-'; return ( CHAR );
<FIRSTCCL>.		BEGIN(CCL); RETURNCHAR;
<FIRSTCCL>{ESCSEQ}	{
			yylval = myesc( yytext );
			BEGIN(CCL);
			return ( CHAR );
			}

<CCL>-/[^\]\n]		return ( '-' );
<CCL>[^\]\n]		RETURNCHAR;
<CCL>"]"		BEGIN(MAIN); return ( ']' );



<NUM>[0-9]+		{
			yylval = myctoi( yytext );
			return ( NUMBER );
			}

<NUM>","			return ( ',' );
<NUM>"}"			BEGIN(MAIN); return ( '}' );

<NUM>.			{
			synerr( "bad character inside {}'s" );
			BEGIN(MAIN);
			return ( '}' );
			}

<NUM>\n			{
			synerr( "missing }" );
			BEGIN(MAIN);
			++linenum;
			return ( '}' );
			}


<BRACEERROR>"}"		synerr( "bad name in {}'s" ); BEGIN(MAIN);
<BRACEERROR>\n		synerr( "missing }" ); ++linenum; BEGIN(MAIN);



<INITIAL,MAIN,QUOTE,CCL>{ESCSEQ}	{
			yylval = myesc( yytext );
			return ( CHAR );
			}



%%
