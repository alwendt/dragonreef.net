/*
 *  Loan amortization.  User can enter principal amount, interest rate, monthly payment, and loan term.
 *  Prints a loan amortization.
 *  User can omit one of the 4 parameters and this program will calculate it based on the remaining 3.
 *  Written by Alan Wendt
 */


#include <stdio.h>
#include <math.h>

double log();
static void recordvalue(char *);
char *names[] = { "Interest", "Months", "Principal", "Payment" };

#include <fcntl.h>
#include <stdlib.h>

double log();

static short lineno = 0;
static char *descrip = 0;
static short payment = 0;
static double principal = 0, payamt = 0, rate = 0, months = 0;
static void header();


/* How many values you got.  Given any 3, program can calculate the 4th */
char ngot;

/* The values, or a NULL pointer if no value gotten. */
char *values[4];

main(int argc, char **argv) {
  int i;
  char buf[1024];
  char *p, *lastp;
  double    endbal, intpay, prinpay, begbal;
  double    testpay;
  double    totpay, totint, totprin;
  short day, month, startmonth = 1, year = 0;
  char  *reply;
  char  unknowns = 0;
  char  *form = "%14.2f";
  char *end;

  printf("Content-type: text/plain\n\n");

  /* printf("echo CGI/1.0 test script report:\n\n"); */

  fgets(buf, sizeof(buf), stdin);
  /* printf("%s\n", buf); */

  /* parse out URL eg &Interest=9.0&Principal=10000 etc. */
  lastp = buf;
  for (p = buf;; p++) {
     if (*p == '&') {
        *p = 0; 
        recordvalue(lastp);
        /* printf("%s\n", lastp); */
        lastp = p + 1;
     } else if (*p == '\000') {
        recordvalue(lastp);
        /* printf("%s\n", lastp); */
        break;
     }
  }  

#define RATE 0
#define MONTHS 1
#define PRINCIPAL 2
#define PAYAMT 3

  if (values[RATE]) {
    rate = strtod(values[0], &end);
  }
  if (values[MONTHS]) {
    months = strtod(values[1], &end);
  }
  if (values[PRINCIPAL]) {
    principal = strtod(values[2],  &end);
  }
  if (values[PAYAMT]) {
    payamt = strtod(values[3], &end);
  }

  /*    I couldn't find a closed formula for the rate, so
   *    this uses binary search on the payment amount, in
   *    terms of a guessed rate.
   */
  if (values[RATE] == NULL) {
      double limit[2];
      unsigned char which;

      /* You cannot pay off a loan if the total payments do
       * not at least equal the principal.
       */
      if (payamt * months < principal) {
        printf("Error: the monthly payment times the\n");
        printf("number of months is less than the principal.\n");
        printf("%.2f * %.2f = %.2f\n", payamt, months, payamt * months);
        exit(0);
      }

      if (unknowns++) goto toomany;
      limit[1] = 5;     /* 500% per year max rate */
      limit[0] = 0;

      /* Refine the result by repeated bisection. */
	  for (;;) {
		  rate = (limit[1] + limit[0]) / 2;
		  testpay = (principal * rate / 12) /
			  (1 - 1 / pow(1 + rate / 12, months));

		  which = (payamt < testpay);

		  if (limit[0] >= limit[1] || limit[which] == rate) {
			  /* The rate can't be refined anymore because the limit of accuracy on
			   * the floating point arithmetic has been reached.  Round to 8 decimal places.
			   */
			  rate = floor(rate * 1e+8 + .5) / 1e+8;
			  /* printf("Interest Rate = %.6f\n", rate * 100); */
			  break;
			  }

		  limit[which] = rate;
	  }
  }
  else rate /= 100;

  if (values[MONTHS] == NULL) {
      if (unknowns++) goto toomany;
      months = log(payamt/(payamt-principal*rate/12))/log(1+rate/12);
      months = -floor(-months);
      /* printf("Number of payments = %u\n", (unsigned)months); */
      }

  if (values[PRINCIPAL] == NULL) {
      if (unknowns++) goto toomany;
      principal = (payamt - payamt / pow(1 + rate / 12, months)) /
      (rate / 12);
      principal = floor(principal * 100)/100;
      /* printf("Amount borrowed = %.2f\n", principal); */
      }

  if (values[PAYAMT] == NULL) {
      if (unknowns++) goto toomany;
      payamt = (principal * rate / 12) /
      (1 - 1 / pow(1 + rate / 12, months));
      payamt = floor(payamt * 100 + 0.5) / 100;
      /* printf("Payment Amount = %.2f\n", payamt); */
      }

  lineno = 999;
  header();

  endbal = principal;     /* Principal balance */

repeat:
  for (month = startmonth; month <= 12; month++) {
      payment++;
      begbal = endbal;      /* Beginning bal = last pd ending bal */
      intpay = begbal * rate / 12;
      intpay = floor(intpay * 100.0 + .5) / 100.0;  /* interest for month */

      if (payamt > begbal + intpay && payment == months && begbal > 0)
      payamt = begbal + intpay; /* payamt too big? */
      prinpay = payamt - intpay;    /* Calculate principal payamt */
      endbal=begbal-prinpay;        /* Calculate new principal balance */
      if (-.005 < endbal && endbal < .005)  endbal = 0;
      totpay    += payamt;
      totprin += prinpay;
      totint    += intpay;
      printf("%+02u-%+02u  ", year, month);
      printf("%14.2f%14.2f%14.2f%14.2f%14.2f\n",
         begbal,     payamt,  prinpay,   intpay,   endbal);
      if (payment == months) goto done;
      }

  year++;
  startmonth = 1;

done:
  for (i = 0; i < 80; i++)
    putchar('=');
  putchar('\n');
  printf("Totals               %14.2f%14.2f%14.2f\n\n\n", totpay, totprin, totint);

  if (payment != months) {
      header();
      goto repeat;
      }
  exit(0);

toomany:
  printf("Too many unknowns.  You must supply at least 3 values.\n");
}

/* Take, for example, a string of the form "Interest=9" or whatever and
 * save the "9" in values[0].
 */
static void recordvalue(char *p) {
  int i;
  char *v;
  for (i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
    int l = strlen(names[i]);
    if (!memcmp(names[i], p, l) && p[l] == '=') {             /* Is this the parameter's name? */
      v = p + l + 1;                                          /* Isolate value */
      if (v[0]) {                                             /* If there is a nonempty value */
        if (values[i]) {
          printf("got 2 values for %s\n", names[i]);
          return;
        }
        values[i] = v;
        ngot++;                                               /* remember how many values we got */
      }
      /* printf("p = '%s' l = %d\n", p, l); */
      /* printf("values[%f] = '%s'\n", i, values[i]); */
    }
  }
}



/* Output the header */
static void header()
    {
    int i;
    lineno += 15;
    if (lineno <= 40) return;

    printf("Loan amount: %12.2f    Payment: %11.2f\n", principal, payamt);
    printf("Interest rate: %11.6f    Term:  %5f\n\n", rate * 100, months);

    printf("Pmt Date      Beg Bal       Payment      Prin Red      Interest       End Bal\n");
    lineno = 0;
    for (i = 0; i < 80; i++)
       putchar('-');
    putchar('\n');
    }

