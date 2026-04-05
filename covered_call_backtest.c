// covered_call_backtest.c
//
// build:
//   gcc -O3 -march=native -ffast-math -fopenmp -o covered_call_backtest covered_call_backtest.c -lm
//
// example single run:
//   ./covered_call_backtest --csv VOO.csv --ticker VOO --ma 200 --delta 0.20 --delay-days 5 --survival-prob 0.80
//
// example grid search:
//   ./covered_call_backtest --csv VOO.csv --ticker VOO --grid
//
// expected CSV columns:
//   date,open,high,low,close
// or
//   date,open,high,low,close,volume

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <math.h>
#include <errno.h>

#ifdef _OPENMP
#include <omp.h>
#endif

#define RISK_FREE 0.04
#define DAYS_PER_YEAR 252.0
#define INITIAL_CAPACITY 4096
#define LINE_BUF 4096
#define MAX_MA_CACHE 512

typedef struct {
    char date[32];
    double open;
    double high;
    double low;
    double close;
    double daily_ret;
} Row;

typedef struct {
    Row *rows;
    size_t n;
    size_t cap;
} Data;

typedef struct {
    int active;
    double strike;
    double sigma;
    int expiration_index;
} OptionPos;

typedef struct {
    double CAGR;
    double MaxDD;
    double Final;
    double AvgTerm;
    double AvgEstSurvival;
    int Sales;
    int ForcedExits;
    int Expirations;
} Stats;

typedef struct {
    const char *csv_path;
    const char *ticker;
    int ma;
    int delay_days;
    double delta;
    double survival_prob;
    int grid;
} Args;

typedef struct {
    int ma;
    double *values;
} SmaCacheEntry;

typedef struct {
    int ma;
    int delay_days;
    double delta;
    double survival_prob;
} Job;

static SmaCacheEntry sma_cache[MAX_MA_CACHE];
static int sma_cache_count = 0;

static void die(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

static void *xmalloc(size_t n) {
    void *p = malloc(n);
    if (!p) die("malloc failed");
    return p;
}

static void *xrealloc(void *p, size_t n) {
    void *q = realloc(p, n);
    if (!q) die("realloc failed");
    return q;
}

static double norm_cdf(double x) {
    return 0.5 * (1.0 + erf(x / sqrt(2.0)));
}

static double call_delta(double S, double K, double T, double r, double sigma) {
    if (sigma <= 0.0 || T <= 0.0) return 0.0;
    double srt = sigma * sqrt(T);
    double d1 = (log(S / K) + (r + 0.5 * sigma * sigma) * T) / srt;
    return norm_cdf(d1);
}

static double call_price(double S, double K, double T, double r, double sigma) {
    if (sigma <= 0.0 || T <= 0.0) {
        double intrinsic = S - K;
        return intrinsic > 0.0 ? intrinsic : 0.0;
    }

    double srt = sigma * sqrt(T);
    double d1 = (log(S / K) + (r + 0.5 * sigma * sigma) * T) / srt;
    double d2 = d1 - srt;
    return S * norm_cdf(d1) - K * exp(-r * T) * norm_cdf(d2);
}

static double find_strike_fast(double S, double T, double r, double sigma, double target_delta) {
    double lo = 0.50 * S;
    double hi = 1.50 * S;

    for (int i = 0; i < 32; i++) {
        double mid = 0.5 * (lo + hi);
        double d = call_delta(S, mid, T, r, sigma);
        if (d > target_delta) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    return 0.5 * (lo + hi);
}

static double forced_sale_prob(
    double price,
    double sma,
    double sigma_ann,
    double mu_ann,
    int days
) {
    if (days <= 0) return 0.0;
    if (!(price > 0.0) || !(sma > 0.0)) return 1.0;
    if (price <= sma) return 1.0;

    double T = days / DAYS_PER_YEAR;
    double x = log(price / sma);

    if (sigma_ann < 1e-12) {
        if (mu_ann >= 0.0) return 0.0;
        return (x + mu_ann * T <= 0.0) ? 1.0 : 0.0;
    }

    double sig2 = sigma_ann * sigma_ann;
    double denom = sigma_ann * sqrt(T);

    double z1 = (-x - mu_ann * T) / denom;
    double z2 = (-x + mu_ann * T) / denom;

    double p = norm_cdf(z1) + exp(-2.0 * mu_ann * x / sig2) * norm_cdf(z2);

    if (p < 0.0) p = 0.0;
    if (p > 1.0) p = 1.0;
    return p;
}

/* Avoid selling a 20-day option when the price is only a smidge over
 * the moving average.
 * This code picks a time term for the call option to sell give the current price,
 * moving average, desired probability of selling out, and the stocks volatility.
 * It should return shorter terms when the stock is close to the moving average.
 */
static int choose_term(
    double price,
    double sma,
    double sigma_ann,
    double mu_ann,
    double target_survival,
    double *est_survival_out
) {
    const int terms[] = {20, 15, 10, 5};
    const int n_terms = (int)(sizeof(terms) / sizeof(terms[0]));

    for (int i = 0; i < n_terms; i++) {
        double forced = forced_sale_prob(price, sma, sigma_ann, mu_ann, terms[i]);
        double survival = 1.0 - forced;
        if (survival >= target_survival) {
            *est_survival_out = survival;
            return terms[i];
        }
    }

    *est_survival_out = 0.0;
    return 0;
}

static void data_init(Data *d) {
    d->n = 0;
    d->cap = INITIAL_CAPACITY;
    d->rows = (Row *)xmalloc(d->cap * sizeof(Row));
}

static void data_push(Data *d, const Row *r) {
    if (d->n == d->cap) {
        d->cap *= 2;
        d->rows = (Row *)xrealloc(d->rows, d->cap * sizeof(Row));
    }
    d->rows[d->n++] = *r;
}

static int parse_csv_line(char *line, Row *row) {
    char *fields[8];
    int nf = 0;

    char *p = line;
    while (*p && nf < 8) {
        fields[nf++] = p;
        char *comma = strchr(p, ',');
        if (!comma) break;
        *comma = '\0';
        p = comma + 1;
    }

    if (nf < 5) return 0;

    for (char *q = fields[nf - 1]; *q; q++) {
        if (*q == '\n' || *q == '\r') {
            *q = '\0';
            break;
        }
    }

    if (!strcasecmp(fields[0], "date")) return 0;

    snprintf(row->date, sizeof(row->date), "%s", fields[0]);
    row->open  = atof(fields[1]);
    row->high  = atof(fields[2]);
    row->low   = atof(fields[3]);
    row->close = atof(fields[4]);
    row->daily_ret = 0.0;
    return 1;
}

static void load_csv(const char *path, Data *d) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "could not open %s: %s\n", path, strerror(errno));
        exit(1);
    }

    char line[LINE_BUF];
    while (fgets(line, sizeof(line), fp)) {
        Row r;
        if (parse_csv_line(line, &r)) {
            data_push(d, &r);
        }
    }
    fclose(fp);

    if (d->n == 0) die("no data rows found in csv");
    if (d->rows[0].close <= 0.0) die("bad first close in csv");

    d->rows[0].daily_ret = 0.0;
    for (size_t i = 1; i < d->n; i++) {
        d->rows[i].daily_ret = d->rows[i].close / d->rows[i - 1].close - 1.0;
    }
}

static double *compute_sma_array(const Data *d, int window) {
    double *sma = (double *)xmalloc(d->n * sizeof(double));
    double sum = 0.0;

    for (size_t i = 0; i < d->n; i++) {
        sum += d->rows[i].close;
        if ((int)i >= window) {
            sum -= d->rows[i - window].close;
        }
        if ((int)i >= window - 1) {
            sma[i] = sum / (double)window;
        } else {
            sma[i] = NAN;
        }
    }

    return sma;
}

static const double *get_sma_array(const Data *d, int window) {
    for (int i = 0; i < sma_cache_count; i++) {
        if (sma_cache[i].ma == window) return sma_cache[i].values;
    }

    if (sma_cache_count >= MAX_MA_CACHE) die("too many SMA windows cached");

    sma_cache[sma_cache_count].ma = window;
    sma_cache[sma_cache_count].values = compute_sma_array(d, window);
    sma_cache_count++;
    return sma_cache[sma_cache_count - 1].values;
}

static double *precompute_sigma20(const Data *d) {
    double *sigma = (double *)xmalloc(d->n * sizeof(double));

    double sum = 0.0;
    double sumsq = 0.0;

    for (size_t i = 0; i < d->n; i++) {
        int add_idx = (int)i - 1;
        if (add_idx >= 1) {
            double x = d->rows[add_idx].daily_ret;
            sum += x;
            sumsq += x * x;
        }

        int remove_idx = (int)i - 21;
        if (remove_idx >= 1) {
            double x = d->rows[remove_idx].daily_ret;
            sum -= x;
            sumsq -= x * x;
        }

        int start = (int)i - 20;
        if (start < 1) start = 1;
        int end = (int)i;
        int n = end - start;

        if (n < 2) {
            sigma[i] = 0.20;
        } else {
            double mean = sum / (double)n;
            double var = (sumsq - (double)n * mean * mean) / (double)(n - 1);
            if (var < 0.0) var = 0.0;
            sigma[i] = sqrt(var) * sqrt(DAYS_PER_YEAR);
        }
    }

    return sigma;
}

static double *precompute_drift20(const Data *d) {
    double *drift = (double *)xmalloc(d->n * sizeof(double));
    double sum = 0.0;

    for (size_t i = 0; i < d->n; i++) {
        int add_idx = (int)i - 1;
        if (add_idx >= 1) {
            double x = log(d->rows[add_idx].close / d->rows[add_idx - 1].close);
            sum += x;
        }

        int remove_idx = (int)i - 21;
        if (remove_idx >= 1) {
            double x = log(d->rows[remove_idx].close / d->rows[remove_idx - 1].close);
            sum -= x;
        }

        int start = (int)i - 20;
        if (start < 1) start = 1;
        int end = (int)i;
        int n = end - start;

        if (n < 2) {
            drift[i] = 0.0;
        } else {
            drift[i] = (sum / (double)n) * DAYS_PER_YEAR;
        }
    }

    return drift;
}

static Stats simulate(
    const Data *d,
    const double *sma,
    const double *sigma20,
    const double *drift20,
    int delay_days,
    double target_delta,
    double target_survival
) {
    int invested = 1;
    double shares = 1.0 / d->rows[0].close;
    double cash = 0.0;
    OptionPos opt = {0, 0.0, 0.0, -1};
    int reentry_index = -1;
    int expirations = 0;
    int forced_exits = 0;

    double peak = -1.0;
    double maxdd = 0.0;
    double final_equity = 0.0;

    double total_term = 0.0;
    double total_est_survival = 0.0;
    int sales = 0;

    for (int i = 0; i < (int)d->n; i++) {
        double price = d->rows[i].close;
        int has_sma = !isnan(sma[i]);
        int just_exited = 0;

        if (invested && opt.active && opt.expiration_index == i) {
            double settled_equity = cash + shares * price
                - shares * fmax(0.0, price - opt.strike);
            shares = settled_equity / price;
            cash = 0.0;
            opt.active = 0;
            expirations++;
        }

        if (invested && has_sma && price < sma[i]) {
            double option_value = 0.0;
            if (opt.active) {
                int remaining_days = opt.expiration_index - i;
                if (remaining_days < 0) remaining_days = 0;
                double T = remaining_days > 0 ? remaining_days / DAYS_PER_YEAR : 0.0;
                option_value = call_price(price, opt.strike, T, RISK_FREE, opt.sigma);
                forced_exits++;
            }
            cash = cash + shares * price - shares * option_value;
            shares = 0.0;
            opt.active = 0;
            invested = 0;
            reentry_index = i + delay_days;
            just_exited = 1;
        }

        if (!invested) {
            if (!just_exited) {
                cash *= 1.0 + RISK_FREE / DAYS_PER_YEAR;
            }
            if (has_sma && i >= reentry_index && price > sma[i]) {
                invested = 1;
                shares = cash / price;
                cash = 0.0;
            }
        }

        if (invested && !opt.active) {
            int option_days = 0;
            double est_survival = 0.0;

            if (has_sma) {
                option_days = choose_term(
                    price,
                    sma[i],
                    sigma20[i],
                    drift20[i],
                    target_survival,
                    &est_survival
                );
            } else {
                option_days = 20;
                est_survival = 1.0;
            }

            if (option_days > 0) {
                int expiration_index = i + option_days;
                if (expiration_index >= (int)d->n) expiration_index = (int)d->n - 1;

                double T = (expiration_index - i) / DAYS_PER_YEAR;
                if (T < 1.0 / DAYS_PER_YEAR) T = 1.0 / DAYS_PER_YEAR;

                double sigma = sigma20[i];
                double strike = find_strike_fast(price, T, RISK_FREE, sigma, target_delta);
                double premium = call_price(price, strike, T, RISK_FREE, sigma);

                cash += shares * premium;
                opt.active = 1;
                opt.strike = strike;
                opt.sigma = sigma;
                opt.expiration_index = expiration_index;

                total_term += option_days;
                total_est_survival += est_survival;
                sales++;
            }
        }

        double option_value = 0.0;
        if (invested && opt.active) {
            int remaining_days = opt.expiration_index - i;
            if (remaining_days < 0) remaining_days = 0;
            double T = remaining_days > 0 ? remaining_days / DAYS_PER_YEAR : 0.0;
            option_value = call_price(price, opt.strike, T, RISK_FREE, opt.sigma);
        }

        double equity = invested ? (cash + shares * price - shares * option_value) : cash;
        final_equity = equity;

        if (peak < 0.0 || equity > peak) peak = equity;
        double dd = equity / peak - 1.0;
        if (dd < maxdd) maxdd = dd;
    }

    Stats s;
    s.Final = final_equity;
    {
        double years = ((double)d->n - 1.0) / DAYS_PER_YEAR;
        if (years < 1.0 / DAYS_PER_YEAR) years = 1.0 / DAYS_PER_YEAR;
        s.CAGR = pow(s.Final, 1.0 / years) - 1.0;
    }
    s.MaxDD = maxdd;
    s.Sales = sales;
    s.AvgTerm = (sales > 0) ? (total_term / (double)sales) : 0.0;
    s.AvgEstSurvival = (sales > 0) ? (total_est_survival / (double)sales) : 0.0;
    s.ForcedExits = forced_exits;
    s.Expirations = expirations;
    return s;
}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s --csv FILE [--ticker TICKER] [--ma N] [--delta X] [--delay-days N] [--survival-prob X] [--grid]\n",
        prog
    );
    exit(1);
}

static Args parse_args(int argc, char **argv) {
    Args a;
    a.csv_path = NULL;
    a.ticker = "VOO";
    a.ma = 200;
    a.delay_days = 5;
    a.delta = 0.30;
    a.survival_prob = 0.80;
    a.grid = 0;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--csv")) {
            if (++i >= argc) usage(argv[0]);
            a.csv_path = argv[i];
        } else if (!strcmp(argv[i], "--ticker")) {
            if (++i >= argc) usage(argv[0]);
            a.ticker = argv[i];
        } else if (!strcmp(argv[i], "--ma")) {
            if (++i >= argc) usage(argv[0]);
            a.ma = atoi(argv[i]);
        } else if (!strcmp(argv[i], "--delay-days")) {
            if (++i >= argc) usage(argv[0]);
            a.delay_days = atoi(argv[i]);
        } else if (!strcmp(argv[i], "--delta")) {
            if (++i >= argc) usage(argv[0]);
            a.delta = atof(argv[i]);
        } else if (!strcmp(argv[i], "--survival-prob")) {
            if (++i >= argc) usage(argv[0]);
            a.survival_prob = atof(argv[i]);
        } else if (!strcmp(argv[i], "--grid")) {
            a.grid = 1;
        } else {
            usage(argv[0]);
        }
    }

    if (!a.csv_path) usage(argv[0]);
    return a;
}

static void run_grid(
    const Data *d,
    const char *ticker,
    const double *sigma20,
    const double *drift20
) {
    const int mas[] = {125, 150, 175, 200, 225};
    const int delays[] = {0, 5, 10};
    const double deltas[] = {0.10, 0.15, 0.20, 0.25, 0.30};
    const double survival_probs[] = {0.50, 0.60, 0.70, 0.80, 0.90, 0.95};

    const int n_mas = (int)(sizeof(mas) / sizeof(mas[0]));
    const int n_delays = (int)(sizeof(delays) / sizeof(delays[0]));
    const int n_deltas = (int)(sizeof(deltas) / sizeof(deltas[0]));
    const int n_surv = (int)(sizeof(survival_probs) / sizeof(survival_probs[0]));

    int njobs = n_mas * n_delays * n_deltas * n_surv;
    Job *jobs = (Job *)xmalloc(njobs * sizeof(Job));
    int k = 0;

    for (int i = 0; i < n_mas; i++) {
        for (int j = 0; j < n_delays; j++) {
            for (int m = 0; m < n_deltas; m++) {
                for (int n = 0; n < n_surv; n++) {
                    jobs[k].ma = mas[i];
                    jobs[k].delay_days = delays[j];
                    jobs[k].delta = deltas[m];
                    jobs[k].survival_prob = survival_probs[n];
                    k++;
                }
            }
        }
    }

    Stats best_stats = {0};
    Job best_job = {0};

    #pragma omp parallel
    {
        Stats thread_best_stats = {0};
        Job thread_best_job = {0};
        int thread_has_best = 0;

        #pragma omp for schedule(dynamic)
        for (int i = 0; i < njobs; i++) {
            const double *sma = get_sma_array(d, jobs[i].ma);
            Stats s = simulate(
                d,
                sma,
                sigma20,
                drift20,
                jobs[i].delay_days,
                jobs[i].delta,
                jobs[i].survival_prob
            );

            #pragma omp critical
            {
                double forced_rate = (s.Sales > 0) ? ((double)s.ForcedExits / s.Sales) : 0.0;
                printf(
                    "ticker=%s ma=%d delay_days=%d delta=%.2f survival_prob=%.2f forced_rate = %.3f forced_exits=%d expirations=%d avg_term=%.2f avg_est_survival=%.4f sales=%d CAGR=%.4f%% MaxDD=%.4f%% Final=%.4f\n",
                    ticker,
                    jobs[i].ma,
                    jobs[i].delay_days,
                    jobs[i].delta,
                    jobs[i].survival_prob,
		    forced_rate,
		    s.ForcedExits,
		    s.Expirations,
                    s.AvgTerm,
                    s.AvgEstSurvival,
                    s.Sales,
                    s.CAGR * 100.0,
                    s.MaxDD * 100.0,
                    s.Final
                );
            }

            if (!thread_has_best || s.Final > thread_best_stats.Final) {
                thread_best_stats = s;
                thread_best_job = jobs[i];
                thread_has_best = 1;
            }
        }

        #pragma omp critical
        {
            if (thread_has_best && thread_best_stats.Final > best_stats.Final) {
                best_stats = thread_best_stats;
                best_job = thread_best_job;
            }
        }
    }

    double forced_rate = (best_stats.Sales > 0) ? ((double)best_stats.ForcedExits / best_stats.Sales) : 0.0;
    printf(
        "\nBEST: ticker=%s ma=%d delay_days=%d delta=%.2f survival_prob=%.2f forced_rate = %.3f forced_exits=%d expirations=%d avg_term=%.2f avg_est_survival=%.4f sales=%d CAGR=%.4f%% MaxDD=%.4f%% Final=%.4f\n",
        ticker,
        best_job.ma,
        best_job.delay_days,
        best_job.delta,
        best_job.survival_prob,
        forced_rate,
        best_stats.ForcedExits,
        best_stats.Expirations,
        best_stats.AvgTerm,
        best_stats.AvgEstSurvival,
        best_stats.Sales,
        best_stats.CAGR * 100.0,
        best_stats.MaxDD * 100.0,
        best_stats.Final
    );

    free(jobs);
}

int main(int argc, char **argv) {
    Args args = parse_args(argc, argv);

    Data d;
    data_init(&d);
    load_csv(args.csv_path, &d);

    double *sigma20 = precompute_sigma20(&d);
    double *drift20 = precompute_drift20(&d);

    if (args.grid) {
        run_grid(&d, args.ticker, sigma20, drift20);
    } else {
        const double *sma = get_sma_array(&d, args.ma);
        Stats s = simulate(
            &d,
            sma,
            sigma20,
            drift20,
            args.delay_days,
            args.delta,
            args.survival_prob
        );

        double forced_rate = (s.Sales > 0) ? ((double)s.ForcedExits / s.Sales) : 0.0;
        printf(
            "ticker=%s ma=%d delay_days=%d delta=%.2f survival_prob=%.2f forced_rate = %.3f forced_exits=%d expirations=%d avg_term=%.2f avg_est_survival=%.4f sales=%d CAGR=%.4f%% MaxDD=%.4f%% Final=%.4f\n",
            args.ticker,
            args.ma,
            args.delay_days,
            args.delta,
            args.survival_prob,
            forced_rate,
            s.ForcedExits,
            s.Expirations,
            s.AvgTerm,
            s.AvgEstSurvival,
            s.Sales,
            s.CAGR * 100.0,
            s.MaxDD * 100.0,
            s.Final
        );
    }

    for (int i = 0; i < sma_cache_count; i++) {
        free(sma_cache[i].values);
    }
    free(drift20);
    free(sigma20);
    free(d.rows);
    return 0;
}
