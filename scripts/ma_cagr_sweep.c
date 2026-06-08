// build:
//   gcc -O3 -march=native -ffast-math -fopenmp -o ma_cagr_sweep scripts/ma_cagr_sweep.c -lm
//
// example:
//   ./ma_cagr_sweep --csv /home/alan/investing/chart-cache/VOO.csv --output voo_ma_cagr_sweep.csv

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#ifdef _OPENMP
#include <omp.h>
#endif

#define DAYS_PER_YEAR 252.0
#define RISK_FREE 0.04
#define INITIAL_CAPACITY 4096
#define LINE_BUF 4096

typedef struct {
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
    double intercept;
    double linear;
    double quadratic;
    int valid;
} Calibration;

typedef struct {
    const char *csv_path;
    const char *output_path;
    int min_days;
    int max_days;
    double delta;
    int delay_days;
    double survival_prob;
} Args;

typedef struct {
    const char *average_type;
    int days;
    double cagr_pct;
} ResultRow;

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
    row->close = atof(fields[4]);
    row->daily_ret = 0.0;
    return row->close > 0.0;
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
    d->rows[0].daily_ret = 0.0;
    for (size_t i = 1; i < d->n; i++) {
        d->rows[i].daily_ret = d->rows[i].close / d->rows[i - 1].close - 1.0;
    }
}

static double norm_cdf(double x) {
    return 0.5 * (1.0 + erf(x / sqrt(2.0)));
}

static double call_delta(double S, double K, double T, double r, double sigma) {
    if (sigma <= 0.0 || T <= 0.0 || S <= 0.0 || K <= 0.0) return 0.0;
    double srt = sigma * sqrt(T);
    double d1 = (log(S / K) + (r + 0.5 * sigma * sigma) * T) / srt;
    return norm_cdf(d1);
}

static double call_price(double S, double K, double T, double r, double sigma) {
    if (sigma <= 0.0 || T <= 0.0 || S <= 0.0 || K <= 0.0) {
        double intrinsic = S - K;
        return intrinsic > 0.0 ? intrinsic : 0.0;
    }
    double srt = sigma * sqrt(T);
    double d1 = (log(S / K) + (r + 0.5 * sigma * sigma) * T) / srt;
    double d2 = d1 - srt;
    return S * norm_cdf(d1) - K * exp(-r * T) * norm_cdf(d2);
}

static double find_strike(double S, double T, double r, double sigma, double target_delta) {
    double best_strike = S;
    double best_error = HUGE_VAL;
    for (int i = 0; i < 400; i++) {
        double strike = S * (0.90 + (0.30 * (double)i / 399.0));
        double err = fabs(call_delta(S, strike, T, r, sigma) - target_delta);
        if (err < best_error) {
            best_error = err;
            best_strike = strike;
        }
    }
    return best_strike;
}

static double *compute_sma_array(const Data *d, int window) {
    double *out = (double *)xmalloc(d->n * sizeof(double));
    double sum = 0.0;
    for (size_t i = 0; i < d->n; i++) {
        sum += d->rows[i].close;
        if ((int)i >= window) sum -= d->rows[i - window].close;
        out[i] = ((int)i >= window - 1) ? (sum / (double)window) : NAN;
    }
    return out;
}

static double *compute_ema_array(const Data *d, int window) {
    double *out = (double *)xmalloc(d->n * sizeof(double));
    double sum = 0.0;
    double multiplier = 2.0 / (window + 1.0);
    for (size_t i = 0; i < d->n; i++) {
        out[i] = NAN;
        if ((int)i < window) {
            sum += d->rows[i].close;
            if ((int)i == window - 1) out[i] = sum / (double)window;
            continue;
        }
        out[i] = ((d->rows[i].close - out[i - 1]) * multiplier) + out[i - 1];
    }
    return out;
}

static double *compute_ma_array(const Data *d, int window, const char *average_type) {
    return !strcmp(average_type, "EMA")
        ? compute_ema_array(d, window)
        : compute_sma_array(d, window);
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
        if (add_idx >= 1) sum += log(d->rows[add_idx].close / d->rows[add_idx - 1].close);
        int remove_idx = (int)i - 21;
        if (remove_idx >= 1) sum -= log(d->rows[remove_idx].close / d->rows[remove_idx - 1].close);
        int start = (int)i - 20;
        if (start < 1) start = 1;
        int end = (int)i;
        int n = end - start;
        drift[i] = (n < 2) ? 0.0 : ((sum / (double)n) * DAYS_PER_YEAR);
    }
    return drift;
}

static double forced_sale_prob(double price, double ma, double sigma_ann, double mu_ann, int days) {
    if (days <= 0) return 0.0;
    if (!(price > 0.0) || !(ma > 0.0)) return 1.0;
    if (price <= ma) return 1.0;

    double T = days / DAYS_PER_YEAR;
    double x = log(price / ma);

    if (sigma_ann < 1e-12) {
        if (mu_ann >= 0.0) return 0.0;
        return (x + mu_ann * T <= 0.0) ? 1.0 : 0.0;
    }

    double sig2 = sigma_ann * sigma_ann;
    double denom = sigma_ann * sqrt(T);
    double z1 = (-x - mu_ann * T) / denom;
    double z2 = (-x + mu_ann * T) / denom;
    double p = norm_cdf(z1) + exp((-2.0 * mu_ann * x) / sig2) * norm_cdf(z2);
    if (p < 0.0) p = 0.0;
    if (p > 1.0) p = 1.0;
    return p;
}

static Calibration fit_quadratic(const double *xs, const double *ys, int n) {
    Calibration model = {0.0, 0.0, 0.0, 0};
    if (n < 12) return model;

    double sx = 0.0, sx2 = 0.0, sx3 = 0.0, sx4 = 0.0;
    double sy = 0.0, sxy = 0.0, sx2y = 0.0;
    for (int i = 0; i < n; i++) {
        double x = xs[i];
        double y = ys[i];
        double x2 = x * x;
        sx += x;
        sx2 += x2;
        sx3 += x2 * x;
        sx4 += x2 * x2;
        sy += y;
        sxy += x * y;
        sx2y += x2 * y;
    }

    double a[3][4] = {
        {(double)n, sx,  sx2, sy},
        {sx,        sx2, sx3, sxy},
        {sx2,       sx3, sx4, sx2y}
    };

    for (int pivot = 0; pivot < 3; pivot++) {
        int max_row = pivot;
        for (int row = pivot + 1; row < 3; row++) {
            if (fabs(a[row][pivot]) > fabs(a[max_row][pivot])) max_row = row;
        }
        if (fabs(a[max_row][pivot]) < 1e-12) return model;
        if (max_row != pivot) {
            for (int col = pivot; col < 4; col++) {
                double tmp = a[pivot][col];
                a[pivot][col] = a[max_row][col];
                a[max_row][col] = tmp;
            }
        }
        double pv = a[pivot][pivot];
        for (int col = pivot; col < 4; col++) a[pivot][col] /= pv;
        for (int row = 0; row < 3; row++) {
            if (row == pivot) continue;
            double factor = a[row][pivot];
            for (int col = pivot; col < 4; col++) a[row][col] -= factor * a[pivot][col];
        }
    }

    model.intercept = a[0][3];
    model.linear = a[1][3];
    model.quadratic = a[2][3];
    model.valid = 1;
    return model;
}

static double calibrated_probability(Calibration model, double raw_value) {
    double x = raw_value;
    if (x < 0.0) x = 0.0;
    if (x > 1.0) x = 1.0;
    if (!model.valid) return x;
    double fitted = model.intercept + model.linear * x + model.quadratic * x * x;
    if (fitted < 0.0) fitted = 0.0;
    if (fitted > 1.0) fitted = 1.0;
    return fitted;
}

static void build_survival_calibration(
    const Data *d,
    const double *ma,
    const double *sigma20,
    const double *drift20,
    double target_delta,
    Calibration models[4]
) {
    const int terms[4] = {20, 15, 10, 5};
    for (int t = 0; t < 4; t++) {
        double *xs = (double *)xmalloc(d->n * sizeof(double));
        double *ys = (double *)xmalloc(d->n * sizeof(double));
        int n = 0;
        int term_days = terms[t];

        for (int index = 0; index + term_days < (int)d->n; index++) {
            double moving_average = ma[index];
            double price = d->rows[index].close;
            if (isnan(moving_average) || !(price > moving_average)) continue;

            double raw_survival = 1.0 - forced_sale_prob(price, moving_average, sigma20[index], drift20[index], term_days);
            int expiration_index = index + term_days;
            if (expiration_index >= (int)d->n) expiration_index = (int)d->n - 1;
            double T = expiration_index > index ? (expiration_index - index) / DAYS_PER_YEAR : (1.0 / DAYS_PER_YEAR);
            if (T < 1.0 / DAYS_PER_YEAR) T = 1.0 / DAYS_PER_YEAR;
            double strike = find_strike(price, T, RISK_FREE, sigma20[index], target_delta);

            int forced_buyback = 0;
            for (int future = index + 1; future < expiration_index; future++) {
                double future_ma = ma[future];
                if (!isnan(future_ma) && d->rows[future].close < future_ma) {
                    forced_buyback = 1;
                    break;
                }
            }

            int expired_otm = (!forced_buyback && d->rows[expiration_index].close <= strike) ? 1 : 0;
            xs[n] = raw_survival;
            ys[n] = (double)expired_otm;
            n++;
        }

        models[t] = fit_quadratic(xs, ys, n);
        free(xs);
        free(ys);
    }
}

static int choose_term(
    const Data *d,
    const double *ma,
    const double *sigma20,
    const double *drift20,
    int index,
    double target_survival,
    Calibration models[4]
) {
    const int terms[4] = {20, 15, 10, 5};
    double price = d->rows[index].close;
    double moving_average = ma[index];
    if (isnan(moving_average)) return 20;
    if (!(price > moving_average)) return 0;

    for (int t = 0; t < 4; t++) {
        int term_days = terms[t];
        double raw_survival = 1.0 - forced_sale_prob(price, moving_average, sigma20[index], drift20[index], term_days);
        double survival = calibrated_probability(models[t], raw_survival);
        if (survival >= target_survival) return term_days;
    }
    return 0;
}

static double option_market_value(OptionPos opt, double spot, int index) {
    if (!opt.active) return 0.0;
    int remaining_days = opt.expiration_index - index;
    if (remaining_days < 0) remaining_days = 0;
    double T = remaining_days > 0 ? remaining_days / DAYS_PER_YEAR : 0.0;
    return call_price(spot, opt.strike, T, RISK_FREE, opt.sigma);
}

static double simulate_cagr(
    const Data *d,
    const double *ma,
    const double *sigma20,
    const double *drift20,
    int delay_days,
    double target_delta,
    double target_survival
) {
    Calibration models[4];
    build_survival_calibration(d, ma, sigma20, drift20, target_delta, models);

    int invested = 1;
    double shares = 1.0 / d->rows[0].close;
    double cash = 0.0;
    OptionPos opt = {0, 0.0, 0.0, -1};
    int reentry_index = -1;
    double final_equity = 0.0;

    for (int i = 0; i < (int)d->n; i++) {
        double price = d->rows[i].close;
        double moving_average = ma[i];
        int has_ma = !isnan(moving_average);
        int just_exited = 0;

        if (invested && opt.active && opt.expiration_index == i) {
            double settled_equity = cash + shares * price - shares * fmax(0.0, price - opt.strike);
            shares = settled_equity / price;
            cash = 0.0;
            opt.active = 0;
        }

        if (invested && has_ma && price < moving_average) {
            double option_value = option_market_value(opt, price, i);
            cash = cash + shares * price - shares * option_value;
            shares = 0.0;
            opt.active = 0;
            invested = 0;
            reentry_index = i + delay_days;
            just_exited = 1;
        }

        if (!invested) {
            if (!just_exited) cash *= 1.0 + RISK_FREE / DAYS_PER_YEAR;
            if (has_ma && i >= reentry_index && price > moving_average) {
                invested = 1;
                shares = cash / price;
                cash = 0.0;
            }
        }

        if (invested && !opt.active) {
            int option_days = choose_term(d, ma, sigma20, drift20, i, target_survival, models);
            if (option_days > 0) {
                int expiration_index = i + option_days;
                if (expiration_index >= (int)d->n) expiration_index = (int)d->n - 1;
                double T = (expiration_index - i) / DAYS_PER_YEAR;
                if (T < 1.0 / DAYS_PER_YEAR) T = 1.0 / DAYS_PER_YEAR;
                double sigma = sigma20[i];
                double strike = find_strike(price, T, RISK_FREE, sigma, target_delta);
                double premium = call_price(price, strike, T, RISK_FREE, sigma);
                cash += shares * premium;
                opt.active = 1;
                opt.strike = strike;
                opt.sigma = sigma;
                opt.expiration_index = expiration_index;
            }
        }

        double option_value = invested ? option_market_value(opt, price, i) : 0.0;
        final_equity = invested ? (cash + shares * price - shares * option_value) : cash;
    }

    double years = ((double)d->n - 1.0) / DAYS_PER_YEAR;
    if (years < 1.0 / DAYS_PER_YEAR) years = 1.0 / DAYS_PER_YEAR;
    return (pow(final_equity, 1.0 / years) - 1.0) * 100.0;
}

static Args parse_args(int argc, char **argv) {
    Args a;
    a.csv_path = NULL;
    a.output_path = NULL;
    a.min_days = 1;
    a.max_days = 210;
    a.delta = 0.20;
    a.delay_days = 10;
    a.survival_prob = 0.50;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--csv") && i + 1 < argc) {
            a.csv_path = argv[++i];
        } else if (!strcmp(argv[i], "--output") && i + 1 < argc) {
            a.output_path = argv[++i];
        } else if (!strcmp(argv[i], "--min-days") && i + 1 < argc) {
            a.min_days = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--max-days") && i + 1 < argc) {
            a.max_days = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--delta") && i + 1 < argc) {
            a.delta = atof(argv[++i]);
        } else if (!strcmp(argv[i], "--delay-days") && i + 1 < argc) {
            a.delay_days = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--survival-prob") && i + 1 < argc) {
            a.survival_prob = atof(argv[++i]);
        } else {
            die("usage: ma_cagr_sweep --csv FILE --output FILE [--min-days N] [--max-days N] [--delta X] [--delay-days N] [--survival-prob X]");
        }
    }

    if (!a.csv_path || !a.output_path) die("both --csv and --output are required");
    return a;
}

int main(int argc, char **argv) {
    Args args = parse_args(argc, argv);
    Data d;
    data_init(&d);
    load_csv(args.csv_path, &d);

    if ((int)d.n < args.max_days + 20) die("not enough rows for requested max window");

    double *sigma20 = precompute_sigma20(&d);
    double *drift20 = precompute_drift20(&d);

    int rows = (args.max_days - args.min_days + 1) * 2;
    ResultRow *results = (ResultRow *)xmalloc((size_t)rows * sizeof(ResultRow));

    #pragma omp parallel for schedule(dynamic)
    for (int job = 0; job < rows; job++) {
        const char *average_type = (job < (args.max_days - args.min_days + 1)) ? "SMA" : "EMA";
        int offset = (job < (args.max_days - args.min_days + 1)) ? job : (job - (args.max_days - args.min_days + 1));
        int days = args.min_days + offset;
        double *ma = compute_ma_array(&d, days, average_type);
        double cagr_pct = simulate_cagr(&d, ma, sigma20, drift20, args.delay_days, args.delta, args.survival_prob);
        free(ma);

        results[job].average_type = average_type;
        results[job].days = days;
        results[job].cagr_pct = cagr_pct;
    }

    FILE *out = fopen(args.output_path, "w");
    if (!out) {
        fprintf(stderr, "could not open %s for writing: %s\n", args.output_path, strerror(errno));
        exit(1);
    }
    fprintf(out, "average_type,days,cagr\n");
    for (int i = 0; i < rows; i++) {
        fprintf(out, "%s,%d,%.6f\n", results[i].average_type, results[i].days, results[i].cagr_pct);
    }
    fclose(out);

    free(results);
    free(drift20);
    free(sigma20);
    free(d.rows);
    printf("wrote %d rows to %s\n", rows, args.output_path);
    return 0;
}
