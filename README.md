> [!IMPORTANT]
> **This repo is archived.** Use [cronphase](https://github.com/acus9345/cronphase) instead; it does the same job and is maintained.

# `unherd` — Cure Cron Thundering Herds & Synchronized Retry Storms

```text
BEFORE unherd (00:00..00:59 UTC) : [█    ▄    ▄   █▄    ▄    ▄   █▄    ▄    ▄   █▄    ▄    ▄    ] Peak: 15 jobs (PAR 12.6x)
AFTER  unherd (00:00..00:59 UTC) : [▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁▂▁] Peak:  2 jobs (PAR  1.8x)
```

Every engineering organization eventually DDoSes its own database at `:00` UTC.

Human engineers overwhelmingly write cron schedules using round numbers: `0 * * * *`, `*/5 * * * *`, `*/15 * * * *`, and `0 0 * * *`. Across a fleet of microservices, Kubernetes `CronJob` manifests, and GitHub Actions workflows, these schedules constructively interfere at minute `:00`, `:15`, `:30`, and `:45`—creating **deterministic thundering herds** and **synchronized exponential backoff retry storms**.

**`unherd`** is a zero-dependency static analyzer, deterministic phase-shift solver, retry-storm physics simulator, and interactive oscilloscope workbench that de-synchronizes your fleet schedules while **strictly preserving every job's exact SLA cadence and daily execution count**.

---

## Why `unherd`?

| Existing Tool | What It Does | What It Misses |
| :--- | :--- | :--- |
| `crontab.guru` | Explains a single cron string in English | Blind to fleet-wide collisions across 20+ jobs |
| Jenkins `H * * * *` | Runtime hash jitter inside Jenkins only | Non-portable to POSIX cron, K8s `CronJob`, GitHub Actions, or Supabase `pg_cron` |
| Random `sleep $((RANDOM))` | Adds ad-hoc shell delay before job execution | Delays observability, breaks idempotency windows, and hides true schedule in config |
| **`unherd`** | **Static fleet-wide collision solver & diff generator** | Emits standard POSIX cron (`3-59/5 * * * *`, `17 * * * *`) compatible with every scheduler on earth |

---

## Quickstart (Zero Dependencies, Python >= 3.9)

```bash
# 1. Audit temporal collision density across a crontab or Kubernetes YAML manifest
python3 -m unherd analyze examples/production_fleet.crontab

# 2. De-synchronize the fleet and output a ready-to-apply unified git diff (`diff -u`)
python3 -m unherd optimize examples/production_fleet.crontab --allow-hour-shift --diff

# 3. Simulate a 15-second downstream DB brownout at the worst collision spike
python3 -m unherd simulate examples/production_fleet.crontab

# 4. Run the verification suite
python3 -m unittest discover -s tests -v
```

---

## How the Solver Works (`unherd/engine.py`)

Given a set of $N$ scheduled jobs $J_1, \dots, J_N$, each job $J_i$ has:
- A set of scheduled fire minutes $T_i \subset \{0, \dots, 1439\}$
- An execution duration spillover $\Delta_i = \lceil \text{duration}_i / 60 \rceil$
- A resource load weight $w_i \in \mathbb{R}^+$
- An optional `pinned` constraint (for strict regulatory/cutoff jobs that must not move)

For periodic jobs with intra-hour period $P_i \in \{5, 10, 15, 20, 30, 60\}$, `unherd` searches phase offsets $\phi_i \in \{0, \dots, P_i - 1\}$ (rewriting `*/5` as $\phi_i\text{-59}/5$, which is 100% standard POSIX cron) and optional hour offsets $\theta_i$ to minimize the lexicographic objective over the 1,440-minute diurnal ring $\mathbb{Z}_{1440}$:

$$\min_{\{\phi_i, \theta_i\}} \left( \max_{t \in \mathbb{Z}_{1440}} L(t), \;\sum_{t=0}^{1439} L(t)^3 + \frac{1}{2} C(t)^2 \right)$$

subject to the **Strict Cadence Invariance Constraint**:
$$|T_i(\phi_i, \theta_i)| = |T_i(0, 0)| \quad \text{and} \quad \Delta t_{\text{consecutive}}(J_i) = P_i \quad \forall i$$

---

## Inline Metadata Directives

`unherd` works out-of-the-box on raw crontabs and YAML files, and also understands optional inline comment annotations for high-precision load modeling:

```cron
#Format: <cron> <command> # name=<id> weight=<0.1..100> duration=<sec> [pinned]
*/5 * * * * /srv/bin/sync-webhooks   # name=stripe-sync weight=2.5 duration=18s
0 0 * * *   /srv/bin/ledger-seal     # name=audit-seal weight=2.0 duration=20s pinned
```

---

## Repository Layout

```text
unherd/
├── pyproject.toml                  # Zero-dependency Python package config
├── README.md                       # Architecture & CLI documentation
├── LICENSE                         # MIT License
├── unherd/
│   ├── __init__.py                 # Public library exports
│   ├── __main__.py                 # Module execution entry point
│   ├── parser.py                   # POSIX cron, @macro, Crontab & K8s/GHA YAML parser
│   ├── engine.py                   # 1440-min analyzer, L_p phase solver & retry simulator
│   └── cli.py                      # Terminal sparkline & unified diff CLI
├── examples/
│   ├── production_fleet.crontab    # 16-job SaaS crontab with :00 / */5 pileups
│   └── k8s_cronjobs.yaml           # Multi-manifest Kubernetes batch/v1 CronJobs
├── tests/
│   └── test_unherd.py              # Unit & property-invariance test suite
└── web/
    └── index.html                  # Standalone Tektronix/Swiss-grid interactive workbench
```
