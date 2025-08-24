Here’s a complete, evaluator-oriented **README.md** you can drop in the repo.

---

# Disproving Carrier: *Proving History the Way Historians Do, but With Data*

Reproducible pipeline to assess **historicity** from **written-evidence footprints** encoded in Wikidata.
We accept the challenge of *quantifying* whether a figure existed—doing it the right way: **with data rather than stipulated priors**.

---

## Contents

* [Overview](#overview)
* [Repository layout](#repository-layout)
* [Data provenance & sampling frame](#data-provenance--sampling-frame)
* [Entity harvesting & normalization](#entity-harvesting--normalization)
* [Feature construction & labels](#feature-construction--labels)
* [Modeling & validation](#modeling--validation)
* [Scenarios (ablations) & stress tests](#scenarios-ablations--stress-tests)
* [Reproducibility guide](#reproducibility-guide)

  * [Quick reproduce (minutes)](#quick-reproduce-minutes)
  * [Full reproduce (from WDQS)](#full-reproduce-from-wdqs)
* [Outputs & where results live](#outputs--where-results-live)
* [Determinism, seeds & snapshots](#determinism-seeds--snapshots)
* [Limitations & ethical notes](#limitations--ethical-notes)
* [How to cite](#how-to-cite)
* [Licenses](#licenses)

---

## Overview

This project operationalizes the way historians weigh written evidence by encoding each item’s **documentary properties** (presence/absence of Wikidata PIDs) and learning how such footprints differ between **uncontested humans** and **mythic/legendary** entries. We:

1. Build a transparent, versioned **sampling frame** from Wikidata (WDQS).
2. Harvest canonical JSON for each QID and normalize to a long table.
3. Construct a sparse **property-presence** matrix with strong **editorial guardrails**.
4. Fit **ridge-penalized logistic** models with fold-wise validation and explicit thresholding.
5. Probe robustness via **ablations**, an **editorial anchor** control, and **random-subset** stress tests.

The current snapshot (reported in the manuscript) includes **11,943 items**
(4,525 historical; 7,418 mythic) and **106 properties** after filters and constraints.

---

## Repository layout

```
disproving-carrier/
├─ README.md
├─ .gitignore
├─ .Rprofile
├─ requirements.txt   # Python deps (harvesting)
├─ queries/           # WDQS SPARQL queries (seed cohorts)
│  ├─ historical_cohort_100BCE_100CE.rq
│  └─ mythic_cohort_classes.rq
├─ src/
│  ├─ R/
│  │  ├─ analyze_wikidata_logistic.R
│  │  ├─ evaluate_label_strategies.R
│  │  ├─ make_features_long_from_all_properties.R
│  │  ├─ resolve_qids_safe_.R
│  │  └─ validate_anchors.R
│  └─ python/
│     └─ wikidata_dump.py
├─ data/
│  ├─ raw/            # WDQS seeds and logs
│  ├─ anchors/        # independent labeling control
│  ├─ misc/            # lookups (e.g., list_of_properties.xlsx)
│  └─ processed/      # long features, metadata (derived)
└─ out/


```

> We version **queries**, **scripts**, and **final artifacts**; we do **not** version caches or large raw dumps.

---

## Data provenance & sampling frame

* **Source:** Wikidata via **WDQS** (SPARQL 1.1).
* **Cohorts:**

  * **Early historical (humans):** `P31 = Q5`, dated **100 BCE–100 CE** by `P569` (birth) or `P570` (death).
    *Rationale:* align Jesus with contemporaneous figures so the **documentary opportunity set** is comparable; avoids the model learning “modern vs. antiquity” proxies driven by survival of sources or administrative change.
  * **Mythic/legendary:** controlled set of mythic instance classes (e.g., deity, demigod, mythological character).
* **Pagination & rate-limiting:** paged with `LIMIT/OFFSET`, \~1 req/s; timestamps and endpoint metadata logged.
* **Reproducibility:** exact queries live in `queries/`; seeds exported to `data/raw/` with dated filenames.

> **Important:** `P569/P570` are used **only** to seed the historical cohort’s period filter. They are **globally excluded** from downstream features to avoid **self-defining** leakage.

---

## Entity harvesting & normalization

* Endpoint: `Special:EntityData` (HTTPS).
* **Polite access:** content-type checks, retries with exponential back-off (incl. HTTP 429), on-disk cache in `data/cache/`.
* **Normalization:** every statement → long table with `qid`, `property` (PID), `rank`, `snaktype`, and canonical `value_raw` (QID / ISO 8601 time / numeric).
* **Labels:** English labels/descriptions are cached for items and QID-valued `value_raw` for interpretability.

---

## Feature construction & labels

* **Binary presence encoding:** build sparse matrix $X$ (QID × PID), where $X_{ij}=1$ iff item *i* has ≥1 statement for property *j*.
* **Editorial guardrails:**

  * Exclude **`P31`** (the ontological label) from predictors (prevents label leakage).
  * Exclude **External Identifiers** (datatype `EI`) to temper documentation/connectivity bias.
  * **Globally exclude `P569`/`P570`** since they were used in sampling (see above).
  * Filter **rare properties** (minimum QID count) for numerical stability.
  * Enforce a **common-property constraint**: every retained property must occur at least once in **both classes** (prevents trivial cohort artifacts).
* **Label $y$:** `1` if `P31 = Q5` (human), `0` otherwise. The label is **never** used as a feature.

---

## Modeling & validation

* **Model:** ridge-penalized logistic regression (**glmnet**, coordinate descent).
* **CV protocol:** **stratified 5-fold CV**, **repeated 10×**, preserving class proportions.
* **Inside each training fold:**

  * Select $\lambda$ by internal CV (glmnet).
  * Choose the **operating threshold** by **Youden’s J**; apply to the held-out fold.
* **Attribution:** coefficients at $\lambda_{\text{1se}}$; univariate log-odds and class-conditional prevalences reported for top properties.
* **Target scoring:** Jesus of Nazareth (**Q302**) scored by each final model; bootstrap CIs available.

---

## Scenarios (ablations) & stress tests

1. **Main (`all_no_dates`)**
   All eligible properties after guardrails (**`P31`**, all **EIs**, and **`P569`/`P570`** excluded).
2. **Minus basic biography (`minus_bio_basic`)**
   Further remove `P19` (place of birth), `P20` (place of death), `P27` (citizenship), `P106` (occupation), `P1412` (languages).

**Random-subset stress tests:** Monte Carlo models fit on random subsets of size **k = 5/10/20/30**
(≈ **4.72% / 9.43% / 18.87% / 28.30%** of available properties), testing robustness under severe information reduction.

---

## Reproducibility guide

### Quick reproduce (minutes)

Prereqs: R (≥ 4.2), Python (≥ 3.9).

1. **Python env** for harvesting utilities

   ```bash
   python -m venv .venv && . .venv/Scripts/activate   # Windows
   pip install -r requirements.txt
   ```
2. **Run modeling pipeline** (uses processed features if present)

   ```bash
   Rscript src/R/analyze_wikidata_logistic.R
   ```

   Outputs land under `out/` (see next section).

> **Faster dev run:** in `analyze_wikidata_logistic.R`, you may temporarily set `repeats <- 1` (default is 10) to speed up CV during local checks.

### Full reproduce (from WDQS)

1. **Build seed cohorts**
   Run the R code in `src/R/get_wikidata_main.R` to populate `data/qid_selected.csv`.
2. **Harvest entities**

   ```bash
   python src/python/wikidata_dump.py   # fills `data/processed/all_properties_long.csv`
   ```
3. **Run modeling**

   ```bash
   Rscript src/R/analyze_wikidata_logistic.R
   ```

---

## Outputs & where results live

* `out/rl_fold_metrics.csv` — per-fold metrics across repeats and scenarios.
* `out/rl_summary.csv` — means & 95% percentile intervals by metric/scenario.
* `out/feature_space_summary.csv` — counts of QIDs by class and total properties after filters.
* `out/jesus_predictions.csv` — Jesus (Q302) probabilities, bootstrap CIs, decisions.
* `out/jesus_random_probs.csv` — per-rep probabilities for random-subset tests.
* `out/jesus_random_summary.csv` — summary stats by *k* (incl. % ≥ 0.5, median).
* `out/top_variables.csv` — top coefficients at $\lambda_{1se}$ with univariate stats.

**Manuscript tables/figures** are produced from these files (see `docs/manuscript/` for LaTeX and mapping).

---

## Determinism, seeds & snapshots

* Random seeds are set in the scripts (`set.seed(123)`), ensuring stable CV splits and bootstraps.
* WDQS **snapshot date** and endpoint metadata are recorded in `data/raw/` logs.
  Re-running against a different snapshot may change counts and marginally shift metrics/CIs.
* We freeze lists and caches for exact reproduction; full re-harvests are supported and documented.

---

## Limitations & ethical notes

* Wikidata’s typing (`P31`) can mislabel edge cases; property presence reflects **editorial practice** as well as historical reality.
* We mitigate leakage and connectivity bias by excluding `P31`, **External Identifiers**, and **`P569`/`P570`** (used only in the period filter).
* Features are property-presence (binary); we do not (yet) model qualifiers, ranks, or temporal constraints.
* Convergence across **multiple** independent probes (ablations, anchors, random subsets) matters more than any single number.
* While imperfect, Wikidata is currently the most practical **open, versioned, multilingual, typed** platform to support **auditable** historical comparison at scale.
* The same design applies to **events** as well as persons (encode their documentary properties: sources, participants, location, datings, institutional context).

---

## How to cite

If you use this code or results, please cite:

```
W. A. G. et al. (2025). Disproving Carrier: Proving History the Way Historians Do, but With Data.
GitHub repository: https://github.com/wagbr/disproving-carrier
```

A machine-readable **CITATION.cff** is included. If a Zenodo DOI is minted for releases, add it here.

---

## Licenses

* **Code:** MIT (see `LICENSE`).
* **Manuscript text, figures, derived tables:** CC BY 4.0 (see `LICENSE-DATA`).
* **Data provenance:** derived from Wikidata; respect their CC0/CC BY policies as applicable and credit Wikidata contributors.

---

## Contact / replication

We ship **exact queries, scripts, and pinned environments** to enable third parties to reproduce the corpus, features, and estimates end-to-end. Issues and replication reports are welcome via the repository’s **Issues** tab.

---

## Reproducibility checklist (for evaluators)

* [ ] `queries/` present with SPARQL and README
* [ ] `src/R/analyze_wikidata_logistic.R` runs end-to-end on your machine
* [ ] `out/metrics/rl_summary.csv` produced with two scenarios: `all_no_dates`, `minus_bio_basic`
* [ ] `out/predictions/jesus_predictions.csv` includes final and CI estimates for Q302
* [ ] `out/predictions/jesus_random_summary.csv` shows *k* = 5/10/20/30 with % ≥ 0.5
* [ ] `out/attribution/top_variables.csv` lists top positive/negative properties with prevalences
* [ ] README + LICENSE + CITATION present; snapshot date documented in `data/raw/`

---

*Prove history the way historians do—**but with data**, openly and auditably.*
