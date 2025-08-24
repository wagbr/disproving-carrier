
# evaluate_label_strategies.R
# -----------------------------------------------------------------------------
# Implements:
#   - Label strategies: "q5" (requires labels_q5.csv) and "anchors" (anchors_hm.csv)
#   - Fold-wise training to avoid leakage (feature filtering + weights + calibration)
#   - Matching by editorial intensity (n_statements/n_sitelinks or a proxy)
#   - Ablation: remove "biografia básica" properties (P569, P570, P19, P20, P27, P106, P1412)
#   - Repeated 5-fold CV with AUC and Balanced Accuracy
#
# INPUT FILES (place in the same folder or set absolute paths below):
#   - features_long.csv  (columns: qid, property, present [0/1])
#       OR
#     features_wide.csv  (columns: qid, Pxxx..., each 0/1)
#   - metadata.csv       (optional; columns: qid, n_statements, n_sitelinks)
#   - anchors_hm.csv     (for strategy = "anchors"; columns: qid, name, class in {H,M})
#   - labels_q5.csv      (for strategy = "q5"; columns: qid, label in {H,M})
#
# HOW TO RUN (edit the parameters in the "USER PARAMETERS" section, then):
#   Rscript evaluate_label_strategies.R
# -----------------------------------------------------------------------------
setwd("C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
suppressPackageStartupMessages({
  required <- c("dplyr","tidyr","readr","purrr","rsample","yardstick","stringr")
  to_install <- setdiff(required, rownames(installed.packages()))
  if (length(to_install)) install.packages(to_install, repos = "https://cloud.r-project.org")
  lapply(required, library, character.only = TRUE)
})


label_strategy <- "anchors" # Choose label strategy: "anchors" or "q5"

features_long_path <- "data/processed/features_long.csv"
features_wide_path <- "features_wide.csv"
metadata_path      <- "data/metadata.csv"

anchors_path       <- "data/anchors/anchors_hm_complete.csv"
labels_q5_path     <- "data/processed/all_properties_long.csv"

out_metrics_csv     <- "out/results_metrics.csv"
out_fold_metrics_csv<- "out/results_fold_metrics.csv"

set.seed(123)
v_folds  <- 5
repeats  <- 10

min_count <- 5

bio_basic_props <- c("P569","P570","P19","P20","P27","P106","P1412")

load_features <- function(features_long_path, features_wide_path) {
  if (file.exists(features_wide_path)) {
    message("Loading wide features: ", features_wide_path)
    X <- readr::read_csv(features_wide_path, show_col_types = FALSE)
    stopifnot("qid" %in% names(X))
     num_cols <- setdiff(names(X), "qid")
    X[num_cols] <- lapply(X[num_cols], function(v) as.integer(v != 0))
    return(list(X=X, items=X %>% dplyr::select(qid)))
  } else if (file.exists(features_long_path)) {
    message("Loading long features: ", features_long_path)
    L <- readr::read_csv(features_long_path, show_col_types = FALSE)
    stopifnot(all(c("qid","property","present") %in% names(L)))
    X <- L %>%
      dplyr::mutate(present = as.integer(present != 0)) %>%
      tidyr::pivot_wider(names_from = property, values_from = present, values_fill = 0) %>%
      dplyr::group_by(qid) %>%
      dplyr::summarise(dplyr::across(dplyr::everything(), ~max(.x, na.rm=TRUE)), .groups="drop")
    return(list(X=X, items=X %>% dplyr::select(qid)))
  } else {
    stop("Missing both features_wide.csv and features_long.csv. Provide one.")
  }
}

load_edit_metadata <- function(X, metadata_path) {
  if (file.exists(metadata_path)) {
    M <- readr::read_csv(metadata_path, show_col_types = FALSE)
    stopifnot("qid" %in% names(M))
    if (!"n_statements" %in% names(M)) M$n_statements <- NA_integer_
    if (!"n_sitelinks"  %in% names(M)) M$n_sitelinks  <- NA_integer_
    return(M %>% dplyr::select(qid, n_statements, n_sitelinks))
  } else {
    message("No metadata.csv found. Using proxy 'num_props' = number of present properties.")
    num_props <- X %>%
      dplyr::mutate(num_props = rowSums(dplyr::across(-qid, ~ as.integer(.x != 0)))) %>%
      dplyr::select(qid, num_props)
    return(num_props)
  }
}

prepare_labels <- function(strategy, anchors_path, labels_q5_path) {
  if (strategy == "anchors") {
    A <- readr::read_csv2(anchors_path, show_col_types = FALSE)
    stopifnot(all(c("qid","class") %in% names(A)))
    A <- A %>% dplyr::filter(class %in% c("H","M")) %>% dplyr::select(qid, class)
    A <- A %>% dplyr::mutate(label = ifelse(class=="H", 1L, 0L)) %>% dplyr::select(qid, label)
    return(A)
  } else if (strategy == "q5") {
    L <- readr::read_csv2(labels_q5_path, show_col_types = FALSE)
    stopifnot(all(c("qid","label") %in% names(L)))
    if (is.character(L$label[1])) {
      L <- L %>% dplyr::mutate(label = ifelse(label %in% c("H","h","historic","historical","Q5"), 1L, 0L))
    } else {
      L <- L %>% dplyr::mutate(label = as.integer(label != 0))
    }
    return(L %>% dplyr::select(qid, label))
  } else {
    stop("Unknown label_strategy: ", strategy)
  }
}

compute_weights <- function(X_train, y_train, min_count=5, alpha=1.0) {
  Pnames <- setdiff(names(X_train), "qid")
  counts <- colSums(as.matrix(X_train[, Pnames, drop=FALSE]) != 0)
  keep <- names(counts)[counts >= min_count]
  if (!length(keep)) stop("All properties filtered out by min_count = ", min_count)
  Xtr <- X_train[, c("qid", keep), drop=FALSE]

  y <- as.integer(y_train != 0)
  n1 <- sum(y == 1)
  n0 <- sum(y == 0)
  if (n1 == 0 || n0 == 0) stop("Training fold has only one class; adjust folds or labels.")

  W <- purrr::map_dfr(keep, function(p) {
    x <- as.integer(Xtr[[p]] != 0)
    p1 <- (sum(x[y==1]) + alpha) / (n1 + 2*alpha)
    p0 <- (sum(x[y==0]) + alpha) / (n0 + 2*alpha)
    w  <- log(p1/(1-p1)) - log(p0/(1-p0))
    dplyr::tibble(property = p, weight = w)
  })
  return(W)
}

score_items <- function(X, W) {
  P <- intersect(W$property, setdiff(names(X), "qid"))
  if (!length(P)) stop("No overlap between weight properties and X columns.")
  Wi <- W$weight; names(Wi) <- W$property
  Xsub <- X[, c("qid", P), drop=FALSE]
  S <- Xsub %>%
    dplyr::mutate(score = as.numeric(as.matrix(dplyr::select(., -qid)) %*% Wi[P])) %>%
    dplyr::select(qid, score)
  return(S)
}

calibrate_prob <- function(scores_train, y_train, scores_test) {
  df_train <- dplyr::tibble(y = as.integer(y_train != 0), s = scores_train)
  fit <- tryCatch(glm(y ~ s, data=df_train, family=binomial()), error=function(e) NULL)
  if (is.null(fit) || !fit$converged) {
    rng <- range(scores_train)
    if (diff(rng) == 0) {
      p_train <- rep(mean(df_train$y), length(scores_train))
      p_test  <- rep(mean(df_train$y), length(scores_test))
    } else {
      p_train <- (scores_train - rng[1]) / diff(rng)
      p_test  <- (scores_test  - rng[1]) / diff(rng)
    }
    return(list(p_train=p_train, p_test=p_test, method="minmax"))
  } else {
    p_train <- as.numeric(predict(fit, newdata=data.frame(s=scores_train), type="response"))
    p_test  <- as.numeric(predict(fit, newdata=data.frame(s=scores_test ), type="response"))
    return(list(p_train=p_train, p_test=p_test, method="platt_glm"))
  }
}

eval_fold <- function(y_train, p_train, y_test, p_test) {
  df_tr <- dplyr::tibble(y = factor(ifelse(y_train!=0,"H","M"), levels=c("M","H")),
                         p = p_train)
  cand <- sort(unique(p_train))
  if (length(cand) > 200) cand <- cand[seq(1, length(cand), length.out=200)]
  best <- NULL; best_J <- -Inf
  for (t in cand) {
    pred <- factor(ifelse(p_train >= t,"H","M"), levels=c("M","H"))
    sens <- yardstick::sens_vec(df_tr$y, pred, estimator="binary")
    spec <- yardstick::spec_vec(df_tr$y, pred, estimator="binary")
    J <- sens + spec - 1
    if (!is.na(J) && J > best_J) { best_J <- J; best <- t }
  }
  thr <- ifelse(is.null(best), 0.5, best)

  df_te <- dplyr::tibble(y = factor(ifelse(y_test!=0,"H","M"), levels=c("M","H")),
                         p = p_test,
                         pred = factor(ifelse(p_test >= thr,"H","M"), levels=c("M","H")))
  auc <- tryCatch(yardstick::roc_auc_vec(df_te$y, df_te$p, estimator="binary"), error=function(e) NA_real_)
  ba  <- tryCatch({
    sens <- yardstick::sens_vec(df_te$y, df_te$pred, estimator="binary")
    spec <- yardstick::spec_vec(df_te$y, df_te$pred, estimator="binary")
    (sens + spec)/2
  }, error=function(e) NA_real_)
  dplyr::tibble(auc=auc, bal_accuracy=ba, threshold=thr)
}

match_by_intensity <- function(df_labels, meta, n_bins=5) {
  Z <- df_labels %>% dplyr::left_join(meta, by="qid")
  if ("num_props" %in% names(Z)) {
    Z$edit_intensity <- Z$num_props
  } else {
    ns <- ifelse(is.na(Z$n_statements), 0, Z$n_statements)
    nl <- ifelse(is.na(Z$n_sitelinks), 0, Z$n_sitelinks)
    Z$edit_intensity <- ns + nl
  }
  qs <- stats::quantile(Z$edit_intensity, probs = seq(0,1,length.out=n_bins+1), na.rm=TRUE)
  Z$bin <- cut(Z$edit_intensity, breaks = unique(qs), include.lowest=TRUE, labels = FALSE)
  Z <- Z %>% dplyr::filter(!is.na(bin))

  out <- Z %>% dplyr::group_by(bin) %>% dplyr::group_modify(function(d, key) {
    Hs <- d %>% dplyr::filter(label==1)
    Ms <- d %>% dplyr::filter(label==0)
    n  <- min(nrow(Hs), nrow(Ms))
    if (n == 0) return(dplyr::tibble())
    Hs <- Hs %>% dplyr::slice_sample(n=n)
    Ms <- Ms %>% dplyr::slice_sample(n=n)
    dplyr::bind_rows(Hs, Ms)
  }) %>% dplyr::ungroup() %>% dplyr::select(qid, label)
  if (!nrow(out)) stop("Matching produced empty set. Try fewer bins or provide metadata.")
  return(out)
}

run_cv <- function(X, y_labels, ablation_props = character(0), meta=NULL) {
  D <- X %>% dplyr::inner_join(y_labels, by="qid")
  if (length(ablation_props)) {
    keep <- setdiff(names(D), c("qid", ablation_props))
    D <- D[, c("qid", keep), drop=FALSE]
  }

  if (!is.null(meta)) {
    matched <- match_by_intensity(y_labels, meta)
    D <- D %>% dplyr::semi_join(matched, by=c("qid","label"))
  }

  folds <- rsample::vfold_cv(D, v = v_folds, repeats = repeats, strata = label)

  fold_metrics <- purrr::map_dfr(seq_along(folds$splits), function(i) {
    s <- folds$splits[[i]]
    tr <- rsample::analysis(s)
    te <- rsample::assessment(s)
    W <- compute_weights(tr, tr$label, min_count = min_count, alpha = 1.0)
    sc_tr <- score_items(tr, W) %>% dplyr::left_join(tr %>% dplyr::select(qid, label), by="qid")
    sc_te <- score_items(te, W) %>% dplyr::left_join(te %>% dplyr::select(qid, label), by="qid")
    cal <- calibrate_prob(sc_tr$score, sc_tr$label, sc_te$score)
    met <- eval_fold(sc_tr$label, cal$p_train, sc_te$label, cal$p_test)
    met$fold_id <- i
    met$calibration <- cal$method
    met
  })

  fold_metrics <- fold_metrics %>% dplyr::mutate(ablation = ifelse(length(ablation_props),"minus_bio_basic","none"))
  summary <- fold_metrics %>%
    dplyr::summarise(
      auc_mean = mean(auc, na.rm=TRUE),
      auc_sd   = sd(auc, na.rm=TRUE),
      ba_mean  = mean(bal_accuracy, na.rm=TRUE),
      ba_sd    = sd(bal_accuracy, na.rm=TRUE),
      .by = ablation
    )
  list(fold_metrics=fold_metrics, summary=summary)
}

# ---------------------- MAIN --------------------------------------------------
feat <- load_features(features_long_path, features_wide_path)
X    <- feat$X
items<- feat$items

meta <- load_edit_metadata(X, metadata_path)

labels <- prepare_labels(label_strategy, anchors_path, labels_q5_path)

X <- X %>% dplyr::semi_join(labels, by="qid")

if (nrow(X) < 50) warning("Very small labeled set after intersection. Consider adding more anchors.")

bio_here <- intersect(bio_basic_props, setdiff(names(X), "qid"))

res_none <- run_cv(X, labels, ablation_props = character(0), meta=meta)
res_bio  <- run_cv(X, labels, ablation_props = bio_here,          meta=meta)

all_summary <- dplyr::bind_rows(res_none$summary, res_bio$summary) %>%
  dplyr::mutate(strategy = label_strategy, .before = 1)

all_folds <- dplyr::bind_rows(res_none$fold_metrics, res_bio$fold_metrics) %>%
  dplyr::mutate(strategy = label_strategy, .before = 1)

readr::write_csv(all_summary, out_metrics_csv)
readr::write_csv(all_folds,  out_fold_metrics_csv)

message("Done. Wrote: ", out_metrics_csv, " and ", out_fold_metrics_csv)
