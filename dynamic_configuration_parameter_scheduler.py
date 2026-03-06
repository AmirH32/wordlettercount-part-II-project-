#!/usr/bin/env python3
"""
Parameters:
1. spark.executor.instances   [2, 10]
2. spark.executor.memory      512m | 1g | 2g | 4g  (encoded in GB)
3. spark.default.parallelism  [4, 40]

Run budget: 15 total (6 initial exploration + 9 BO-guided exploitation).

Usage:
    python3 dynamic_configuration_parameter_scheduler.py <input_file>
"""

import sys
import os
import subprocess
import time
import random
import json
import csv
import numpy as np
from datetime import datetime
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern, ConstantKernel
from scipy.stats import norm

# constants
SPARK_SUBMIT     = "./spark-3.5.4-bin-hadoop3/bin/spark-submit"
MASTER_URL       = "k8s://https://128.232.80.18:6443"
NAMESPACE        = "cc-group8"
SERVICE_ACCOUNT  = "spark-cc-group8"
IMAGE            = "andylamp/spark:v3.5.4-amd64"
PVC_NAME         = "nfs-cc-group8"
MOUNT_PATH       = "/test-data"
APP_JAR          = "local:///test-data/WordLetterCount-1.0.jar"
APP_CLASS        = "com.ccgroup8.spark.WordLetterCount"
DRIVER_MEM       = "1g"
MAX_EXECUTORS    = 10 # hard cluster constraint

# ================================================================================================
# Parameters which we will search

PARAM_EXECUTOR_INSTANCES = {
    "spark_conf_key": "spark.executor.instances",
    "type":           "int",
    "low":            2,
    "high":           MAX_EXECUTORS,
}

PARAM_EXECUTOR_MEMORY = {
    "spark_conf_key": "spark.executor.memory",
    "type":           "cat", # Index into the choice list flag
    # stored internally as GB floats
    "choices_gb":     [0.5, 1.0, 2.0, 4.0],
    "choices_str":    ["512m", "1g", "2g", "4g"],
    "low":            0,   # index bounds for GP
    "high":           3,
}

PARAM_PARALLELISM = {
    "spark_conf_key": "spark.default.parallelism",
    "type":           "int",
    "low":            4,
    "high":           40,
}

# Budget
N_INITIAL = 6    # Latin Hypercube Sampling exploration phase
N_BO      = 9    # Bayesian-Optimisation exploitation phase
N_TOTAL   = N_INITIAL + N_BO   # = 15, safely within 10-20 limit

# Results log
results_log = []   # list of {"params": {...}, "time": float}
# ================================================================================================

# Encoding helpers
def encode(instances, mem_index, parallelism):
    return np.array([float(instances), float(mem_index), float(parallelism)])


def decode(x):
    instances   = int(
        np.clip(round(x[0]),
            PARAM_EXECUTOR_INSTANCES["low"], PARAM_EXECUTOR_INSTANCES["high"])
    )
    mem_index   = int(
        np.clip(round(x[1]),
            PARAM_EXECUTOR_MEMORY["low"], PARAM_EXECUTOR_MEMORY["high"])
    )
    parallelism = int(
        np.clip(round(x[2]),
            PARAM_PARALLELISM["low"], PARAM_PARALLELISM["high"])
    )
    return instances, mem_index, parallelism


# ========================================================================
# Exploration
# Formally named : Latin Hypercube Sampling
#
# Each parameter dimension is divided into N equal intervals (where N = number
# of samples). One point is drawn uniformly at random from within each interval,
# guaranteeing full coverage of each dimension's range. The per-dimension samples
# are then randomly paired across dimensions to form complete configurations.
#
# This avoids the clustering that can occur with pure random sampling, ensuring
# the initial runs are spread across the parameter space before the GP takes over.

def latin_hypercube_samples(n):
    """
    Generate `n` parameter configurations using Latin Hypercube Sampling.
    Returns a list of (instances, mem_index, parallelism) tuples.
    """
    # Build samples for each dimension in [0, 1]
    def lhs_1d(n):
        cuts = np.linspace(0, 1, n + 1)
        points = [random.uniform(cuts[i], cuts[i + 1]) for i in range(n)]
        random.shuffle(points)
        return points

    inst_samples = lhs_1d(n)
    mem_samples  = lhs_1d(n)
    par_samples  = lhs_1d(n)

    configs = []
    for i_f, m_f, p_f in zip(inst_samples, mem_samples, par_samples):
        instances   = round(PARAM_EXECUTOR_INSTANCES["low"] +
                            i_f * (PARAM_EXECUTOR_INSTANCES["high"] -
                                   PARAM_EXECUTOR_INSTANCES["low"]))
        instances   = int(np.clip(instances, PARAM_EXECUTOR_INSTANCES["low"],
                                             PARAM_EXECUTOR_INSTANCES["high"]))
        mem_index   = int(np.clip(round(m_f * (len(PARAM_EXECUTOR_MEMORY["choices_str"]) - 1)),
                                  0, len(PARAM_EXECUTOR_MEMORY["choices_str"]) - 1))
        parallelism = round(PARAM_PARALLELISM["low"] +
                            p_f * (PARAM_PARALLELISM["high"] -
                                   PARAM_PARALLELISM["low"]))
        parallelism = int(np.clip(parallelism, PARAM_PARALLELISM["low"],
                                               PARAM_PARALLELISM["high"]))
        configs.append((instances, mem_index, parallelism))

    return configs


# ========================================================================
# Gaussian Process surrogate model
#
# Running the cluster is expensive, so we use a 'surrogate', a cheap probabilistic
# model that approximates the execution-time from a small number of runs.
#
# The GP is fitted to observed (config, execution_time) data points. At any unvisited
# config it predicts a mean and a variance (uncertainty)
#
# Expected Improvement (EI) is then computed from those predictions. It scores
# each candidate config by how likely it is to beat the current best time. We run
# the config with the highest EI score, observe the real time, and add it to our
# data before refitting the GP.
#
# Each new observation updates our posterior belief about the surface.
# This is just Bayesianism covered in the Data Science course.

def build_gp():
    """Construct the GP regressor"""
    kernel = ConstantKernel(1.0, (1e-3, 1e3)) * Matern(
        length_scale=1.0, length_scale_bounds=(1e-2, 1e2), nu=2.5
    )
    return GaussianProcessRegressor(
        kernel=kernel,
        alpha=1e-4,           # small noise term for numerical stability
        normalize_y=True,
        n_restarts_optimizer=5,
    )


def expected_improvement(candidates, gp, y_best, xi=0.01):
    """
    Compute Expected Improvement

    The GP provides a bell curve (mu, sigma) for each candidate x
    If mu is lower than current best we like that.
    If sigma is high, the potential for improvement is there

    xi means that mu which is close to the best is more likely to be chosen if XI is high,
    but because we are low on budget, we keep it low.

    EI(x) = E[max(0, y_best - f(x))]
           = (y_best - mu(x) - xi) · CDF(Z) + sigma(x) · PDF(Z)
    where Z = (y_best - mu(x) - xi) / sigma(x)
    """
    mu, sigma = gp.predict(candidates, return_std=True)
    sigma = sigma.reshape(-1, 1)
    mu    = mu.reshape(-1, 1)

    z  = (y_best - mu - xi) / (sigma + 1e-9)  # avoid division by zero
    ei = (y_best - mu - xi) * norm.cdf(z) + sigma * norm.pdf(z)
    ei[sigma < 1e-10] = 0.0  # no uncertainty => no improvement to expect
    return ei.flatten()


def next_candidate(gp, y_best, n_restarts=200):
    """
    Draw `n_restarts` random candidates, evaluate EI for all of them,
    and return the best.
    """
    # Random candidates across the search space
    inst_cands = np.random.uniform(PARAM_EXECUTOR_INSTANCES["low"],
                                   PARAM_EXECUTOR_INSTANCES["high"], n_restarts)
    mem_cands  = np.random.uniform(PARAM_EXECUTOR_MEMORY["low"],
                                   PARAM_EXECUTOR_MEMORY["high"],    n_restarts)
    par_cands  = np.random.uniform(PARAM_PARALLELISM["low"],
                                   PARAM_PARALLELISM["high"],        n_restarts)
    X_cand = np.column_stack([inst_cands, mem_cands, par_cands])

    ei_vals = expected_improvement(X_cand, gp, y_best)
    best_x  = X_cand[np.argmax(ei_vals)]
    return decode(best_x)


# ==========================================
# Spark runner
def run_spark(input_file, instances, mem_str, parallelism):
    """
    Execute WordLetterCount with the given parameter set and return wall-clock
    execution time in seconds. Returns None on error.
    """
    cmd = [
        SPARK_SUBMIT,
        "--master",      MASTER_URL,
        "--deploy-mode", "cluster",
        "--name",        "word-letter-counter",
        "--class",       APP_CLASS,
        "--conf", f"spark.executor.instances={instances}",
        "--conf", f"spark.executor.memory={mem_str}",
        "--conf", f"spark.default.parallelism={parallelism}",
        "--conf", f"spark.kubernetes.namespace={NAMESPACE}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={SERVICE_ACCOUNT}",
        "--conf", f"spark.kubernetes.container.image={IMAGE}",
        "--conf", f"spark.kubernetes.driver.volumes.persistentVolumeClaim.{PVC_NAME}.mount.path={MOUNT_PATH}",
        "--conf", f"spark.kubernetes.driver.volumes.persistentVolumeClaim.{PVC_NAME}.mount.readOnly=false",
        "--conf", f"spark.kubernetes.driver.volumes.persistentVolumeClaim.{PVC_NAME}.options.claimName={PVC_NAME}",
        "--conf", f"spark.kubernetes.executor.volumes.persistentVolumeClaim.{PVC_NAME}.mount.path={MOUNT_PATH}",
        "--conf", f"spark.kubernetes.executor.volumes.persistentVolumeClaim.{PVC_NAME}.mount.readOnly=false",
        "--conf", f"spark.kubernetes.executor.volumes.persistentVolumeClaim.{PVC_NAME}.options.claimName={PVC_NAME}",
        APP_JAR,
        "-i", input_file,
    ]

    print("  CMD: " + " ".join(cmd))
    t_start = time.time()
    result  = subprocess.run(cmd, capture_output=True)
    elapsed = time.time() - t_start

    if result.returncode != 0:
        print(f"  [WARN] spark-submit exited with code {result.returncode}")
        print(result.stderr.decode()[-500:])
        return None

    return elapsed


# ==========================================
# Output helpers

def write_dynamic_csv(timestamp, input_file, best):
    """
    Write all run results to experiments/<timestamp>/dynamic.csv
    as required by the assignment spec.
    """
    exp_dir = os.path.join("experiments", timestamp)
    os.makedirs(exp_dir, exist_ok=True)

    csv_path = os.path.join(exp_dir, "dynamic.csv")
    header = ["run", "phase", "spark.executor.instances", "spark.executor.memory",
              "spark.default.parallelism", "execution_time_s", "is_best"]

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["# Dynamic Parameter Configuration Scheduler"])
        writer.writerow([f"# Input file   : {input_file}"])
        writer.writerow([f"# Timestamp    : {timestamp}"])
        writer.writerow([f"# Total runs   : {len(results_log)}  ({N_INITIAL} LHS + {N_BO} BO)"])
        writer.writerow([f"# Best config  : instances={best['instances']}  "
                         f"memory={best['memory']}  parallelism={best['parallelism']}  "
                         f"time={best['time_s']}s"])
        writer.writerow([])
        writer.writerow(header)
        for r in results_log:
            is_best = (r["run"] == best["run"])
            writer.writerow([
                r["run"], r["phase"], r["instances"], r["memory"],
                r["parallelism"], r["time_s"], "YES" if is_best else ""
            ])

    print(f"Results written to {csv_path}")
    return csv_path


# ======================================
# Main scheduler loop
def main():
    if len(sys.argv) != 2:
        print("Usage: python3 dynamic_configuration_parameter_scheduler.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    timestamp  = datetime.now().strftime("%Y%m%dT%H%M%S")  # ISO-8601 format

    print(f"\n{'='*65}")
    print("Dynamic Parameter Configuration Scheduler")
    print(f"Input file : {input_file}")
    print(f"Timestamp  : {timestamp}")
    print(f"Budget     : {N_TOTAL} runs  ({N_INITIAL} LHS + {N_BO} Bayesian BO)")
    print(f"Parameters : spark.executor.instances | spark.executor.memory | spark.default.parallelism")
    print(f"{'='*65}\n")

    X_observed = []  # list of encoded np.array vectors
    y_observed = []  # list of execution times (s)
    run_count  = 0

    # =================
    # Exploration : LHS
    print(f"Exploration  ({N_INITIAL} LHS samples)")
    initial_configs = latin_hypercube_samples(N_INITIAL)

    for instances, mem_index, parallelism in initial_configs:
        run_count += 1
        mem_str = PARAM_EXECUTOR_MEMORY["choices_str"][mem_index]
        print(f"\n[Run {run_count}/{N_TOTAL}]  instances={instances}  "
              f"memory={mem_str}  parallelism={parallelism}")

        elapsed = run_spark(input_file, instances, mem_str, parallelism)
        if elapsed is None:
            print("  -> Run failed, skipping.")
            run_count -= 1
            continue

        print(f"  -> Execution time: {elapsed:.2f} s")
        X_observed.append(encode(instances, mem_index, parallelism))
        y_observed.append(elapsed)
        results_log.append({
            "run": run_count, "phase": "LHS",
            "instances": instances, "memory": mem_str,
            "parallelism": parallelism, "time_s": round(elapsed, 3)
        })

    # =====================
    # Bayesian Optimisation
    print(f"\n Bayesian Optimisation  ({N_BO} runs)")

    gp = build_gp()

    for bo_iter in range(N_BO):
        run_count += 1

        X_arr = np.array(X_observed)
        y_arr = np.array(y_observed)
        gp.fit(X_arr, y_arr)

        y_best   = np.min(y_arr)
        best_idx = np.argmin(y_arr)
        print(f"\n  GP fitted on {len(y_arr)} points.  Current best: "
              f"{y_best:.2f} s  @ run {results_log[best_idx]['run']}")

        instances, mem_index, parallelism = next_candidate(gp, y_best)
        mem_str = PARAM_EXECUTOR_MEMORY["choices_str"][mem_index]

        print(f"\n[Run {run_count}/{N_TOTAL}]  instances={instances}  "
              f"memory={mem_str}  parallelism={parallelism}  [EI-selected]")

        elapsed = run_spark(input_file, instances, mem_str, parallelism)
        if elapsed is None:
            print("  -> Run failed, skipping.")
            run_count -= 1
            continue

        print(f"  -> Execution time: {elapsed:.2f} s")
        X_observed.append(encode(instances, mem_index, parallelism))
        y_observed.append(elapsed)
        results_log.append({
            "run": run_count, "phase": "BO",
            "instances": instances, "memory": mem_str,
            "parallelism": parallelism, "time_s": round(elapsed, 3)
        })

    # ============
    # Final report
    print(f"\n{'='*65}")
    print("All runs complete. Summary:")
    print(f"{'='*65}")
    print(f"{'Run':<6} {'Phase':<6} {'Instances':<12} {'Memory':<10} "
          f"{'Parallelism':<14} {'Time (s)':<10}")
    print("-" * 58)
    for r in results_log:
        print(f"{r['run']:<6} {r['phase']:<6} {r['instances']:<12} "
              f"{r['memory']:<10} {r['parallelism']:<14} {r['time_s']:<10}")

    best = min(results_log, key=lambda r: r["time_s"])
    print(f"\n★  Best configuration found:")
    print(f"     spark.executor.instances   = {best['instances']}")
    print(f"     spark.executor.memory      = {best['memory']}")
    print(f"     spark.default.parallelism  = {best['parallelism']}")
    print(f"     Execution time             = {best['time_s']} s  (run {best['run']})")
    print(f"{'='*65}")

    # Write experiments/<timestamp>/dynamic.csv
    write_dynamic_csv(timestamp, input_file, best)


if __name__ == "__main__":
    main()