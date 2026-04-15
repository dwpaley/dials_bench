#!/usr/bin/env python3
"""
run_bench.py - DIALS benchmark runner with branch comparison.

Usage:
    run_bench.py [--dials "branch(es)"] [--cctbx_project "branch(es)"]
                 [--dxtbx "branch(es)"] [--nproc N]

At least one repo argument is required. Each branch spec is a quoted string
with one branch ("feat1") or multiple branches space-separated ("dep1 dep2 feat3").
"""

import argparse
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORK_DIR = Path(__file__).parent.resolve()
DIALS_DIR = WORK_DIR / "dials"
DIALS_ENV = DIALS_DIR / "dials"          # source this to activate DIALS
MODULES_DIR = DIALS_DIR / "modules"

REPO_NAMES = ("dials", "cctbx_project", "dxtbx")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def eprint(*args, **kwargs):
    """Print to stderr."""
    print(*args, file=sys.stderr, **kwargs)


def run_cmd(cmd, cwd=None, timeout=None, capture=False):
    """
    Run a shell command.  Raises SystemExit on non-zero return code.

    Args:
        cmd: command string (passed to bash with shell=True).
        cwd: working directory (Path or str); defaults to WORK_DIR.
        timeout: seconds (None = no limit).
        capture: if True, return stdout as str; otherwise stream to terminal.
    Returns:
        stdout string when capture=True, else None.
    """
    if cwd is None:
        cwd = WORK_DIR
    kwargs = dict(
        shell=True,
        executable="/bin/bash",
        cwd=str(cwd),
    )
    if capture:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    result = subprocess.run(cmd, timeout=timeout, **kwargs)
    if result.returncode != 0:
        if capture:
            sys.exit(
                f"ERROR: command failed (rc={result.returncode}):\n  {cmd}\n"
                f"{result.stderr.strip()}"
            )
        else:
            sys.exit(f"ERROR: command failed (rc={result.returncode}):\n  {cmd}")
    return result.stdout if capture else None


def git(repo_path, *args, capture=True):
    """Run a git command inside repo_path.  Returns stdout string."""
    cmd = "git -C " + str(repo_path) + " " + " ".join(args)
    return run_cmd(cmd, capture=capture)


def dials_cmd(command, cwd=None):
    """Run a single DIALS command after sourcing the DIALS environment."""
    full = f"source {DIALS_ENV} && {command}"
    return run_cmd(full, cwd=cwd, capture=False)


# ---------------------------------------------------------------------------
# Prerequisite check
# ---------------------------------------------------------------------------


def check_prerequisites():
    """Abort early if DIALS is not installed."""
    if not DIALS_ENV.exists():
        sys.exit(
            "ERROR: DIALS environment not found. Run setup_bench.py first.\n"
            f"  Expected: {DIALS_ENV}"
        )
    if not MODULES_DIR.is_dir():
        sys.exit(
            f"ERROR: DIALS modules directory not found: {MODULES_DIR}\n"
            "Run setup_bench.py first."
        )


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------


def ensure_main(repo_path, main_name='main'):
    """Checkout main branch and pull latest."""
    repo_path = Path(repo_path)
    eprint(f"  [git] {repo_path.name}: checkout main")
    git(repo_path, "checkout", main_name, capture=False)
    git(repo_path, "pull", "--ff-only", capture=False)


def checkout_branch(repo_path, branch):
    """Checkout a single branch in repo_path."""
    repo_path = Path(repo_path)
    eprint(f"  [git] {repo_path.name}: checkout {branch}")
    git(repo_path, "checkout", branch, capture=False)


def merge_branches_ff(repo_path, branches, temp_branch):
    """
    Create temp_branch from main and merge each branch with --ff-only.

    Args:
        repo_path: Path to the repository.
        branches: list of branch names to merge in order.
        temp_branch: name for the temporary branch to create.
    """
    repo_path = Path(repo_path)
    eprint(f"  [git] {repo_path.name}: creating temp branch {temp_branch!r}")
    git(repo_path, "checkout", "-b", temp_branch, capture=False)
    for branch in branches:
        eprint(f"  [git] {repo_path.name}: merging {branch} (--ff-only)")
        result = subprocess.run(
            f"git -C {repo_path} merge --ff-only {branch}",
            shell=True,
            executable="/bin/bash",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            sys.exit(
                f"ERROR: --ff-only merge of {branch!r} into {temp_branch!r} failed "
                f"in {repo_path.name}.\n{result.stderr.strip()}\n"
                "Tip: rebase the branch onto main before running benchmarks."
            )


def cleanup_temp_branch(repo_path, temp_branch):
    """Delete temp_branch after checking out main."""
    repo_path = Path(repo_path)
    eprint(f"  [git] {repo_path.name}: cleaning up temp branch {temp_branch!r}")
    try:
        git(repo_path, "checkout", "main", capture=False)
        git(repo_path, "branch", "-D", temp_branch, capture=False)
    except SystemExit as exc:
        eprint(f"WARNING: cleanup failed for {repo_path.name}: {exc}")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def rebuild_dials():
    """Rebuild DIALS using bootstrap.py (sources DIALS env first for Python)."""
    eprint("  [build] Rebuilding DIALS (this may take several minutes)...")
    cmd = f"source {DIALS_ENV} && python bootstrap.py --libtbx build"
    run_cmd(cmd, cwd=DIALS_DIR, timeout=1800, capture=False)
    eprint("  [build] Done.")


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------


def _parse_time_taken(log_text, key="Time Taken:"):
    """Extract a float from a log line like 'Time Taken: 12.34'."""
    for line in reversed(log_text.splitlines()):
        if key in line:
            parts = line.split()
            try:
                return float(parts[-1])
            except (ValueError, IndexError):
                pass
    return None


def run_benchmark(nproc, image_range):
    """
    Run the full DIALS pipeline for the given image range and nproc.

    Args:
        nproc: number of processors for find_spots and integrate.
        image_range: tuple (start, end) e.g. (1, 360) or (1, 1800).

    Returns:
        dict with keys: import, find_spots, index, refine, integrate, total.
        Each value is wall-clock seconds (float).
    """
    start_img, end_img = image_range
    template = f"data/test_0####.cbf"
    timings = {}
    pipeline_start = time.perf_counter()

    # Step 1: import
    t0 = time.perf_counter()
    dials_cmd(
        f"dials.import template={template} image_range={start_img},{end_img}",
        cwd=WORK_DIR,
    )
    timings["import"] = time.perf_counter() - t0

    # Step 2: find_spots
    t0 = time.perf_counter()
    result = subprocess.run(
        f"source {DIALS_ENV} && dials.find_spots imported.expt nproc={nproc}",
        shell=True,
        executable="/bin/bash",
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
    )
    wall = time.perf_counter() - t0
    if result.returncode != 0:
        sys.exit(
            f"ERROR: dials.find_spots failed (rc={result.returncode}):\n"
            f"{result.stderr.strip()}"
        )
    log_time = _parse_time_taken(result.stdout + result.stderr, "Time Taken:")
    timings["find_spots"] = log_time if log_time is not None else wall

    # Step 3: index
    t0 = time.perf_counter()
    dials_cmd(
        "dials.index imported.expt strong.refl space_group=P43212",
        cwd=WORK_DIR,
    )
    timings["index"] = time.perf_counter() - t0

    # Step 4: refine
    t0 = time.perf_counter()
    result = subprocess.run(
        f"source {DIALS_ENV} && dials.refine indexed.expt indexed.refl",
        shell=True,
        executable="/bin/bash",
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
    )
    wall = time.perf_counter() - t0
    if result.returncode != 0:
        sys.exit(
            f"ERROR: dials.refine failed (rc={result.returncode}):\n"
            f"{result.stderr.strip()}"
        )
    log_time = _parse_time_taken(result.stdout + result.stderr, "Total time taken:")
    timings["refine"] = log_time if log_time is not None else wall

    # Step 5: integrate
    t0 = time.perf_counter()
    result = subprocess.run(
        f"source {DIALS_ENV} && dials.integrate refined.expt refined.refl nproc={nproc}",
        shell=True,
        executable="/bin/bash",
        cwd=str(WORK_DIR),
        capture_output=True,
        text=True,
    )
    wall = time.perf_counter() - t0
    if result.returncode != 0:
        sys.exit(
            f"ERROR: dials.integrate failed (rc={result.returncode}):\n"
            f"{result.stderr.strip()}"
        )
    log_time = _parse_time_taken(result.stdout + result.stderr, "Total time taken:")
    timings["integrate"] = log_time if log_time is not None else wall

    timings["total"] = time.perf_counter() - pipeline_start
    return timings


def run_full_benchmark(nproc):
    """
    Run both benchmark tests (single-core 360 images, multi-core 1800 images).

    Returns:
        dict with keys "single_core" and "multi_core", each a timings dict.
    """
    eprint("  [bench] Test 1: single-core, 360 images")
    single = run_benchmark(nproc=1, image_range=(1, 360))
    eprint("  [bench] Test 2: multi-core, 1800 images")
    multi = run_benchmark(nproc=nproc, image_range=(1, 1800))
    return {"single_core": single, "multi_core": multi}


# ---------------------------------------------------------------------------
# Comparison / output
# ---------------------------------------------------------------------------


def compute_comparison(baseline, feature):
    """
    Compute per-step deltas between baseline and feature results.

    Returns:
        dict mirroring the structure of baseline/feature with comparison entries.
    """
    comparison = {}
    for test_key in ("single_core", "multi_core"):
        comparison[test_key] = {}
        for step, base_val in baseline[test_key].items():
            feat_val = feature[test_key].get(step)
            if base_val is not None and feat_val is not None and base_val != 0:
                delta_pct = (feat_val - base_val) / base_val * 100.0
            else:
                delta_pct = None
            comparison[test_key][step] = {
                "baseline": base_val,
                "feature": feat_val,
                "delta_pct": round(delta_pct, 1) if delta_pct is not None else None,
            }
    return comparison


def print_human_summary(comparison):
    """Print a human-readable timing comparison table to stderr."""
    eprint("\n=== Benchmark Results ===")
    for test_key in ("single_core", "multi_core"):
        label = "Single-core (360 img)" if test_key == "single_core" else "Multi-core (1800 img)"
        eprint(f"\n{label}:")
        eprint(f"  {'Step':<14} {'Baseline':>10} {'Feature':>10} {'Delta':>10}")
        eprint(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10}")
        for step, vals in comparison[test_key].items():
            base_s = f"{vals['baseline']:.2f}s" if vals["baseline"] is not None else "n/a"
            feat_s = f"{vals['feature']:.2f}s" if vals["feature"] is not None else "n/a"
            delta_s = (
                f"{vals['delta_pct']:+.1f}%"
                if vals["delta_pct"] is not None
                else "n/a"
            )
            eprint(f"  {step:<14} {base_s:>10} {feat_s:>10} {delta_s:>10}")


# ---------------------------------------------------------------------------
# Branch setup helpers
# ---------------------------------------------------------------------------


def setup_repos_baseline(repo_specs):
    """Ensure all repos are on main before the baseline run."""
    eprint("[setup] Checking repos onto main...")
    for repo_name in repo_specs:
        repo_path = MODULES_DIR / repo_name
        main_name = 'master' if repo_name == 'cctbx_project' else 'main'
        ensure_main(repo_path, main_name)


def apply_branches_case_a(repo_specs):
    """
    Case A: single branch per repo.  Checkout the requested branch.

    Args:
        repo_specs: dict {repo_name: [branch_name]}
    """
    eprint("[setup] Applying feature branches...")
    for repo_name, branches in repo_specs.items():
        repo_path = MODULES_DIR / repo_name
        checkout_branch(repo_path, branches[0])


def apply_branches_case_b_pre(repo_specs, temp_branches):
    """
    Case B pre-step: merge all-but-last branches into temp branch per repo.

    Args:
        repo_specs: dict {repo_name: [branch1, branch2, ..., last]}
        temp_branches: dict {repo_name: temp_branch_name} (populated here)
    """
    eprint("[setup] Merging dependency branches (pre-measurement)...")
    for repo_name, branches in repo_specs.items():
        repo_path = MODULES_DIR / repo_name
        dep_branches = branches[:-1]
        temp_branch = temp_branches[repo_name]
        merge_branches_ff(repo_path, dep_branches, temp_branch)


def apply_branches_case_b_post(repo_specs):
    """
    Case B post-step: merge the final (feature) branch into the current temp branch.

    Args:
        repo_specs: dict {repo_name: [branch1, branch2, ..., last]}
    """
    eprint("[setup] Merging final feature branch (post-measurement)...")
    for repo_name, branches in repo_specs.items():
        repo_path = MODULES_DIR / repo_name
        final_branch = branches[-1]
        eprint(f"  [git] {repo_name}: merging {final_branch} (--ff-only)")
        result = subprocess.run(
            f"git -C {repo_path} merge --ff-only {final_branch}",
            shell=True,
            executable="/bin/bash",
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            sys.exit(
                f"ERROR: --ff-only merge of {final_branch!r} failed in {repo_name}.\n"
                f"{result.stderr.strip()}"
            )


def cleanup_all_temp_branches(repo_specs, temp_branches):
    """Return all repos to main and delete temp branches."""
    eprint("[cleanup] Restoring repos to main...")
    for repo_name in repo_specs:
        repo_path = MODULES_DIR / repo_name
        cleanup_temp_branch(repo_path, temp_branches[repo_name])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run DIALS benchmarks and compare branches."
    )
    parser.add_argument(
        "--dials",
        metavar="BRANCHES",
        help='Branch spec for dials repo, e.g. "feat1" or "dep1 dep2 feat3"',
    )
    parser.add_argument(
        "--cctbx_project",
        metavar="BRANCHES",
        help="Branch spec for cctbx_project repo",
    )
    parser.add_argument(
        "--dxtbx",
        metavar="BRANCHES",
        help="Branch spec for dxtbx repo",
    )
    parser.add_argument(
        "--nproc",
        type=int,
        default=10,
        metavar="N",
        help="Number of processors for multi-core benchmark (default: 10)",
    )
    args = parser.parse_args()

    # Collect specified repos
    repo_specs = {}
    for repo_name in REPO_NAMES:
        raw = getattr(args, repo_name)
        if raw is not None:
            branches = raw.split()
            if not branches:
                parser.error(f"--{repo_name} value is empty")
            repo_specs[repo_name] = branches

    if not repo_specs:
        parser.error("At least one of --dials, --cctbx_project, --dxtbx is required.")

    return args, repo_specs


def main():
    args, repo_specs = parse_args()
    nproc = args.nproc

    check_prerequisites()

    # Determine case: A (single branch) or B (multiple branches per spec)
    # Case B applies when ANY repo has more than one branch.
    is_case_b = any(len(branches) > 1 for branches in repo_specs.values())

    timestamp = datetime.datetime.now().isoformat(timespec="seconds")
    ts_tag = str(int(time.time()))

    configuration = {repo: " ".join(branches) for repo, branches in repo_specs.items()}
    configuration["nproc"] = nproc
    output = {
        "timestamp": timestamp,
        "configuration": configuration,
    }

    if not is_case_b:
        # ------------------------------------------------------------------
        # Case A: single branch per repo
        # ------------------------------------------------------------------
        eprint(f"[main] Case A: single branch comparison")

        # Baseline: all repos on main
        setup_repos_baseline(repo_specs)
        rebuild_dials()
        eprint("[bench] Running baseline benchmarks (main)...")
        baseline = run_full_benchmark(nproc)

        # Feature: checkout requested branches
        try:
            apply_branches_case_a(repo_specs)
            rebuild_dials()
            eprint("[bench] Running feature benchmarks...")
            feature = run_full_benchmark(nproc)

            output["baseline"] = baseline
            output["feature"] = feature
            output["comparison"] = compute_comparison(baseline, feature)
        finally:
            eprint("[cleanup] Restoring repos to main...")
            for repo_name in repo_specs:
                repo_path = MODULES_DIR / repo_name
                main_name = 'master' if repo_name == 'cctbx_project' else 'main'
                try:
                    ensure_main(repo_path, main_name)
                except SystemExit as exc:
                    eprint(f"WARNING: cleanup failed for {repo_name}: {exc}")

    else:
        # ------------------------------------------------------------------
        # Case B: multiple branches — dep1..depN vs dep1..depN+feature
        # ------------------------------------------------------------------
        eprint(f"[main] Case B: multi-branch merge comparison")

        temp_branches = {
            repo_name: f"temp-bench-{ts_tag}"
            for repo_name in repo_specs
        }

        try:
            # Pre: main + all-but-last branches
            setup_repos_baseline(repo_specs)
            apply_branches_case_b_pre(repo_specs, temp_branches)
            rebuild_dials()
            eprint("[bench] Running pre benchmarks (deps only)...")
            pre = run_full_benchmark(nproc)

            # Post: + final (feature) branch
            apply_branches_case_b_post(repo_specs)
            rebuild_dials()
            eprint("[bench] Running post benchmarks (deps + feature)...")
            post = run_full_benchmark(nproc)

            output["baseline"] = pre
            output["feature"] = post
            output["comparison"] = compute_comparison(pre, post)

        finally:
            cleanup_all_temp_branches(repo_specs, temp_branches)

    # Print human summary
    print_human_summary(output["comparison"])

    # JSON output to stdout
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
