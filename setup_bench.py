#!/usr/bin/env python3
"""
One-time setup for DIALS benchmarking environment.
  - Downloads and runs get_test_data.com (360 core CBF images + 3600 symlinks)
  - Installs DIALS via bootstrap.py if not already present
"""

import os
import stat
import sys
import subprocess
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WORK_DIR = Path(__file__).parent.resolve()
DATA_DIR = WORK_DIR / "data"
DIALS_DIR = WORK_DIR / "dials"
DIALS_ENV = DIALS_DIR / "dials"

TEST_DATA_SCRIPT = WORK_DIR / "get_test_data.com"
BOOTSTRAP_PY = WORK_DIR / "bootstrap.py"

TEST_DATA_URL = "https://bl831.als.lbl.gov/~jamesh/benchmarks/testdata/get_test_data.com"
BOOTSTRAP_URL = "https://raw.githubusercontent.com/dials/dials/main/installer/bootstrap.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    """Print a progress message to stderr."""
    print(f"[setup_bench] {msg}", file=sys.stderr, flush=True)


def die(msg: str) -> None:
    """Print an error message and exit non-zero."""
    print(f"[setup_bench] ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


def download(url: str, dest: Path) -> None:
    """Download *url* to *dest*, printing progress."""
    log(f"Downloading {url} -> {dest}")
    try:
        urllib.request.urlretrieve(url, dest)
    except Exception as exc:
        die(f"Failed to download {url}: {exc}")


def run(cmd: str, cwd: Path, timeout: int = 1800) -> None:
    """Run *cmd* via bash in *cwd*; exit on failure."""
    log(f"Running: {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        executable="/bin/bash",
        cwd=cwd,
        timeout=timeout,
    )
    if result.returncode != 0:
        die(f"Command failed (exit {result.returncode}): {cmd}")


def run_capture(cmd: str, cwd: Path) -> str:
    """Run *cmd* via bash in *cwd*, return stdout; exit on failure."""
    result = subprocess.run(
        cmd,
        shell=True,
        executable="/bin/bash",
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        die(f"Command failed (exit {result.returncode}): {cmd}\n{result.stderr}")
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Task 1: test data
# ---------------------------------------------------------------------------

def setup_data() -> None:
    """Download get_test_data.com, run it, and verify the output."""
    log("=== Data setup ===")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Check whether data is already present
    core_images = list(DATA_DIR.glob("core_*.cbf"))
    core_large = [f for f in core_images if f.stat().st_size > 6_000_000]
    test_images = list(DATA_DIR.glob("test_*.cbf"))

    if len(core_large) == 360 and len(test_images) == 3600:
        log("Test data already present (360 core images, 3600 test images). Skipping.")
        return

    # Download get_test_data.com if needed
    if not TEST_DATA_SCRIPT.exists():
        download(TEST_DATA_URL, TEST_DATA_SCRIPT)
    else:
        log(f"{TEST_DATA_SCRIPT} already exists, skipping download.")

    # Make executable
    current_mode = TEST_DATA_SCRIPT.stat().st_mode
    TEST_DATA_SCRIPT.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # Run it (tcsh script — relies on tcsh being available)
    run(str(TEST_DATA_SCRIPT), cwd=WORK_DIR, timeout=3600)

    # Verify
    core_images = list(DATA_DIR.glob("core_*.cbf"))
    core_large = [f for f in core_images if f.stat().st_size > 6_000_000]
    test_images = list(DATA_DIR.glob("test_*.cbf"))

    if len(core_large) != 360:
        die(
            f"Expected 360 core CBF images > 6 MB, found {len(core_large)}. "
            "Check get_test_data.com output above."
        )
    if len(test_images) != 3600:
        die(
            f"Expected 3600 test CBF symlinks, found {len(test_images)}. "
            "Check get_test_data.com output above."
        )

    log(f"Data verified: {len(core_large)} core images, {len(test_images)} test images.")


# ---------------------------------------------------------------------------
# Task 2: DIALS installation
# ---------------------------------------------------------------------------

def setup_dials() -> None:
    """Install DIALS via bootstrap.py unless already installed."""
    log("=== DIALS setup ===")

    # Check for existing installation
    if DIALS_ENV.exists():
        log(f"{DIALS_ENV} already exists. Verifying DIALS installation...")
        version = run_capture(f"source {DIALS_ENV} && dials.version", cwd=WORK_DIR)
        if "DIALS" in version:
            log(f"DIALS already installed: {version.splitlines()[0]}")
            return
        else:
            log("DIALS env exists but dials.version did not return expected output. Reinstalling...")

    # Download bootstrap.py
    if not BOOTSTRAP_PY.exists():
        download(BOOTSTRAP_URL, BOOTSTRAP_PY)
    else:
        log(f"{BOOTSTRAP_PY} already exists, skipping download.")

    # Run bootstrap (builds DIALS + libtbx)
    log("Running bootstrap.py --libtbx (this may take 30-60 minutes)...")
    run(f"python3 {BOOTSTRAP_PY} --libtbx", cwd=WORK_DIR, timeout=7200)

    # Verify
    if not DIALS_ENV.exists():
        die(
            f"{DIALS_ENV} not found after bootstrap. "
            "Check bootstrap output above for errors."
        )

    version = run_capture(f"source {DIALS_ENV} && dials.version", cwd=WORK_DIR)
    if "DIALS" not in version:
        die(
            f"dials.version did not return expected output after install.\n"
            f"Output was: {version}"
        )

    log(f"DIALS installed successfully: {version.splitlines()[0]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_data()
    setup_dials()
    log("=== Setup complete ===")


if __name__ == "__main__":
    main()
