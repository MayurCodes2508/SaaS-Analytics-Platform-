import subprocess
import sys

# mode passed from GitHub Actions (default incremental)
mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"

# 1) Run extract
extract = subprocess.run(
    ["python", "extract/api_extract.py", mode],
    capture_output=True,
    text=True
)

print(extract.stdout)  # keep logs visible in GitHub Actions

if extract.returncode != 0:
    raise Exception("Extract failed")

# 2) Parse output
run_id = None
dt = None

for line in extract.stdout.splitlines():
    if line.startswith("RUN_ID="):
        run_id = line.split("=")[1].strip()
    elif line.startswith("DATE_STR="):
        dt = line.split("=")[1].strip()

if not run_id or not dt:
    raise Exception("Failed to parse RUN_ID or DATE_STR from extract")

# 3) Run process with SAME values
process = subprocess.run(
    ["python", "process/parquet_process.py", run_id, mode, dt]
)

if process.returncode != 0:
    raise Exception("Process failed")

print(f"Pipeline completed successfully- {run_id}")