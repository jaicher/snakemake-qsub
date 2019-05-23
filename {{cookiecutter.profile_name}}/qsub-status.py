#!/usr/bin/env python
"""
qsub-status.py

Formerly broad-status.py from github.com/broadinstitute/snakemake-broad-uger

Obtains status for qsub job id

Original license from Broad Institute
MIT License

Copyright (c) 2018 Broad Institute

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import sys
import subprocess
import re
from pathlib import Path
import time


# get directory for checking for jobs before resorting to qacct
CLUSTER_DIR = Path("{{cookiecutter.cluster_dir}}")


# define custom exception for status checks
class StatusCheckException(Exception):
    pass  # custom exception for when one way of checking status fails


def qstat_error(qstat_stdout):
    """ Returns true if error state from qstat stdout, false otherwise
    """
    state = ""
    for line in qstat_stdout.split("\n"):
        if line.startswith("job_state"):
            # get job state
            _, state = line.split(":")
            state = state.strip()
            break  # exit for loop
    return "E" in state


def extract_time(line, time_name):
    """ Extracts time elapsed in seconds from usage line for given name
    """
    result = re.search(f"{time_name}=([^,]+)(,|$,\n)", line)
    if not result:
        return 0  # treat as zero seconds
    time_str = re.search(f"{time_name}=([^,]+)(,|$,\n)", line).group(1)
    elapsed_time = 0
    multiplier = 1
    multipliers = (1, 60, 60, 24)
    for t, m in zip(reversed(time_str.split(":")), multipliers):
        elapsed_time += multiplier * m * int(t)
        multiplier *= m
    return elapsed_time


def handle_hung_qstat(
        jobid, qstat_stdout,
        cpu_hung_min_time=int({{cookiecutter.cpu_hung_min_time}}),
        cpu_hung_max_ratio=int({{cookiecutter.cpu_hung_max_ratio}}),
        debug=False
):
    """ Kills job if hanging, returning True if it determined it was hung job

    Kills job if hanging. Determines that job is hanging by evaluating the
    cpu/walltime ratio -- if it below cpu_hung_max_ratio, considered hung.
    Only evaluates the ratio if wallclock has passed cpu_hung_min_time.

    Parameters
    ----------
    jobid: str
    qstat_stdout: str
    cpu_hung_min_time: int
        Only kill job if the walltime has passed this many minutes
    cpu_hung_max_ratio: int
        Only kill job if the cpu/walltime is below this ratio
    debug: Optional[bool]
        If set, print additonal information to stderr

    Returns
    -------
    bool: True if was hung job and killed
    """
    for line in qstat_stdout.split("\n"):
        if line.startswith("usage"):
            # get the wallclock time
            wallclock = extract_time(line, "wallclock")
            if wallclock < cpu_hung_min_time * 60:
                # not enough time has passed to declare job as hung
                return False
            # get cpu time
            cpu = extract_time(line, "cpu")
            if (cpu / wallclock) < cpu_hung_max_ratio:
                # hung job, so kill it
                if debug:
                    print(f"usage ratio low, killing...", file=sys.stderr)
                subprocess.run(
                    ["qdel", jobid], encoding="utf-8",
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                return True  # we just killed the job
            # otherwise, we aren't below the ratio
            return False
    return False  # we weren't able to get the ratio


def qstat_status(jobid, debug=False):
    """ qstat to obtain job status, raises StatusCheckException if qstat fails

    Parameters
    ----------
    jobid: str
        The job being evaluated

    Returns
    -------
    str: status string (running, failed, success) (success not possible)
    debug: Optional[bool]
        If set, print additonal information to stderr

    Raises
    ------
    StatusCheckException if jobid not found by qstat
    """
    # run qstat
    proc = subprocess.run(
        ["qstat", "-j", jobid], encoding="utf-8",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        # qstat failed...
        raise StatusCheckException(f"qstat failed on job {jobid}")
    # otherwise kill job if CPU usage is too low
    hung = handle_hung_qstat(jobid, proc.stdout, debug=debug)
    status = "failed" if hung or qstat_error(proc.stdout) else "running"
    return status


def cluster_dir_status(jobid):
    """ Checks `CLUSTER_DIR` for status

    Parameters
    ----------
    jobid: str
        The job being evaluated

    Returns
    -------
    str: status string (running, failed, success) (no running here)

    Raises
    ------
    StatusCheckException if jobid not found by this method
    """
    # get the potential exit file path
    exit_file_path = CLUSTER_DIR.joinpath(f"{jobid}.exit")
    # try to open the job exit file
    try:
        exit_file = exit_file_path.open("r")
    except FileNotFoundError:
        raise StatusCheckException(f"cluster_dir_status failed on job {jobid}")
    # with opened file, parse exit status -- last line
    exit_status = exit_file.readlines()[-1].strip()
    # status is success or failed here
    status = "success" if exit_status == "0" else "failed"
    # delete exit file
    try:
        exit_file_path.unlink()
    except FileNotFoundError:
        pass  # okay that it has already been deleted
    return status


def qacct_status(jobid):
    """ Checks qacct for status

    Parameters
    ----------
    jobid: str
        The job being evaluated

    Returns
    -------
    str: status string (running, failed, success) (no running here)

    Raises
    ------
    StatusCheckException if jobid not found by this method
    """
    proc = subprocess.run(
        ["qacct", "-j", jobid], encoding="utf-8",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        # qacct failed...
        raise StatusCheckException(f"qacct failed on job {jobid}")
    # otherwise
    job_props = {}  # keep track of job properties
    # update job properties from stdout from qacct
    for line in proc.stdout.split('\n'):
        parts = line.split(maxsplit=1)
        if len(parts) <= 1:
            continue

        key, value = parts
        job_props[key.strip()] = value.strip()

    # check failed and exit status properties...
    if (job_props.get("failed", "1") == "0" and
            job_props.get("exit_status", "1") == "0"):
        status = "success"
    else:
        status = "failed"
    # return final status
    return status


def missing_status(
        jobid, reset=False,
        missing_job_wait=float({{cookiecutter.missing_job_wait}})
):
    """ Handles missing status

    Parameters
    ----------
    jobid: str
        The job being evaluated
    reset: bool
        If True, just delete the missing status file
    missing_job_wait: float
        The time elapsed in minutes before a missing job id will be evaluated
        by qacct. If qacct has a status exception, job is considered failed

    Returns
    -------
    str: status string (running, failed, success)
    """
    # what is the file we check?
    p = CLUSTER_DIR.joinpath(f"{jobid}.missing")
    # default status is running
    status = "running"
    # if not resetting, create file or check time elapsed...
    if not reset:
        if not p.exists():
            # first time job missing -> create file
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()  # mark the time
        else:
            time_elapsed = (time.time() - p.stat().st_mtime) / 60
            if time_elapsed > missing_job_wait:
                try:
                    status = qacct_status(jobid)
                except StatusCheckException:
                    # considered to be failed
                    status = "failed"
    # if job is failed or if we are resetting, delete the file
    if reset or status == "failed":
        # no longer need the missing file path
        try:
            p.unlink()
        except FileNotFoundError:
            pass  # okay that file doesn't exist
    return status


def check_status(jobid, debug=False):
    """ Uses qstat/local files/qacct to check for the status of given jobid

    Parameters
    ----------
    jobid: str
        The job being evaluated
    debug: Optional[bool]
        If set, print additonal information to stderr

    Returns
    -------
    str: status string (running, failed, success)
    """
    # check qstat
    try:
        # get qstat status
        status = qstat_status(jobid, debug=debug)
        # reset missing job file (qstat worked, so not missing)
        missing_status(jobid, reset=True)
        # return job status
        return status
    except StatusCheckException:
        if debug:
            print("qstat failed, keep going", file=sys.stderr)
        pass
    # try checking cluster dir exit file
    try:
        status = cluster_dir_status(jobid)
        # reset missing job file (this check worked, so not missing)
        missing_status(jobid, reset=True)
        # return job status
        return status
    except StatusCheckException:
        # this check also failed, keep going
        if debug:
            print("exit file check failed, keep going", file=sys.stderr)
        pass
    # treat as missing file for now -- if hits deadline, use qacct
    status = missing_status(jobid, reset=False)  # keep waiting or failed?
    # return final status
    return status


if __name__ == "__main__":
    # let's get started by extracting the job id
    jobid = sys.argv[1]

    try:
        print(check_status(jobid))
    except KeyboardInterrupt:
        sys.exit(0)
