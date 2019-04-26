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


# define function to allow us to extract time from string
def process_time(time_str):
    """ Extracts time in seconds from string formatted as 'HH:MM:SS'

    Parameters
    ----------
    time_str: String
        String encoding time elapsed in format 'HH:MM:SS'

    Returns
    -------
    int
        Time elapsed in seconds
    """
    hours, minutes, seconds = time_str.split(":", 2)
    total_time = int(seconds) + 60 * (int(minutes) + 60 * int(hours))
    return total_time


# let's get started by extracting the job id
jobid = sys.argv[1]

try:
    proc = subprocess.run(["qstat", "-j", jobid], encoding='utf-8',
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if proc.returncode == 0:
        state = ""
        for line in proc.stdout.split('\n'):
            # let's determine job state
            if line.startswith("job_state"):
                parts = line.split(":")
                state = parts[1].strip()
            # let's determine if our cpu usage is hung (cpu <<< walltime)
            if line.startswith("usage"):
                # extract cpu and wallclock time from usage line
                match = re.search(
                    "wallclock=([0-9]+:[0-9]+:[0-9]+),"
                    " cpu=([0-9]+:[0-9]+:[0-9]+)", line
                )
                # if we are able to extract the time...
                if match:
                    wallclock = process_time(match.group(1))
                    cpu = process_time(match.group(2))
                    # only do anything about it if we have been waiting a while
                    if wallclock * 60 > int({{cookiecutter.cpu_hung_min_time}}):
                        # only if the ratio of usage is below a certain value
                        usage_ratio = cpu / wallclock
                        if usage_ratio < float({{cookiecutter.cpu_hung_max_ratio}}):
                            # kill the job (wait for next check to show failed)
                            proc = subprocess.run(
                                ["qdel", jobid], encoding="utf-8",
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE
                            )


        if "E" in state:
            print("failed")
        else:
            print("running")
    else:
        proc = subprocess.run(["qacct", "-j", jobid], encoding='utf-8',
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if proc.returncode == 0:
            job_props = {}
            for line in proc.stdout.split('\n'):
                parts = line.split(maxsplit=1)
                if len(parts) <= 1:
                    continue

                key, value = parts
                job_props[key.strip()] = value.strip()

            if (job_props.get("failed", "1") == "0" and
                    job_props.get("exit_status", "1") == "0"):
                print("success")
            else:
                print("failed")
        else:
            # If not found with qstat or qacct, it's probably in some sort of
            # transistion phase (from running to finished), so let's not
            # confuse snakemake to think it may have failed.
            print("running")
except KeyboardInterrupt:
    pass
