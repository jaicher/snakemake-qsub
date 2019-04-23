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


jobid = sys.argv[1]

try:
    proc = subprocess.run(["qstat", "-j", jobid], encoding='utf-8',
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if proc.returncode == 0:
        state = ""
        for line in proc.stdout.split('\n'):
            if line.startswith("job_state"):
                parts = line.split(":")
                state = parts[1].strip()

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
