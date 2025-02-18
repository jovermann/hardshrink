#!/usr/bin/python
#
# vsize.py - get current virtual memory consumed by the current process
#
#
# Copyright (C) 2019 by Johannes Overmann <Johannes.Overmann@joov.de>


import os
import platform
import subprocess


def get_vsize():
    """Get vsize of the current process in bytes.
    
    vsize means the virtual memory used.
    """
    if platform.system() == "Darwin": # macOS
        cmd = "ps x -p {} -o vsz".format(os.getpid())
        lines = subprocess.check_output(cmd.split())
        lines = lines.splitlines()
        return int(lines[1])
    else:
        # Linux
        path = "/proc/{:d}/status".format(os.getpid())
        try:
            with open(path, "rb") as f:
                for line in f:
                    if line.startswith(b"VmSize:"):
                        return int(line[7:].lstrip().split()[0]) * 1024        
        except IOError:
            pass
    return 0


    
      
if __name__ == "__main__":
    print("vsize={} {:.1f}MB".format(get_vsize(), get_vsize() / 1024.0 / 1024.0))


