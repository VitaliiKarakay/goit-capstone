#!/usr/bin/env python3
"""Strip spec.replicas from the worker Deployment in a multi-doc kubectl manifest stream."""
import re
import sys

chunks = re.split(r"^---\s*$", sys.stdin.read(), flags=re.MULTILINE)
out = []
for ch in chunks:
    if not ch.strip():
        continue
    if re.search(r"^kind:\s*Deployment\s*$", ch, re.MULTILINE) and re.search(
        r"^  name:\s*worker\s*$", ch, re.MULTILINE
    ):
        ch = re.sub(r"^  replicas:\s*\d+\s*$", "", ch, flags=re.MULTILINE)
    out.append(ch.rstrip("\n"))
print("---\n".join(out))
if out:
    print()