#!/usr/bin/env python3
"""Download DS V4 Flash model from HuggingFace Hub to HPC cache."""
import os
import time
import sys
from huggingface_hub import snapshot_download

cache = os.path.expanduser("~/hf_cache")
os.environ["HF_HOME"] = cache
os.makedirs(cache, exist_ok=True)

print(f"HF_HOME={cache}", flush=True)
print(f"Start: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

path = snapshot_download(
    "deepseek-ai/DeepSeek-V4-Flash",
    cache_dir=cache,
    resume_download=True,
    local_files_only=False,
)
print(f"Done: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
print(f"Path: {path}", flush=True)
