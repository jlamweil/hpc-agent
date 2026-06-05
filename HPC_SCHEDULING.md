# HPC Scheduling Guide — GPU Wait Times

## Overview

The HPC cluster at **hpc-login.u-strasbg.fr** uses SLURM with partitions and QoS controls. Wait times vary dramatically depending on the partition, GPU type, and whether you have preemptive QoS. This doc maps model requirements to expected scheduling delays.

---

## Accessible Partitions

| Partition | Account | QoS | Queue Depth | Wait Time |
|-----------|---------|-----|-------------|-----------|
| `publicgpu` | `icube` | `defaultqos` | **9,826 pending**, 65 running | **~79 days** |
| `pri2021gpu` | `qosmivgpu` | `preemptqos` | **4 pending**, 16 running | **1–4 days** |
| `public` (CPU) | `icube` | `defaultqos` | 5,396 pending, 25 running | months |
| `grantgpu` | — | — | 0 pending, 5 running | ❌ no access |
| `grant` | — | — | 55 pending, 118 running | ❌ no access |

### Key Insight

- `publicgpu` is **massively oversubscribed** (~150:1 pending:running). Any job submitted there schedules for **~79 days out** regardless of GPU type.
- `pri2021gpu` with `preemptqos` is **the only viable path** for GPU workloads. It preempts lower-priority jobs, reducing wait to **1–4 days**.

---

## GPU Nodes by Type

| GPU | VRAM | Nodes | GPUs/Node | Total GPUs | Model Capacity |
|-----|------|-------|-----------|------------|----------------|
| **H200** | 141 GB | 5 | 8 | 40 | V4-Flash native, V4-Pro FP8 |
| **H100 80GB NVLink** | 80 GB | 2 | 4 | 8 | V4-Flash FP8, 70B Q4 |
| **H100 40GB** | 40 GB | 2 | 4 | 8 | 70B Q4, V4-Flash borderline |
| **A100 80GB HGX** | 80 GB | 1 | 4 | 4 | 70B Q4, V4-Flash FP8 tight |
| **A100 40GB** | 40 GB | 4 | 2 | 8 | 32B Q4, 70B Q2 |
| **A40** | 48 GB | 2 | 2 | 4 | 32B Q4, 70B Q4 tight |
| **L40S** | ~45 GB | 2 | 4 | 8 | 32B Q4, 70B Q3 |
| **V100** | 32 GB | 4 | 4 | 16 | 16B Q4, 32B Q2 |
| **RTX 6000** | 24 GB | 6 | 2-3 | ~15 | 16B Q4 |
| **P100** | 16 GB | 13 | 4 | 52 | 7B/8B Q4 |
| **RTX 5000** | 16 GB | 1 | 2 | 2 | 7B/8B Q4 |

---

## Model → GPU → Wait Time Matrix

For each model, this shows: minimum viable GPU config, which partition to use, and expected wait.

### Tiny Models (<10B params)

| Model | VRAM (GGUF Q4) | Min GPU | Wait (publicgpu) | Wait (pri2021gpu) |
|-------|---------------|---------|-----------------|-------------------|
| TinyLlama 1.1B | 0.7 GB | **any GPU** (including P100) | 1s if P100 idle (only idle nodes) | 1-2 days |
| Llama 3.2 1B | 0.7 GB | any GPU | 1s if P100 idle | 1-2 days |
| Llama 3.2 3B | 2 GB | any GPU | 1s if P100 idle | 1-2 days |
| Qwen2.5 7B | 5 GB | any GPU | 1s if P100 idle | 1-2 days |

> **Verdict:** These fit on P100 nodes which are **currently idle**. Submit without constraint and get instant start on publicgpu.

### Mid Models (10B–32B)

| Model | VRAM (GGUF Q4) | Min GPU | Wait (publicgpu) | Wait (pri2021gpu) |
|-------|---------------|---------|-----------------|-------------------|
| DeepSeek-Coder-V2-16B | 16 GB | **RTX 5000**, P100, or any | **~79 days** | **1-2 days** |
| Qwen2.5-Coder-32B | 22 GB | **A40** (48GB), L40S, or larger | **~79 days** | **1-2 days** |
| Mistral Small 22B | 14 GB | RTX 6000 (24GB), A40, or larger | **~79 days** | **1-2 days** |

> **Verdict:** Must use `pri2021gpu`. These don't fit on P100 (16GB is tight for 16B class). A40/L40S/RTX6000 nodes are fully allocated — need preemption.

### Large Models (70B–236B)

| Model | VRAM (GGUF Q4) | Min GPU | Wait (publicgpu) | Wait (pri2021gpu) |
|-------|---------------|---------|-----------------|-------------------|
| Llama 3.3 70B | 42 GB | **1× A100 80GB**, 2× A100 40GB | **~79 days** | **1-4 days** |
| DeepSeek-R1-70B | 42 GB | 1× A100 80GB, 2× A100 40GB | **~79 days** | **1-4 days** |
| Qwen2.5 72B | 44 GB | 1× A100 80GB | **~79 days** | **1-4 days** |
| DeepSeek-V2-236B | 140 GB | 4× A100 80GB | **~79 days** | **4+ days** |

> **Verdict:** A100 80GB HGX (hpc-n931) is the best fit for 70B class. DeepSeek-V2-236B needs 4 GPUs — rare availability.

### DeepSeek V4 Models

#### DeepSeek-V4-Flash (284B total, 13B active)

| Config | Precision | Size | Min GPUs | Wait (publicgpu) | Wait (pri2021gpu) |
|--------|-----------|------|----------|-----------------|-------------------|
| **Native FP4+FP8** (vLLM) | FP4+FP8 mixed | ~146 GB | **4× H200** (8×) | **~79 days** | N/A (H200 not in pri2021gpu) |
| **FP8 quantized** (SGLang) | FP8 | ~284 GB | **4–8× H100 80GB** | **~79 days** | **1–4 days** ✅ |
| **GGUF Q4_K_M** | 4-bit | ~60 GB | **1× A100 80GB** | **~79 days** | **1–4 days** |
| **GGUF Q3_K_M** | 3-bit | ~45 GB | **1× A40 48GB** | **~79 days** | **1–4 days** |
| **GGUF Q2_K** | 2-bit | ~30 GB | **1× V100 32GB** | **~79 days** | **1–4 days** |

> **Best path:** FP8 quantized on **pri2021gpu H100 80GB NVLink** (hpc-n933/34), 4 GPUs. ~1 day wait with preemption. **(Currently submitted: job 17072213)**

#### DeepSeek-V4-Pro (1.6T total, 49B active)

| Config | Precision | Size | Min GPUs | Wait |
|--------|-----------|------|----------|------|
| **Native FP4+FP8** | FP4+FP8 mixed | ~800 GB | **8× H200** | **~79 days** (only publicgpu) |
| **FP8 quantized** | FP8 | ~1.6 TB | **8+ GPUs** | Not feasible on this cluster |

> **Verdict:** V4-Pro is **not practical** on this cluster. Only publicgpu has H200 nodes, and they're fully packed with ~79 day wait.

---

## Scheduling Strategies

### 1️⃣ Always use `pri2021gpu` with `preemptqos`
```bash
#SBATCH --partition=pri2021gpu
#SBATCH --account=qosmivgpu
#SBATCH --qos=preemptqos
```

This **preempts** lower-priority jobs. Without it, publicgpu has a **79-day queue**.

### 2️⃣ Pin to specific nodes if you need faster start
```bash
# H100 40GB — different node, potentially shorter queue
#SBATCH --nodelist=hpc-n982

# A100 80GB HGX — less competition 
#SBATCH --nodelist=hpc-n931
```

### 3️⃣ Don't over-request GPUs you don't need
```bash
# Wrong: --gres=gpu:4 if the model fits on 1 GPU
# Right: --gres=gpu:1  (fits in more idle slots)
```

### 4️⃣ Use GPU constraints to avoid landing on old hardware
```bash
#SBATCH --constraint=gpuh100  # Only H100-class (80GB or 40GB)
#SBATCH --constraint='gpuh100&gpu80g'  # Specifically H100 80GB
```

### 5️⃣ For small models, target idle P100 nodes
P100 (16GB) nodes are the only ones with **idle** capacity on publicgpu. Models ≤7B fit there and start instantly:
```bash
#SBATCH --partition=publicgpu
# No constraint — will land on idle P100
```

### 6️⃣ Batch your work to amortize wait time
Once you get a GPU slot, use the full time allocation. For vLLM servers, request 12-24h:
```bash
#SBATCH --time=24:00:00
```

---

## Current Queue State (June 3, 2026)

| Partition | Pending | Running | Ratio | Our Jobs |
|-----------|---------|---------|-------|----------|
| `publicgpu` | 9,826 | 65 | 151:1 | — |
| `pri2021gpu` | 4 | 16 | 0.25:1 | **17072213**, 17072219, 17074051 |
| `public` | 5,396 | 25 | 216:1 | — |

### Our Active Jobs

| Job ID | Target | GPUs | Est. Start | Status |
|--------|--------|------|------------|--------|
| **17072213** | H100 80GB NVLink (hpc-n933) | 4 | Jun 4 03:56 | PD (Resources) |
| 17072219 | A100 80GB HGX (hpc-n931) | 4 | Jun 7 17:23 | PD (Priority) |
| 17074051 | H100 40GB (hpc-n982) | 4 | Jun 7 16:51 | PD (Priority) |

---

## Practical Recommendations

| If you want to run... | Use... | Expected wait |
|-----------------------|--------|---------------|
| **Quick test** (≤7B model) | `publicgpu` no constraint → P100 idle | **Instant** |
| **DeepSeek-Coder-V2-16B** | `pri2021gpu` → RTX 6000/A40 | **1-2 days** |
| **Qwen2.5-Coder-32B** | `pri2021gpu` → A40/A100 | **1-4 days** |
| **Llama 3.3 70B** | `pri2021gpu` → A100 80GB | **1-4 days** |
| **DeepSeek-V4-Flash** | `pri2021gpu` → H100 80GB NVLink | **1-4 days** |
| **DeepSeek-V4-Pro** | ❌ Not practical (needs 8× H200, only in publicgpu) | **N/A** |
