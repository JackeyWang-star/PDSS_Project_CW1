# Distributed Sparse Matrix & Tensor Algebra Engine

## Overview
This project implements a distributed engine for large-scale sparse matrix and tensor operations using **Apache Spark**.  
It is part of *Programming for Data Science at Scale (PDSS)* 2025–2026 coursework.

---

## Project Scope
Implemented Operations:
- [ ] SpMV (Sparse Matrix × Dense Vector)
- [ ] SpMV (Sparse Matrix × Sparse Vector)
- [ ] SpMM (Sparse Matrix × Dense Matrix)
- [ ] SpMM (Sparse Matrix × Sparse Matrix)
- [ ] Tensor Algebra (e.g. MTTKRP)

---

## System Components
- **Frontend (10%)**: user API for specifying operations and operand types  
- **Execution Engine (35%)**: manages distributed computation using RDDs  
- **Data Layout (10%)**: includes loading, dense/sparse representations  
- **Optimizations (10%)**: intelligent partitioning and hashing  
- **Evaluation (30%)**: performance benchmarks, ablation studies, and comparisons  
- **Conclusion (5%)**

---

## File Structure
```
PDSS_Project_[GroupID]/
├── src/
├── data/
├── results/
├── report.pdf
├── README.md
├── build.sbt
└── honesty.txt
```

---

## Running Instructions
1. Build the project:
   ```bash
   sbt package
   ```
2. Run on Spark:
   ```bash
   spark-submit --class Main target/scala-2.12/pdss_project.jar
   ```
3. Input/output paths can be configured in `config.conf`.

---

## Evaluation
- Micro benchmarks vs Spark DataFrame
- Distributed optimization analysis
- End-to-end performance
- Comparison with real-world system
- Real-world application demo

---

## Policies & Notes
- **No `collect()` on large RDDs**
- **No extensions for CW1**
- **All team members submit identical files**
- **AI-generated code or text is not permitted**
- CW2 reviews via **Yammer** (VPN required)
