# Project Mad-Fin: Agentic RAG Multi-Cloud Data Mesh


## The Problem
Traditional ETL (Extract, Transform, Load) pipelines are static. If business policies change, the code breaks or needs manual updates.

##  The Solution 
I built an autonomous pipeline where an **LLM Agent (Gemini 2.5 Flash)** retrieves data governance policies from a knowledge base and decides which Python tools to execute. This is "Policy-as-Code" in action.

### Key Features:
- **Autonomous Reasoning:** The AI Agent decides the order of execution (Cleansing -> Masking -> Exporting).
- **Data Governance:** Uses **SHA-256 Cryptographic Hashing** to ensure PII (Personally Identifiable Information) is never exposed.
- **FinOps Optimization:** Automatically prunes zero-value "noise" records to save cloud storage costs.
- **High-Performance Storage:** Exports data in **Parquet** format for sub-second query speeds.
- **Scale:** Processes **1,000+ synthetic fintech records** with full time-series support.

## Tech Stack
- **Languages:** Python (Pandas, PyArrow)
- **AI Framework:** LangChain (LCEL)
- **Model:** Google Gemini 2.5 Flash / Gemini Pro
- **Cloud Warehouse:** Google Cloud Platform (BigQuery)
- **Visualization:** Looker Studio / Power BI

## 📂 Project Structure
- `agent_orchestrator.py`: The main "Brain" of the pipeline.
- `data_policy.txt`: The "Knowledge Base" (RAG) containing corporate rules.
- `.env`: (Hidden) Secure API Key management.
- `requirements.txt`: Project dependencies.

-----
