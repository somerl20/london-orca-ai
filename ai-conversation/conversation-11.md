# Conversation 11
Date: May 27, 2026 20:47 UTC
---

london: cd to /Users/omerlondon/Documents/projects/orca-ai/london-orca-ai
read this file docs/Data Platform Engineer — Home Assignment/docs/Data Platform Engineer — Home Assignment.pdf
and write the readme according to the doc with instruction for IOS and for windows
Including all relevant instructions

LLM: Now let me read the PDF file.

LLM: Let me explore the existing project structure first.

LLM: Now I have a solid picture of the entire project. Let me write a comprehensive README.

LLM: Now I have everything I need. Let me write the comprehensive README.

LLM: The README has been written at [README.md](README.md). Here's what it covers:

**Structure:**
- **Architecture diagram** — visual flow from generator → SeaweedFS → RabbitMQ → Worker → Delta Lake → analytics/monitoring
- **Prerequisites** — separate sections for macOS and Windows, including Docker Desktop memory settings, WSL2 configuration, and the Java requirement for running tests locally on Windows
- **Quick Start** — single `docker compose up` command with profile variants for monitoring/analytics
- **Service URLs & Credentials** — all 7 services with ports and login details
- **Generator Configuration** — all env vars with defaults, plus local (non-Docker) run commands with PowerShell equivalents
- **Scaling Workers** — horizontal scaling with `--scale worker=N`
- **Validating Data** — 3 options: exec script in container, Metabase, raw SQL
- **Monitoring** — Prometheus targets and key metrics to watch
- **Running Tests** — venv setup for both platforms, Java setup for Windows, individual test targets
- **Failure Handling** — table mapping every failure scenario to the pipeline's behaviour
- **Troubleshooting** — OOM, port conflicts, SeaweedFS 403, Windows-specific `JAVA_HOME` and `python` issues
