# Troubleshooting

**Worker exits immediately on startup**

```bash
docker-compose logs worker
```

Common causes: RabbitMQ not yet ready (the healthcheck retries for up to 100 s), or SeaweedFS bucket not created yet. The `restart: on-failure` policy will retry automatically.

---

**`java.lang.OutOfMemoryError` in worker logs**

Increase Docker Desktop memory to at least 6 GB (see Prerequisites in the README).

---

**SeaweedFS S3 returns 403**

The bucket must exist before the generator uploads. The generator creates it on startup; if it races, restarting the generator container resolves it:

```bash
docker-compose restart generator
```

---

**Windows: `python` command not found**

Use `py` instead of `python`, or add the Python installation directory to your `PATH` in System Settings → Environment Variables.

---

**Windows: `JAVA_HOME` not set error when running tests locally**

```powershell
# PowerShell — set for the current session
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Add these to your PowerShell profile (`$PROFILE`) to persist across sessions.

---

**Port already in use**

Stop any local services on the conflicting port, or override the host port in `docker-compose.yml`. Common conflicts: port `5672` (local RabbitMQ), `8080` (other HTTP servers).
