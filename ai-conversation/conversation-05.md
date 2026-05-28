# Conversation 5
Date: May 27, 2026 18:48 UTC
---

london: Look at docker-compose.yml and at transformations 
What do I need for open Spark UI ?

LLM: Let me check the docker-compose.yml and any transformation-related files.

LLM: There's no `docker-compose.yml` in this project, and no transformation files either. The project only has `android/`, `docs/`, `infra/firebase/`, and a `README.md`.

Could you double-check where these files are? They may be in a different directory or not yet added to the repo (the git status shows `infra/` is untracked, so it might be partially set up). A few options:

1. **Share the path** — if docker-compose.yml is elsewhere on your machine, tell me where.
2. **Check if it's in `docs/`** — the docs folder might have a backend decision doc referencing Spark.

LLM: Now I understand the context. "Spark" here refers to **Firebase's free plan**, not Apache Spark. The "Spark UI" is the **Firebase Console**.

LLM: Now I have the full picture. You're using the **Firebase Emulator Suite**, not Apache Spark. The "Spark UI" referred to in your question appears to be the **Firebase Emulator UI**.

To open it, you need to:

1. **Install Firebase CLI** (if not already):
   ```bash
   npm install -g firebase-tools
   ```

2. **Start the emulators** from the `infra/firebase/` directory:
   ```bash
   cd infra/firebase
   firebase emulators:start
   ```

3. **Open the Emulator UI** in your browser:
   ```
   http://localhost:4000
   ```

From [firebase.json](infra/firebase/firebase.json), these are all the ports your emulators run on:

| Service | Port |
|---|---|
| **Emulator UI** | **4000** |
| Auth | 9099 |
| Firestore | 8080 |
| Storage | 9199 |
| Hub | 4400 |
| Logging | 4500 |

Note: your `.firebaserc` points to project `demo-roadtip`. The `demo-` prefix is special — Firebase treats it as a local demo project that doesn't require a real Firebase account or internet connection.
