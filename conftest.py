import sys
import os

_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_root, "generator", "src"))
sys.path.insert(0, os.path.join(_root, "transformations", "src"))

# Ensure Spark workers use the same Python interpreter as the driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Colima (alternative Docker runtime on macOS) uses a non-standard socket path.
# Testcontainers needs DOCKER_HOST to find it.
_colima_sock = os.path.expanduser("~/.colima/default/docker.sock")
if os.path.exists(_colima_sock) and not os.environ.get("DOCKER_HOST"):
    os.environ["DOCKER_HOST"] = f"unix://{_colima_sock}"
