import os

# Tests run directly on dc1 rather than inside the Docker network.
# PostgreSQL is published on the host at 127.0.0.1:5432.
os.environ["DB_HOST"] = "127.0.0.1"
