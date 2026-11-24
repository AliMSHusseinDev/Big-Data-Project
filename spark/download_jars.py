"""Install the Kafka connector for this exact Spark/Scala pair and Microsoft's JDBC driver."""

import hashlib
import os
from pathlib import Path
from urllib.request import urlopen

artifacts = [
    ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.5"),
    ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", "3.5.5"),
    ("org.apache.kafka", "kafka-clients", "3.4.1"),
    ("org.apache.commons", "commons-pool2", "2.11.1"),
    ("com.microsoft.sqlserver", "mssql-jdbc", "12.8.1.jre11"),
]
if Path("/run/secrets/build_ca").exists():
    os.environ["SSL_CERT_FILE"] = "/run/secrets/build_ca"
for group, name, version in artifacts:
    filename = f"{name}-{version}.jar"
    url = f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{name}/{version}/{filename}"
    content = urlopen(url, timeout=90).read()
    expected = urlopen(url + ".sha1", timeout=30).read().decode().split()[0]
    if hashlib.sha1(content).hexdigest() != expected:
        raise ValueError(f"Checksum mismatch for {filename}")
    Path("/opt/spark/jars", filename).write_bytes(content)
