import socket
import sys
from pyspark.sql import SparkSession


def _module_info(name):
    """Return (version, path) for a module, or an error string if it can't be imported."""
    try:
        mod = __import__(name)
        return getattr(mod, "__version__", "N/A"), getattr(mod, "__file__", "N/A")
    except Exception as e:
        return f"Error: {e}", "N/A"


def get_node_info(partition):
    hostname = socket.gethostname()
    try:
        ip_address = socket.gethostbyname(hostname)
    except Exception:
        ip_address = "N/A"

    cffi_version, cffi_path = _module_info("cffi")
    numpy_version, numpy_path = _module_info("numpy")

    yield (
        hostname,
        {
            "IP Address": ip_address,
            "Python Version": sys.version.split()[0],
            "Python Path": sys.executable,
            "CFFI Version": cffi_version,
            "CFFI Path": cffi_path,
            "NumPy Version": numpy_version,
            "NumPy Path": numpy_path,
        },
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("NodeManager-Python-Details").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Use a high partition count so tasks land on as many worker nodes as possible.
    # Set this to at least the total number of executor cores in the cluster.
    NUM_PARTITIONS = 2000

    # Step 2: Run once per partition (mapPartitions avoids a per-element call)
    node_data = (
        spark.sparkContext.parallelize(range(NUM_PARTITIONS), NUM_PARTITIONS)
        .mapPartitions(get_node_info)
        .collect()
    )

    # Step 3: Deduplicate by hostname -> one entry per unique NodeManager/worker
    unique_nodes = {}
    for hostname, info in node_data:
        unique_nodes.setdefault(hostname, info)

    # Step 4: Print details for every unique node
    print("=" * 100)
    print(f"DISCOVERED {len(unique_nodes)} UNIQUE WORKER NODE(S)")
    print("=" * 100)

    for host, d in sorted(unique_nodes.items()):
        print(f"HOST: {host} ({d['IP Address']})")
        print("-" * 100)
        print(f"  Python Version : {d['Python Version']}  ({d['Python Path']})")
        print(f"  CFFI Version   : {d['CFFI Version']}  ({d['CFFI Path']})")
        print(f"  NumPy Version  : {d['NumPy Version']}  ({d['NumPy Path']})")
        print("=" * 100)

    spark.stop()
