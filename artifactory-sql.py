import argparse
import datetime
import os
import sqlite3


def import_data(input_files, output):
    with sqlite3.connect(output, isolation_level="DEFERRED") as db:
        c = db.cursor()
        c.execute(
            "CREATE TABLE IF NOT EXISTS logs(date_timestamp INTEGER, hash TEXT, request_time_ms INTEGER, request_type TEXT, ip TEXT, repo TEXT, path TEXT, size_bytes INTEGER) STRICT"
        )
        c.execute("""PRAGMA synchronous = OFF""")
        c.execute("""PRAGMA journal_mode = OFF""")
        for f in input_files:
            parse_file(f, c)


def parse_file(input_file, c):
    print("Parsing " + input_file)
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            parts = line.split("|")
            assert len(parts) == 7, line
            date_timestamp_raw = parts[0]
            date_timestamp = datetime.datetime.strptime(
                date_timestamp_raw, "%Y%m%d%H%M%S"
            ).timestamp()
            hash = parts[1]
            request_time_ms = int(parts[2])
            request_type = parts[3]
            assert request_type in [
                "UPLOAD",
                "DOWNLOAD",
                "REQUEST",
                "REDIRECT",
            ], line
            ip = parts[4]
            resource = parts[5]
            resource_parts = resource.split(":", 1)
            assert len(resource_parts) == 2, line
            repo = resource_parts[0]
            path = resource_parts[1]
            size_bytes = int(parts[6])
            c.execute(
                "INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    date_timestamp,
                    hash,
                    request_time_ms,
                    request_type,
                    ip,
                    repo,
                    path,
                    size_bytes,
                ),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-o",
        "--output",
        default="artifactory.db",
        help="The name of the SQLite database to create.",
    )
    parser.add_argument(
        "input", nargs="+", help="The Artifactory log file(s) to parse."
    )
    args = parser.parse_args()
    import_data(args.input, args.output)
    os.execlp("sqlite3", "sqlite3", os.path.abspath(args.output))
