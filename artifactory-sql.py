import argparse
import datetime
import os
import sqlite3


def import_data(input_files, output):
    with sqlite3.connect(output, isolation_level="DEFERRED") as db:
        c = db.cursor()
        # https://jfrog.com/help/r/artifactory-how-to-debug-artifactory-issues-based-on-http-status-codes/request-log
        c.execute(
            "CREATE TABLE IF NOT EXISTS logs(date_timestamp INTEGER, trace_id TEXT, remote_address TEXT, username TEXT, request_method TEXT, request_url TEXT, return_status INTEGER, request_content_length_bytes INTEGER, response_content_length_bytes INTEGER, request_duration_ms INTEGER, request_user_agent TEXT) STRICT"
        )
        c.execute("PRAGMA synchronous = OFF")
        c.execute("PRAGMA journal_mode = OFF")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_logs_search ON logs (request_url, response_content_length_bytes, remote_address)"
        )
        for f in input_files:
            parse_file(f, c)
        db.commit()


def parse_file(input_file, c):
    print("Parsing " + input_file)
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            parts = line.split("|")
            assert len(parts) == 11, line
            date_timestamp_raw = parts[0]
            date_timestamp = int(
                datetime.datetime.fromisoformat(
                    date_timestamp_raw.replace("Z", "+00:00")
                ).timestamp()
            )
            trace_id = parts[1]
            remote_address = parts[2]
            username = parts[3]
            request_method = parts[4]
            assert request_method in [
                "DELETE",
                "GET",
                "HEAD",
                "OPTIONS",
                "PATCH",
                "POST",
                "PROPFIND",
                "PUT",
            ], line
            request_url = parts[5]
            return_status = int(parts[6])
            request_content_length_bytes = int(parts[7])
            response_content_length_bytes = int(parts[8])
            request_duration_ms = int(parts[9])
            request_user_agent = parts[10]
            c.execute(
                "INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    date_timestamp,
                    trace_id,
                    remote_address,
                    username,
                    request_method,
                    request_url,
                    return_status,
                    request_content_length_bytes,
                    response_content_length_bytes,
                    request_duration_ms,
                    request_user_agent,
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
