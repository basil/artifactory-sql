import argparse
import geoip2.database
import datetime
import os
import sqlite3


class LogImporter:
    def __init__(self, output, mmdb):
        self.output = output
        self.mmdb = mmdb

    def __enter__(self):
        self.db = sqlite3.connect(self.output)
        self.cursor = self.db.cursor()
        if self.mmdb:
            self.reader = geoip2.database.Reader(self.mmdb)
        else:
            self.reader = None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.reader:
            self.reader.close()
        self.db.close()

    def setup_database(self):
        # https://jfrog.com/help/r/artifactory-how-to-debug-artifactory-issues-based-on-http-status-codes/request-log
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS logs(date_timestamp INTEGER, trace_id TEXT, remote_address TEXT, remote_organization TEXT, username TEXT, request_method TEXT, request_url TEXT, return_status INTEGER, request_content_length_bytes INTEGER, response_content_length_bytes INTEGER, request_duration_ms INTEGER, request_user_agent TEXT) STRICT"
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_logs_search ON logs (request_url, response_content_length_bytes, remote_address, remote_organization)"
        )
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = OFF")
        self.db.commit()

    def import_files(self, input_files):
        self.setup_database()
        for f in input_files:
            self.parse_file(f)

    def parse_file(self, input_file):
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
                remote_organization = None
                if self.reader:
                    try:
                        remote_organization = self.reader.asn(
                            remote_address
                        ).autonomous_system_organization
                    except geoip2.errors.AddressNotFoundError:
                        pass
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
                self.cursor.execute(
                    "INSERT INTO logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        date_timestamp,
                        trace_id,
                        remote_address,
                        remote_organization,
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
        self.db.commit()


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
        "-m",
        "--mmdb",
        default=None,
        help="Path to the GeoLite2 ASN MaxMind database file.",
    )
    parser.add_argument(
        "input", nargs="+", help="The Artifactory log file(s) to parse."
    )
    args = parser.parse_args()
    with LogImporter(args.output, args.mmdb) as importer:
        importer.import_files(args.input)
    os.execlp("sqlite3", "sqlite3", os.path.abspath(args.output))
