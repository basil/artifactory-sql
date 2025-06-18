# Artifactory SQL

An [SQLite](https://www.sqlite.org) interface to [Artifactory](https://jfrog.com/artifactory/) log files. This program:

* Imports the log files into an SQLite database.
* Drops you into an `sqlite3(1)` shell in that database.

## Usage

```sh
$ uv run artifactory-sql.py -h
usage: artifactory-sql.py [-h] [-o OUTPUT] [-a ASN] [-c CITY]
                          input [input ...]

positional arguments:
  input                The Artifactory log file(s) to parse.

options:
  -h, --help           show this help message and exit
  -o, --output OUTPUT  The name of the SQLite database to create. (default:
                       artifactory.db)
  -a, --asn ASN        Path to the GeoLite2 ASN MaxMind database file.
                       (default: None)
  -c, --city CITY      Path to the GeoLite2 City MaxMind database file.
                       (default: None)
```

## Examples

```sql
$ uv run artifactory-sql.py
SQLite version 3.37.2 2022-01-06 13:25:41
Enter ".help" for usage hints.
sqlite> .tables
logs
sqlite> .mode column
sqlite> .header on
sqlite> .schema logs
CREATE TABLE logs(date_timestamp, hash, request_time_ms, request_type, ip, repo, path, size_bytes);
sqlite> SELECT DATETIME(date_timestamp, 'unixepoch'), repo, path, size_bytes FROM logs ORDER BY size_bytes DESC LIMIT 2;
DATETIME(date_timestamp, 'unixepoch')  repo               path                                                                                                              size_bytes
-------------------------------------  -----------------  ----------------------------------------------------------------------------------------------------------------  ----------
2022-12-31 10:13:08                    jcenter-cache      io/prestosql/presto-server/319/presto-server-319.tar.gz                                                           918994355
2023-01-01 04:05:03                    incrementals       org/jenkins-ci/plugins/aws-java-sdk/1.11.1026-rc242.25123f405d91/aws-java-sdk-1.11.1026-rc242.25123f405d91.hpi    200625409
sqlite> SELECT repo, path, size_bytes, count(path) AS cnt FROM logs GROUP BY path ORDER BY cnt DESC LIMIT 5;
repo               path                                                            size_bytes  cnt
-----------------  --------------------------------------------------------------  ----------  ----
releases           org/jenkins-ci/main/jenkins-war/maven-metadata.xml              25680       4214
releases           org/jenkins-ci/plugins/swarm-client/3.22/swarm-client-3.22.jar  6821342     2345
maven-repo1-cache  last_updated.txt                                                29          1577
releases           org/jenkins-ci/plugins/swarm-client/3.17/swarm-client-3.17.jar  2607496     667
releases           org/jenkins-ci/plugins/swarm-client/3.19/swarm-client-3.19.jar  6714221     602
sqlite>
```

## License

Licensed under [the MIT License](LICENSE).
