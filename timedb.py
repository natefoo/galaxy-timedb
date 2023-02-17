#!/usr/bin/env python3
import argparse
import os.path
import sqlite3
from collections import namedtuple

import psycopg2
import requests
from psycopg2.extras import NamedTupleCursor

CREATE_SQL = """
CREATE TABLE runtimes (
    tool_id text,
    base_tool_id text,
    tool_version text,
    update_time timestamp DEFAULT CURRENT_TIMESTAMP,
    run_count int,
    min_runtime int,
    median_runtime int,
    mean_runtime int,
    pct95_runtime int,
    pct99_runtime int,
    max_runtime int,
    active bool DEFAULT true
)
"""

# TODO: quick oneshot for counts
# select tool_id, count(tool_id) as run_count from job where state = 'ok' group by tool_id order by tool_id

COUNT_SQL = """
SELECT
    count(tool_id) AS run_count
FROM job
WHERE
    tool_id = %s
    AND tool_version = %s
    AND state = 'ok'
"""

SUMMARY_SQL = """
WITH runtime_data AS (
    SELECT
        metric_value
    FROM job_metric_numeric
    WHERE
        metric_name = 'runtime_seconds'
        AND
            job_id in (
                SELECT
                    id
                FROM job
                WHERE
                    tool_id = %s
                    AND tool_version = %s
                    AND state = 'ok'
            )
    )
SELECT
    min(metric_value) AS min,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY metric_value) ::bigint AS quant_1st,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY metric_value) ::bigint AS median,
    avg(metric_value) AS mean,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY metric_value) ::bigint AS quant_3rd,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) ::bigint AS perc_95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY metric_value) ::bigint AS perc_99,
    max(metric_value) AS max,
    sum(metric_value) AS sum,
    stddev(metric_value) AS stddev
FROM runtime_data
"""

INSERT_TOOL_SQL = """
INSERT INTO runtimes (
    tool_id,
    base_tool_id,
    tool_version,
    run_count,
    min_runtime,
    median_runtime,
    mean_runtime,
    pct95_runtime,
    pct99_runtime,
    max_runtime)
VALUES (
    :tool_id,
    :base_tool_id,
    :tool_version,
    :run_count,
    :min_runtime,
    :median_runtime,
    :mean_runtime,
    :pct95_runtime,
    :pct99_runtime,
    :max_runtime)
"""

UPDATE_TOOL_SQL = """
UPDATE
    runtimes
SET
    tool_id = :tool_id,
    update_time = datetime('now'),
    run_count = :run_count,
    min_runtime = :min_runtime,
    median_runtime = :median_runtime,
    mean_runtime = :mean_runtime,
    pct95_runtime = :pct95_runtime,
    pct99_runtime = :pct99_runtime,
    max_runtime = :max_runtime
WHERE
    tool_id = :tool_id
    AND tool_version = :tool_version
"""

DEACTIVATE_TOOL_SQL = """
UPDATE
    runtimes
SET
    update_time = datetime('now'),
    active = false
WHERE
    tool_id = :tool_id
    AND tool_version = :tool_version
"""


class Tool:
    def __init__(self, id, version, base_id=None, update_time=None, run_count=-1, min_runtime=None, median_runtime=None, mean_runtime=None, pct95_runtime=None, pct99_runtime=None, max_runtime=None, active=True):
        self.id = id
        self.version = version
        self.base_id = base_id
        self.update_time = update_time
        self.run_count = run_count
        self.min_runtime = min_runtime
        self.median_runtime = median_runtime
        self.mean_runtime = mean_runtime
        self.pct95_runtime = pct95_runtime
        self.pct99_runtime = pct99_runtime
        self.max_runtime = max_runtime
        self.active = active
        assert version is not None
        self.set_base_id()

    def set_base_id(self):
        if self.base_id is not None:
            assert self.id.startswith(self.base_id), f"{self.id}, {self.base_id}"
        elif "/" in self.id:
            self.base_id, id_version = self.id.rsplit("/", 1)
            assert self.version == id_version, f"{self.version} != {id_version}"
        else:
            self.base_id = self.id

    def __str__(self):
        return f"{self.key}: run_count={self.run_count}, min_runtime={self.min_runtime}, median_runtime={self.median_runtime}, mean_runtime={self.mean_runtime}, pct95_runtime={self.pct95_runtime}, pct99_runtime={self.pct99_runtime}, max_runtime={self.max_runtime}, active={self.active}"

    @property
    def key(self):
        return f"{self.base_id}/{self.version}"

    def update_stats(self, summary):
        self.min_runtime = int(summary.min)
        self.median_runtime = int(summary.median)
        self.mean_runtime = int(summary.mean)
        self.pct95_runtime = int(summary.perc_95)
        self.pct99_runtime = int(summary.perc_99)
        self.max_runtime = int(summary.max)

    def upsert_values(self):
        return {
            "tool_id": self.id,
            "base_tool_id": self.base_id,
            "tool_version": self.version,
            "run_count": self.run_count,
            "min_runtime": self.min_runtime,
            "median_runtime": self.median_runtime,
            "mean_runtime": self.mean_runtime,
            "pct95_runtime": self.pct95_runtime,
            "pct99_runtime": self.pct99_runtime,
            "max_runtime": self.max_runtime,
        }


def handle_args():
    parser = argparse.ArgumentParser(description="Tool Runtime Database Utility")
    parser.add_argument("--pgconn", help="PostgreSQL connection string")
    parser.add_argument("--older-than", default="1 week", help="Update all entries older than")
    parser.add_argument("--galaxy-url", default="https://usegalaxy.org", help="Galaxy server URL")
    parser.add_argument("--tool-id", help="Force update to given tool")
    parser.add_argument("sqlite_db_file", help="Runtime SQLite database file")
    return parser.parse_args()


def tool_factory(cursor, row):
    tool_id = row[0]
    version = row[2]
    kwargs_as_args = row[1:2] + row[3:]
    return Tool(tool_id, version, *kwargs_as_args)


class App:
    def __init__(self, db_file=None, pg_conn_string=None, galaxy_url=None, older_than=None):
        self.db_file = db_file
        self.pg_conn_string = pg_conn_string
        self.galaxy_url = galaxy_url
        self.older_than = older_than
        self.__pg_con = None

    @property
    def pg_con(self):
        if not self.__pg_con:
            self.__pg_con = psycopg2.connect(self.pg_conn_string)
        return self.__pg_con

    def make_db(self):
        con = sqlite3.connect(self.db_file)
        cur = con.cursor()
        cur.execute(CREATE_SQL)

    def get_server_tools(self):
        tools = {}
        url = self.galaxy_url.rstrip("/") + "/api/tools?in_panel=false"
        response = requests.get(url)
        for tool_elem in response.json():
            tool = Tool(tool_elem["id"], tool_elem["version"])
            tools[tool.key] = tool
        return tools

    def get_db_tools(self, for_update=False):
        tools = {}
        sql = """SELECT * FROM runtimes WHERE active"""
        args = None
        if for_update:
            sql += " AND update_time < datetime('now', ?)"
            args = (f"-{self.older_than}",)
        con = sqlite3.connect(self.db_file)
        con.row_factory = tool_factory
        cur = con.cursor()
        if args:
            cur.execute(sql, args)
        else:
            cur.execute(sql)
        for tool in cur.fetchall():
            tools[tool.key] = tool
        return tools

    def run_count(self, tool):
        with self.pg_con.cursor() as cur:
            cur.execute(COUNT_SQL, (tool.id, tool.version))
            count = cur.fetchone()[0]
        return count

    def summary_stats(self, tool):
        with self.pg_con.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute(SUMMARY_SQL, (tool.id, tool.version))
            summary = cur.fetchone()
        return summary

    def commit_tool(self, tool, sql):
        con = sqlite3.connect(self.db_file)
        cur = con.cursor()
        cur.execute(sql, tool.upsert_values())
        con.commit()

    def upsert_tool(self, tool, upsert_sql):
        count = self.run_count(tool)
        tool.run_count = count
        summary = self.summary_stats(tool)
        tool.update_stats(summary)
        print(tool)
        self.commit_tool(tool, upsert_sql)

    def handle_tool_changes(self):
        server_tools = self.get_server_tools()
        db_tools = self.get_db_tools()
        server_tool_keys = set(server_tools.keys())
        db_tool_keys = set(db_tools.keys())
        for tool_key in server_tool_keys - db_tool_keys:
            self.upsert_tool(server_tools[tool_key], INSERT_TOOL_SQL)
        for tool_key in db_tool_keys - server_tool_keys:
            print(f"deactivating removed tool: {tool.key}")
            self.commit_tool(tool, DEACTIVATE_TOOL_SQL)
        for tool_key, tool in self.get_db_tools(for_update=True).items():
            self.upsert_tool(tool, UPDATE_TOOL_SQL)


def main():
    args = handle_args()
    app = App(db_file=args.sqlite_db_file, pg_conn_string=args.pgconn, galaxy_url=args.galaxy_url, older_than=args.older_than)
    if not os.path.exists(args.sqlite_db_file):
        app.make_db()
    if args.tool_id:
        tool_id, version = args.tool_id.rsplit("/", 1)
        tool = Tool(args.tool_id, version)
        app.new_tool(tool)
    else:
        app.handle_tool_changes()


if __name__ == "__main__":
    main()
