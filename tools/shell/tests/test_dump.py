# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os
from pathlib import Path

def test_dump_create(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE TABLE a(i INTEGER)')
    result.check_stdout('COMMIT')

@pytest.mark.parametrize("pattern", [
    "a",
    "a%"
])
def test_dump_specific(shell, pattern):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (i INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (42);")
        .statement(f".dump {pattern}")
    )
    result = test.run()
    result.check_stdout('CREATE TABLE a(i INTEGER)')

# Original comment: more types, tables and views
def test_dump_mixed(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE a (d DATE, k FLOAT, t TIMESTAMP);")
        .statement("CREATE TABLE b (c INTEGER);")
        .statement(".changes off")
        .statement("INSERT INTO a VALUES (DATE '1992-01-01', 0.3, NOW());")
        .statement("INSERT INTO b SELECT * FROM range(0,10);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE TABLE a(d DATE, k FLOAT, t TIMESTAMP);')

def test_dump_blobs(shell):
    test = (
        ShellTest(shell)
        .statement("create table test(t VARCHAR, b BLOB);")
        .statement(".changes off")
        .statement("insert into test values('literal blob', '\\x07\\x08\\x09');")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout("'\\x07\\x08\\x09'")

def test_dump_newline(shell):
    test = (
        ShellTest(shell)
        .statement("create table newline_data as select concat(chr(10), '\n') s;")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout("concat")
    result.check_stdout("chr(10)")

def test_dump_indexes(shell):
    test = (
        ShellTest(shell)
        .statement("create table integer(i int);")
        .statement("create index i_index on integer(i);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout("CREATE INDEX i_index")

def test_dump_views(shell):
    test = (
        ShellTest(shell)
        .statement("create table integer(i int);")
        .statement("create view v1 as select * from integer;")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout("CREATE VIEW v1")

@pytest.mark.parametrize("pattern", [None, "nested index"])
def test_dump_nested_schema_path(shell, tmp_path, pattern):
    source_database = tmp_path / "nested source.db"
    restored_database = tmp_path / "nested restored.db"
    create = (
        ShellTest(shell, [str(source_database)])
        .statement('CREATE SCHEMA "parent.schema"')
        .statement('CREATE SCHEMA "parent.schema"."child""schema"')
        .statement(
            'CREATE SCHEMA "parent.schema"."child""schema"."grand child"'
        )
        .statement(
            'CREATE TABLE "parent.schema"."child""schema"."grand child".'
            '"nested table"(i INTEGER)'
        )
        .statement(
            'CREATE INDEX "nested index" ON '
            '"parent.schema"."child""schema"."grand child"."nested table"(i)'
        )
        .statement(
            'INSERT INTO "parent.schema"."child""schema"."grand child".'
            '"nested table" VALUES (42)'
        )
    )
    create_result = create.run()
    create_result.check_stdout(None)
    create_result.check_stderr(None)

    dump_command = ".dump" if pattern is None else f".dump {pattern}"
    dump = ShellTest(shell, [str(source_database)]).statement(dump_command).run()
    schema_path = '"parent.schema"."child""schema"."grand child"'
    assert f"CREATE SCHEMA IF NOT EXISTS {schema_path};" in dump.stdout
    assert f'CREATE TABLE {schema_path}."nested table"' in dump.stdout
    assert f'CREATE INDEX "nested index" ON {schema_path}."nested table"' in dump.stdout
    assert f'INSERT INTO {schema_path}."nested table" VALUES(42);' in dump.stdout
    assert ";;" not in dump.stdout
    assert '"nested source".' not in dump.stdout

    restore_sql = (
        dump.stdout
        + f'\nSELECT * FROM {schema_path}."nested table";\n'
        + "SELECT count(*) FROM duckdb_indexes() "
        + "WHERE index_name = 'nested index' AND table_name = 'nested table';\n"
    )
    restored = ShellTest(shell, [str(restored_database)]).run_raw(restore_sql)
    restored.check_stdout("42\n1")
    restored.check_stderr(None)


def test_dump_schema_qualified(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE SCHEMA other;")
        .statement("CREATE TABLE other.t_in_other(a INT);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS other;')
    result.check_stdout('CREATE TABLE other.t_in_other(a INTEGER);')
    result.check_stdout('COMMIT')

def test_dump_schema_with_data(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE SCHEMA test_schema;")
        .statement("CREATE TABLE test_schema.tbl(x INT, y VARCHAR);")
        .statement(".changes off")
        .statement("INSERT INTO test_schema.tbl VALUES (1, 'hello'), (2, 'world');")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS test_schema;')
    result.check_stdout('CREATE TABLE test_schema.tbl(x INTEGER, y VARCHAR);')
    result.check_stdout("INSERT INTO test_schema.tbl VALUES(1,'hello');")
    result.check_stdout('COMMIT')

def test_dump_multiple_schemas(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE SCHEMA s1;")
        .statement("CREATE SCHEMA s2;")
        .statement("CREATE TABLE s1.t1(a INT);")
        .statement("CREATE TABLE s2.t2(b INT);")
        .statement(".changes off")
        .statement("INSERT INTO s1.t1 VALUES (10);")
        .statement("INSERT INTO s2.t2 VALUES (20);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS s1;')
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS s2;')
    result.check_stdout('INSERT INTO s1.t1 VALUES(10);')
    result.check_stdout('INSERT INTO s2.t2 VALUES(20);')

def test_dump_quoted_schema(shell):
    test = (
        ShellTest(shell)
        .statement('CREATE SCHEMA "my-schema";')
        .statement('CREATE TABLE "my-schema"."my-table"(a INT);')
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS "my-schema";')
    result.check_stdout('CREATE TABLE "my-schema"."my-table"(a INTEGER);')

def test_dump_if_not_exists(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE SCHEMA other;")
        .statement("CREATE TABLE IF NOT EXISTS other.tbl(x INT);")
        .statement(".changes off")
        .statement("INSERT INTO other.tbl VALUES (42);")
        .statement(".dump")
    )
    result = test.run()
    result.check_stdout('CREATE SCHEMA IF NOT EXISTS other;')
    result.check_stdout('INSERT INTO other.tbl VALUES(42);')
    result.check_stdout('COMMIT')
