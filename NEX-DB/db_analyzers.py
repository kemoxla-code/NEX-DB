# db_analyzers.py
import sqlite3
from abc import ABC, abstractmethod

DB_ANALYZERS: list[type["BaseDBAnalyzer"]] = []

def register_db(cls: type["BaseDBAnalyzer"]) -> type["BaseDBAnalyzer"]:
    DB_ANALYZERS.append(cls)
    return cls

class BaseDBAnalyzer(ABC):
    @abstractmethod
    def run(self, db_path: str, **kwargs) -> list[dict]:
        pass

@register_db
class ConnectionErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        try:
            conn = sqlite3.connect(db_path, timeout=1)
            conn.execute("PRAGMA schema_version;")
            conn.close()
        except sqlite3.OperationalError as e:
            issues.append({"stage":"Connection","error":"ConnectionError",
                           "message":str(e),"context":db_path})
        except sqlite3.DatabaseError as e:
            issues.append({"stage":"Connection","error":"DatabaseError",
                           "message":str(e),"context":db_path})
        return issues

@register_db
class SQLSyntaxAnalyzer(BaseDBAnalyzer):
    TEST_QUERIES = [
        "SELEC 1",
        "SELECT * FROM nonexistent_table;",
        "SELECT 'text' + 5;",
        "SELECT SUM(amount) FROM sales;"
    ]
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        for sql in kwargs.get("scripts", self.TEST_QUERIES):
            try:
                cur.execute(sql)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "syntax error" in msg: err="SyntaxError"
                elif "no such table" in msg: err="MissingTableOrColumn"
                elif "datatype mismatch" in msg: err="TypeError"
                elif "aggregate" in msg: err="AggregationError"
                else: err="OperationalError"
                issues.append({"stage":"SQLSyntax","error":err,
                               "message":str(e),"context":sql})
        conn.close()
        return issues

@register_db
class ConstraintsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON;")
        cur = conn.cursor()
        cur.execute("PRAGMA quick_check;")
        for row in cur.fetchall():
            t = row[0] or ""
            if "failed" in t.lower():
                issues.append({"stage":"Constraints","error":"IntegrityCheckFailed",
                               "message":t,"context":"PRAGMA quick_check"})
        conn.close()
        return issues

@register_db
class OperationalErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA integrity_check;")
            conn.close()
        except sqlite3.DatabaseError as e:
            msg = str(e).lower()
            if "disk i/o error" in msg: err="DiskIOError"
            elif "out of memory" in msg: err="OutOfMemory"
            else: err="DatabaseError"
            issues.append({"stage":"Operational","error":err,
                           "message":str(e),"context":db_path})
        return issues

@register_db
class StructuralErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA integrity_check;")
            for row in cur.fetchall():
                m = row[0] or ""
                if "malformed" in m.lower():
                    issues.append({"stage":"Structural","error":"MalformedDatabase",
                                   "message":m,"context":"PRAGMA integrity_check"})
        except sqlite3.DatabaseError as e:
            issues.append({"stage":"Structural","error":"IntegrityCheckError",
                           "message":str(e),"context":"PRAGMA integrity_check"})
        conn.close()
        return issues

@register_db
class TransactionErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("BEGIN;")
            try: conn.execute("BEGIN;")
            except sqlite3.OperationalError as e:
                issues.append({"stage":"Transaction","error":"NestedTransactionError",
                               "message":str(e),"context":"BEGIN within BEGIN"})
            try: conn.execute("VACUUM;")
            except sqlite3.OperationalError as e:
                issues.append({"stage":"Transaction","error":"VacuumInTransactionError",
                               "message":str(e),"context":"VACUUM"})
            conn.execute("COMMIT;")
        finally:
            conn.close()
        return issues

@register_db
class ComplexQueryErrorsAnalyzer(BaseDBAnalyzer):
    TEST_COMPLEX = [
        "SELECT * FROM (SELECT 1 UNION ALL SELECT 2) t1, (SELECT 3 UNION ALL SELECT 4) t2;",
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt LIMIT 200000) SELECT * FROM cnt;"
    ]
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        for sql in kwargs.get("tests", self.TEST_COMPLEX):
            try:
                cur.execute(sql)
                cur.fetchall()
            except sqlite3.OperationalError as e:
                issues.append({"stage":"ComplexQuery","error":"ComplexQueryError",
                               "message":str(e),"context":sql})
        conn.close()
        return issues

@register_db
class IndexAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("EXPLAIN QUERY PLAN SELECT * FROM sqlite_master WHERE type='table';")
        for row in cur.fetchall():
            d = " ".join(str(x) for x in row)
            if "SCAN TABLE" in d and "USING INDEX" not in d:
                issues.append({"stage":"Index","error":"UnindexedQuery",
                               "message":d,"context":"sqlite_master query"})
        conn.close()
        return issues

@register_db
class MaintenanceErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("VACUUM;")
        except sqlite3.OperationalError as e:
            issues.append({"stage":"Maintenance","error":"VacuumError",
                           "message":str(e),"context":"VACUUM"})
        conn.close()
        return issues

@register_db
class ExtensionErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        try:
            conn.enable_load_extension(True)
            conn.load_extension("nonexistent_extension")
        except sqlite3.OperationalError as e:
            issues.append({"stage":"Extension","error":"LoadExtensionError",
                           "message":str(e),"context":"load_extension()"})
        finally:
            conn.close()
        return issues

@register_db
class DesignLogicErrorsAnalyzer(BaseDBAnalyzer):
    def run(self, db_path: str, **kwargs) -> list[dict]:
        issues = []
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
        for tbl, sql in cur.fetchall():
            if "PRIMARY KEY" not in sql.upper():
                issues.append({"stage":"DesignLogic","error":"MissingPrimaryKey",
                               "message":f"table `{tbl}` has no PRIMARY KEY",
                               "context":tbl})
        conn.close()
        return issues


def run_all_db(db_path: str, **kwargs) -> list[dict]:
    results = []
    for Analyzer in DB_ANALYZERS:
        results.extend(Analyzer().run(db_path, **kwargs))
    return results
