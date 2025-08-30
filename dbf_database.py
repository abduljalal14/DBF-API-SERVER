from dbfread import DBF
from typing import List, Dict, Any, Optional
import os


def _normalize_key(val):
    """Konversi nilai join ke bentuk string seragam, hilangkan leading zero kalau numeric"""
    if val is None:
        return None
    try:
        # coba konversi ke integer biar "00015" == "15"
        return str(int(str(val).strip()))
    except ValueError:
        # kalau bukan angka, kembalikan string strip biasa
        return str(val).strip()


class DBFTable:
    def __init__(self, filepath: str):
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File DBF tidak ditemukan: {filepath}")
        self.filepath = filepath
        self.table = DBF(filepath, load=False, encoding='latin1')

    def count(self) -> int:
        """Hitung jumlah record dalam tabel"""
        return len(self.table)

    def select(
        self,
        columns: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Query sederhana mirip SQL:
        SELECT columns FROM table WHERE condition LIMIT x OFFSET y
        """
        result = []
        count = 0

        for i, record in enumerate(self.table):
            # OFFSET
            if i < offset:
                continue

            # WHERE
            if where:
                match = True
                for k, v in where.items():
                    if _normalize_key(record.get(k)) != _normalize_key(v):
                        match = False
                        break
                if not match:
                    continue

            # Pilih kolom tertentu
            if columns:
                row = {col: record.get(col) for col in columns}
            else:
                row = dict(record)

            result.append(row)

            count += 1
            if limit and count >= limit:
                break

        return result

    def join(
        self,
        other: "DBFTable",
        left_key: str,
        right_key: str,
        columns: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Join sederhana (INNER JOIN):
        SELECT ... FROM self
        JOIN other ON self[left_key] = other[right_key]
        """
        left_data = self.select(where=where, limit=limit, offset=offset)
        return DBFTable.join_records(left_data, other, left_key, right_key, columns)

    @staticmethod
    def join_records(
        records: List[Dict[str, Any]],
        other: "DBFTable",
        left_key: str,
        right_key: str,
        columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Join hasil query (list of dict) dengan tabel lain
        Bisa digunakan untuk multi-join
        """
        # index data kanan
        right_data = {_normalize_key(r.get(right_key)): r for r in other.select()}

        result = []
        for row in records:
            join_key = _normalize_key(row.get(left_key))
            match = right_data.get(join_key)
            if match:
                joined_row = dict(row)
                for k, v in match.items():
                    if k not in joined_row:
                        joined_row[k] = v
                if columns:
                    joined_row = {col: joined_row.get(col) for col in columns}
                result.append(joined_row)
        return result


class DBFDatabase:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.tables = {}

    def register_table(self, name: str, filename: str):
        filepath = os.path.join(self.base_path, filename)
        self.tables[name] = DBFTable(filepath)

    def table(self, name: str) -> DBFTable:
        if name not in self.tables:
            raise ValueError(f"Tabel {name} belum terdaftar")
        return self.tables[name]
