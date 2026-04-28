#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Small table helpers for TSV-backed transfer metadata.

This intentionally implements only the DataFrame-like surface used by the
operator path, so the core CLI does not need pandas on locked-down systems.
"""

from collections import OrderedDict


class BoolMask:
    """Boolean mask with pandas-like combination operators."""

    def __init__(self, values):
        self.values = [bool(value) for value in values]

    def __iter__(self):
        return iter(self.values)

    def __len__(self):
        return len(self.values)

    def __invert__(self):
        return BoolMask([not value for value in self.values])

    def __and__(self, other):
        other_values = list(other)
        return BoolMask([
            left and bool(right)
            for left, right in zip(self.values, other_values)
        ])

    def __or__(self, other):
        other_values = list(other)
        return BoolMask([
            left or bool(right)
            for left, right in zip(self.values, other_values)
        ])

    def any(self):
        return any(self.values)

    def all(self):
        return all(self.values)


class Series:
    """Minimal column vector used by TransferTable."""

    def __init__(self, values):
        self._values = list(values)

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __getitem__(self, index):
        return self._values[index]

    def __eq__(self, other):
        return BoolMask([value == other for value in self._values])

    def __ne__(self, other):
        return BoolMask([value != other for value in self._values])

    @property
    def values(self):
        return list(self._values)

    def tolist(self):
        return list(self._values)

    def unique(self):
        seen = OrderedDict()
        for value in self._values:
            seen.setdefault(value, None)
        return list(seen.keys())

    def nunique(self):
        return len(self.unique())

    def dropna(self):
        return Series([
            value for value in self._values
            if value is not None and str(value) != "nan"
        ])

    def apply(self, func):
        return Series([func(value) for value in self._values])

    def isin(self, values):
        values = set(values)
        return BoolMask([value in values for value in self._values])

    def any(self):
        return any(self._values)

    def all(self):
        return all(self._values)


class _ILoc:
    def __init__(self, table):
        self._table = table

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._table._rows[key]
        if isinstance(key, slice):
            return TransferTable(
                self._table._rows[key],
                columns=self._table.columns,
                attrs=dict(self._table.attrs),
            )
        if isinstance(key, (list, tuple)):
            return TransferTable(
                [self._table._rows[index] for index in key],
                columns=self._table.columns,
                attrs=dict(self._table.attrs),
            )
        raise TypeError("Unsupported iloc key: {0!r}".format(key))


class _Loc:
    def __init__(self, table):
        self._table = table

    def __setitem__(self, key, value):
        row_key, column = key
        self._table._rows[row_key][column] = value
        if column not in self._table.columns:
            self._table.columns.append(column)


class _GroupBy:
    def __init__(self, table, by):
        self._groups = OrderedDict()
        self.groups = OrderedDict()
        keys = [by] if isinstance(by, str) else list(by)
        for index, row in enumerate(table._rows):
            key_values = tuple(row.get(key, "") for key in keys)
            group_key = key_values[0] if len(key_values) == 1 else key_values
            self._groups.setdefault(group_key, []).append(row)
            self.groups.setdefault(group_key, []).append(index)
        self._columns = list(table.columns)

    def __iter__(self):
        for key, rows in self._groups.items():
            yield key, TransferTable(rows, columns=self._columns)

    def __len__(self):
        return len(self._groups)


class TransferTable:
    """Record-backed table with the small DataFrame surface used by the CLI."""

    def __init__(self, rows=None, columns=None, attrs=None):
        self._rows = [dict(row) for row in (rows or [])]
        self.columns = list(columns or self._infer_columns(self._rows))
        self.attrs = dict(attrs or {})

    @staticmethod
    def _infer_columns(rows):
        columns = []
        for row in rows:
            for column in row:
                if column not in columns:
                    columns.append(column)
        return columns

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, key):
        if isinstance(key, str):
            return Series([row.get(key, "") for row in self._rows])
        if isinstance(key, BoolMask):
            key = list(key)
        if isinstance(key, Series):
            key = key.tolist()
        if isinstance(key, (list, tuple)) and all(isinstance(item, bool) for item in key):
            return TransferTable(
                [row for row, keep in zip(self._rows, key) if keep],
                columns=self.columns,
                attrs=dict(self.attrs),
            )
        raise TypeError("Unsupported table key: {0!r}".format(key))

    def __setitem__(self, key, values):
        if key not in self.columns:
            self.columns.append(key)
        if isinstance(values, Series):
            values = values.tolist()
        if isinstance(values, (list, tuple)):
            if len(values) != len(self._rows):
                raise ValueError("Column length does not match table length")
            for row, value in zip(self._rows, values):
                row[key] = value
            return
        for row in self._rows:
            row[key] = values

    @property
    def empty(self):
        return len(self._rows) == 0

    @property
    def iloc(self):
        return _ILoc(self)

    @property
    def loc(self):
        return _Loc(self)

    def copy(self):
        return TransferTable(self._rows, columns=self.columns, attrs=dict(self.attrs))

    def iterrows(self):
        for index, row in enumerate(self._rows):
            yield index, row

    def groupby(self, by, dropna=True):
        return _GroupBy(self, by)

    def to_rows(self):
        return [dict(row) for row in self._rows]
