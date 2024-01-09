#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the SQLite database file schema extractor."""

import unittest

from sqliterc import schema_extractor

from tests import test_lib


class SQLiteSchemaExtractorTest(test_lib.BaseTestCase):
  """Tests for the SQLite database file schema extractor."""

  # pylint: disable=protected-access

  def testInitialize(self):
    """Tests the __init__ function."""
    # TODO: pass artifact definitions path.
    test_extractor = schema_extractor.SQLiteSchemaExtractor(None)
    self.assertIsNotNone(test_extractor)

  # TODO: add tests for _CheckSignature
  # TODO: add tests for _FormatSchemaAsText
  # TODO: add tests for _FormatSchemaAsYAML
  # TODO: add tests for _GetDatabaseSchema
  # TODO: add tests for _GetDatabaseIdentifier
  # TODO: add tests for _GetDatabaseSchemaFromFileObject
  # TODO: add tests for GetDisplayPath
  # TODO: add tests for ExtractSchemas
  # TODO: add tests for FormatSchema


if __name__ == '__main__':
  unittest.main()
