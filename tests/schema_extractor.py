#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the SQLite database file schema extractor."""

import io
import os
import unittest

import artifacts

from sqliterc import schema_extractor

from tests import test_lib


class SQLiteSchemaExtractorTest(test_lib.BaseTestCase):
  """Tests for the SQLite database file schema extractor."""

  # pylint: disable=protected-access

  _ARTIFACT_DEFINITIONS_PATH = os.path.join(
        os.path.dirname(artifacts.__file__), 'data')
  if not os.path.isdir(_ARTIFACT_DEFINITIONS_PATH):
    _ARTIFACT_DEFINITIONS_PATH = os.path.join(
        '/', 'usr', 'share', 'artifacts')

  def testInitialize(self):
    """Tests the __init__ function."""
    test_extractor = schema_extractor.SQLiteSchemaExtractor(
        self._ARTIFACT_DEFINITIONS_PATH)
    self.assertIsNotNone(test_extractor)

  def testCheckSignature(self):
    """Tests the _CheckSignature function."""
    test_extractor = schema_extractor.SQLiteSchemaExtractor(
        self._ARTIFACT_DEFINITIONS_PATH)

    file_object = io.BytesIO(b'SQLite format 3\x00')
    result = test_extractor._CheckSignature(file_object)
    self.assertTrue(result)

    file_object = io.BytesIO(b'\xff' * 16)
    result = test_extractor._CheckSignature(file_object)
    self.assertFalse(result)

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
