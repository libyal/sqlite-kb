#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the SQLite database resources."""

import unittest

from sqliterc import resources

from tests import test_lib


class ColumnDefinitionTest(test_lib.BaseTestCase):
  """Tests for the column definition."""

  def testInitialize(self):
    """Tests the __init__ function."""
    test_definition = resources.ColumnDefinition()
    self.assertIsNotNone(test_definition)


class DatabaseDefinitionTest(test_lib.BaseTestCase):
  """Tests for the database definition."""

  def testInitialize(self):
    """Tests the __init__ function."""
    test_definition = resources.DatabaseDefinition()
    self.assertIsNotNone(test_definition)


if __name__ == '__main__':
  unittest.main()
