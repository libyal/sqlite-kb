#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Script to extract the schema from SQLite database files."""

import argparse
import os
import sys

from sqliterc import schema_extractor


def Main():
  """The main program function.

  Returns:
    bool: True if successful or False if not.
  """
  argument_parser = argparse.ArgumentParser(description=(
      'Extract the schema from a SQLite database file.'))

  argument_parser.add_argument(
      '--format', dest='format', action='store', type=str,
      choices=['text', 'yaml'], default='text', metavar='FORMAT',
      help='output format.')

  argument_parser.add_argument(
      'database_path', type=str,
      help='path to the database file to extract schema from.')

  options = argument_parser.parse_args()

  if not os.path.exists(options.database_path):
    print(f'No such database file: {options.database_path:s}')
    return False

  extractor = schema_extractor.SQLiteSchemaExtractor()

  database_schema = extractor.GetDatabaseSchema(options.database_path)
  if not database_schema:
    print((f'Unable to determine schema from database file: '
           f'{options.database_path:s}'))
    return False

  output_text = extractor.FormatSchema(database_schema, options.format)

  print(output_text)

  return True


if __name__ == '__main__':
  if not Main():
    sys.exit(1)
  else:
    sys.exit(0)
