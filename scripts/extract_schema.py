#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Script to extract the schema from SQLite database files."""

import argparse
import logging
import os
import sqlite3
import sys
import textwrap


class ColumnDefinition(object):
  """Column definition.

  Attributes:
    name (str): name of the column.
    value_type (str): value type of the column.
  """

  def __init__(self):
    """Initializes a column definition."""
    super(ColumnDefinition, self).__init__()
    self.name = None
    self.value_type = None


class SQLiteSchemaExtractor(object):
  """SQLite database file schema extractor."""

  _SCHEMA_QUERY = (
      'SELECT tbl_name, sql '
      'FROM sqlite_master '
      'WHERE type = "table" AND tbl_name != "xp_proc" '
      'AND tbl_name != "sqlite_sequence"')

  def _FormatSchemaAsText(self, schema):
    """Formats a schema into a word-wrapped string.

    Args:
      schema (dict[str, str]): schema as an SQL query per table name.

    Returns:
      str: schema formatted as word-wrapped string.
    """
    textwrapper = textwrap.TextWrapper()
    textwrapper.break_long_words = False
    textwrapper.drop_whitespace = True
    textwrapper.width = 80 - (10 + 4)

    lines = []
    table_index = 1
    number_of_tables = len(schema)
    for table_name, query in sorted(schema.items()):
      line = f'      \'{table_name:s}\': ('
      lines.append(line)

      query = query.replace('\'', '\\\'')
      query = textwrapper.wrap(query)
      query = [f'          \'{line:s} \'' for line in query]

      last_line = query[-1]
      if table_index == number_of_tables:
        query[-1] = ''.join([last_line[:-2], '\')}}]'])
      else:
        query[-1] = ''.join([last_line[:-2], '\'),'])

      lines.extend(query)
      table_index += 1

    return '\n'.join(lines)

  def _FormatSchemaAsYAML(self, schema):
    """Formats a schema into YAML.

    Args:
      schema (dict[str, str]): schema as an SQL query per table name.

    Returns:
      str: schema formatted as YAML.

    Raises:
      RuntimeError: if a query could not be parsed.
    """
    lines = ['# SQLite-kb database schema.']

    for table_name, query in sorted(schema.items()):
      query_start = f'CREATE TABLE {table_name:s} ('
      if not query.startswith(query_start) or query[-1] != ')':
        raise RuntimeError('Unsupported query.')

      query = query[len(query_start):-1]

      column_definitions = {}
      while query:
        if query.startswith('UNIQUE'):
          # TODO: set unique status in column definition.
          break

        if query.startswith('PRIMARY KEY'):
          # TODO: set primary key status in column definition.
          break

        column, _, query = query.partition(',')
        query = query.lstrip()

        column_segments = column.split(' ')
        column_name = column_segments[0]

        if column_name in column_definitions:
          raise RuntimeError(f'Column: {column_name:s} already defined.')

        column_definition = ColumnDefinition()
        column_definition.name = column_name
        column_definition.value_type = column_segments[1]

        column_definitions[column_name] = column_definition

      lines.extend([
          '---',
          f'table: {table_name:s}',
          'columns:'])

      for column_definition in column_definitions.values():
        lines.extend([
            f'- name: {column_definition.name:s}',
            f'  value_type: {column_definition.value_type:s}'])

    return '\n'.join(lines)

  def FormatSchema(self, schema, output_format):
    """Formats a schema into a word-wrapped string.

    Args:
      schema (dict[str, str]): schema as an SQL query per table name.
      output_format (str): output format.

    Returns:
      str: formatted schema.

    Raises:
      RuntimeError: if a query could not be parsed.
    """
    if output_format == 'text':
      return self._FormatSchemaAsText(schema)

    if output_format == 'yaml':
      return self._FormatSchemaAsYAML(schema)

  def GetDatabaseSchema(self, database_path):
    """Retrieves schema from given database.

    Args:
      database_path (str): file path to database.

    Returns:
      dict[str, str]: schema as an SQL query per table name or None if
          the schema could not be retrieved.
    """
    schema = None

    database = sqlite3.connect(database_path)
    database.row_factory = sqlite3.Row

    try:
      cursor = database.cursor()

      rows = cursor.execute(self._SCHEMA_QUERY)

      schema = {
          table_name: ' '.join(query.split()) for table_name, query in rows}

    except sqlite3.DatabaseError as exception:
      logging.error(f'Unable to query schema with error: {exception!s}')

    finally:
      database.close()

    return schema


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

  extractor = SQLiteSchemaExtractor()

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
