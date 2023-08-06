# -*- coding: utf-8 -*-
"""SQLite database file schema extractor."""

import logging
import os
import sqlite3
import tempfile
import textwrap

from dfimagetools import definitions as dfimagetools_definitions
from dfimagetools import file_entry_lister


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

  _READ_BUFFER_SIZE = 16 * 1024 * 1024

  _SCHEMA_QUERY = (
      'SELECT tbl_name, sql '
      'FROM sqlite_master '
      'WHERE type = "table" AND tbl_name != "xp_proc" '
      'AND tbl_name != "sqlite_sequence"')

  def __init__(self, mediator=None):
    """Initializes a SQLite database file schema extractor.

    Args:
      mediator (Optional[dfvfs.VolumeScannerMediator]): a volume scanner
          mediator.
    """
    super(SQLiteSchemaExtractor, self).__init__()
    self._mediator = mediator

  def _CheckSignature(self, file_object):
    """Checks the signature of a given database file-like object.

    Args:
      file_object (dfvfs.FileIO): file-like object of the database.

    Returns:
      bool: True if the signature matches that of a SQLite database, False
          otherwise.
    """
    if not file_object:
      return False

    file_object.seek(0, os.SEEK_SET)
    file_data = file_object.read(16)
    return file_data == b'SQLite format 3\x00'

  def _GetDisplayPath(self, path_segments, data_stream_name):
    """Retrieves a path to display.

    Args:
      path_segments (list[str]): path segments of the full path of the file
          entry.
      data_stream_name (str): name of the data stream.

    Returns:
      str: path to display.
    """
    display_path = ''

    path_segments = [
        segment.translate(
            dfimagetools_definitions.NON_PRINTABLE_CHARACTER_TRANSLATION_TABLE)
        for segment in path_segments]
    display_path = ''.join([display_path, '/'.join(path_segments)])

    if data_stream_name:
      data_stream_name = data_stream_name.translate(
          dfimagetools_definitions.NON_PRINTABLE_CHARACTER_TRANSLATION_TABLE)
      display_path = ':'.join([display_path, data_stream_name])

    return display_path or '/'

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
      # Note that the space between the table name and ( is optional.
      query_start = f'CREATE TABLE {table_name:s}'
      if not query.startswith(query_start) or query[-1] != ')':
        raise RuntimeError(f'Unsupported query: "{query:s}"')

      for query_index in range(len(query_start), len(query)):
        if query[query_index] == '(':
          query_index += 1
          break

      # Note that there can be a space between ( and the column definition.
      query = query[query_index:-1].lstrip()

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

        # Note that a column definition can be defined without a type.
        if len(column_segments) > 1:
          column_definition.value_type = column_segments[1]

        column_definitions[column_name] = column_definition

      lines.extend([
          '---',
          f'table: {table_name:s}',
          'columns:'])

      for column_definition in column_definitions.values():
        lines.append(f'- name: {column_definition.name:s}')
        if column_definition.value_type:
          lines.append(f'  value_type: {column_definition.value_type:s}')

    lines.append('')
    return '\n'.join(lines)

  def _GetDatabaseSchemaFromFileObject(self, file_object):
    """Retrieves schema from given database file-like object.

    Args:
      file_object (dfvfs.FileIO): file-like object of the database.

    Returns:
      dict[str, str]: schema as an SQL query per table name or None if
          the schema could not be retrieved.
    """
    # TODO: find an alternative solution that can read a SQLite database
    # directly from a file-like object.
    with tempfile.NamedTemporaryFile(delete=True) as temporary_file:
      file_object.seek(0, os.SEEK_SET)
      file_data = file_object.read(self._READ_BUFFER_SIZE)
      while file_data:
        temporary_file.write(file_data)
        file_data = file_object.read(self._READ_BUFFER_SIZE)

      return self.GetDatabaseSchema(temporary_file.name)

  def ExtractSchemas(self, path, options=None):
    """Extracts database schemas from the path.

    Args:
      path (str): path of a SQLite 3 database file or storage media image
          containing SQLite 3 database files.
      options (Optional[dfvfs.VolumeScannerOptions]): volume scanner options. If
          None the default volume scanner options are used, which are defined in
          the dfVFS VolumeScannerOptions class.

    Yields:
      tuple[str, dict[str, str]]: path segments and schema as an SQL query per
          table name.
    """
    entry_lister = file_entry_lister.FileEntryLister(mediator=self._mediator)

    base_path_specs = entry_lister.GetBasePathSpecs(path, options=options)
    if not base_path_specs:
      database_schema = self.GetDatabaseSchema(path)
      if not database_schema:
        logging.warning(
            f'Unable to determine schema from database file: {path:s}')
      else:
        path_segments = os.path.split(path)
        yield path_segments, database_schema

    else:
      for file_entry, path_segments in entry_lister.ListFileEntries(
          base_path_specs):
        if file_entry.size < 16:
          continue

        file_object = file_entry.GetFileObject()
        if not self._CheckSignature(file_object):
          continue

        display_path = self._GetDisplayPath(path_segments, '')
        logging.info(f'Extracting schema from database file: {display_path:s}')

        database_schema = self._GetDatabaseSchemaFromFileObject(file_object)
        if not database_schema:
          logging.warning((
              f'Unable to determine schema from database file: '
              f'{display_path:s}'))
          continue

        yield path_segments, database_schema

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

    raise RuntimeError(f'Unsupported output format: {output_format:s}')

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
