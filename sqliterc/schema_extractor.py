# -*- coding: utf-8 -*-
"""SQLite database file schema extractor."""

import logging
import os
import re
import sqlite3
import tempfile
import textwrap

from artifacts import definitions as artifacts_definitions
from artifacts import reader as artifacts_reader
from artifacts import registry as artifacts_registry

from dfimagetools import definitions as dfimagetools_definitions
from dfimagetools import file_entry_lister

from sqliterc import resources
from sqliterc import yaml_definitions_file


class SQLiteSchemaExtractor(object):
  """SQLite database file schema extractor."""

  _DATABASE_DEFINITIONS_FILE = (
      os.path.join(os.path.dirname(__file__), 'data', 'known_databases.yaml'))

  _READ_BUFFER_SIZE = 16 * 1024 * 1024

  _SCHEMA_QUERY = (
      'SELECT tbl_name, sql '
      'FROM sqlite_master '
      'WHERE type = "table" AND tbl_name != "xp_proc" '
      'AND tbl_name != "sqlite_sequence"')

  def __init__(
      self, artifact_definitions, data_location, mediator=None):
    """Initializes a SQLite database file schema extractor.

    Args:
      artifact_definitions (str): path to a single artifact definitions
          YAML file or a directory of definitions YAML files.
      data_location (str): path to the SQLite-kb data files.
      mediator (Optional[dfvfs.VolumeScannerMediator]): a volume scanner
          mediator.
    """
    super(SQLiteSchemaExtractor, self).__init__()
    self._artifacts_registry = artifacts_registry.ArtifactDefinitionsRegistry()
    self._data_location = data_location
    self._known_database_definitions = {}
    self._mediator = mediator

    reader = artifacts_reader.YamlArtifactsReader()
    if os.path.isdir(artifact_definitions):
      self._artifacts_registry.ReadFromDirectory(reader, artifact_definitions)
    elif os.path.isfile(artifact_definitions):
      self._artifacts_registry.ReadFromFile(reader, artifact_definitions)

    definitions_file = yaml_definitions_file.YAMLDatabaseDefinitionsFile()
    for database_definition in definitions_file.ReadFromFile(
        self._DATABASE_DEFINITIONS_FILE):
      artifact_definition = self._artifacts_registry.GetDefinitionByName(
          database_definition.artifact_definition)
      if not artifact_definition:
        logging.warning((f'Unknown artifact definition: '
                         f'{database_definition.artifact_definition:s}'))
      else:
        self._known_database_definitions[
            database_definition.database_identifier] = artifact_definition

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

      # Replace \t and \n by a space.
      query = re.sub(r'[\n\t]+', r' ', query, count=0)
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
      original_query = query

      # Replace \t by a space.
      query = re.sub(r'[\t]+', r' ', query, count=0)

      if not query.startswith('CREATE ') or query[-1] != ')':
        raise RuntimeError(f'Unsupported query: "{original_query:s}"')

      query = query[7:-1]

      if query.startswith('VIRTUAL '):
        continue

      if not query.startswith('TABLE '):
        raise RuntimeError(f'Unsupported query: "{original_query:s}"')

      query = query[6:]

      if query[0] == '\'':
        query_start = f'\'{table_name:s}\''
      elif query[0] == '"':
        query_start = f'"{table_name:s}"'
      elif query[0] == '`':
        query_start = f'`{table_name:s}`'
      elif query[0] == '[':
        query_start = f'[{table_name:s}]'
      else:
        query_start = table_name

      if not query.startswith(query_start):
        raise RuntimeError(f'Unsupported query: "{original_query:s}"')

      # Note that there can be a space between table name and "(".
      query = query[len(query_start):].lstrip()

      if not query[0] == '(':
        raise RuntimeError(f'Unsupported query: "{original_query:s}"')

      # Note that there can be a space between "(" and the column definition.
      query = query[1:].lstrip()

      column_definitions = {}
      while query:
        # Strip comments.
        if query.startswith('-- '):
          _, _, query = query.partition('\n')
          query = query.lstrip()

        if query.startswith('CONSTRAINT'):
          break

        if query.startswith('UNIQUE'):
          # TODO: set unique status in column definition.
          break

        if query.startswith('PRIMARY KEY'):
          # TODO: set primary key status in column definition.
          break

        # TODO: handle CONSTRAINT

        column, _, query = query.partition(',')
        query = query.lstrip()

        column_segments = column.split(' ')
        column_name = column_segments[0]
        if column_name[0] in ('\'', '"', '`', '['):
          column_name = column_name[1:-1]

        if column_name in column_definitions:
          raise RuntimeError(f'Column: {column_name:s} already defined.')

        column_definition = resources.ColumnDefinition()
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

  def _GetDatabaseSchema(self, database_path):
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

      schema = dict(rows)

    except sqlite3.DatabaseError as exception:
      logging.error(f'Unable to query schema with error: {exception!s}')

    finally:
      database.close()

    return schema

  def _GetDatabaseIdentifier(self, path_segments):
    """Determines the database identifier.

    Args:
      path_segments (list[str]): path segments.

    Returns:
      str: database identifier or None if the type could not be determined.
    """
    # TODO: make comparison more efficient.
    for database_identifier, artifact_definition in (
        self._known_database_definitions.items()):
      for source in artifact_definition.sources:
        if source.type_indicator in (
            artifacts_definitions.TYPE_INDICATOR_DIRECTORY,
            artifacts_definitions.TYPE_INDICATOR_FILE,
            artifacts_definitions.TYPE_INDICATOR_PATH):
          for source_path in set(source.paths):
            source_path_segments = source_path.split(source.separator)

            if not source_path_segments[0]:
              source_path_segments = source_path_segments[1:]

            # TODO: add support for parameters.
            last_index = len(source_path_segments)
            for index in range(1, last_index + 1):
              source_path_segment = source_path_segments[-index]
              if not source_path_segment or len(source_path_segment) < 2:
                continue

              if (source_path_segment[0] == '%' and
                  source_path_segment[-1] == '%'):
                source_path_segments = source_path_segments[-index + 1:]
                break

            if len(source_path_segments) > len(path_segments):
              continue

            is_match = True
            last_index = min(len(source_path_segments), len(path_segments))
            for index in range(1, last_index + 1):
              source_path_segment = source_path_segments[-index]
              # TODO: improve handling of *
              if '*' in source_path_segment:
                continue

              path_segment = path_segments[-index].lower()
              source_path_segment = source_path_segment.lower()

              is_match = path_segment == source_path_segment
              if not is_match:
                break

            if is_match:
              return database_identifier

    return None

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

      return self._GetDatabaseSchema(temporary_file.name)

  def GetDisplayPath(self, path_segments, data_stream_name=None):
    """Retrieves a path to display.

    Args:
      path_segments (list[str]): path segments of the full path of the file
          entry.
      data_stream_name (Optional[str]): name of the data stream.

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

  def ExtractSchemas(self, path, options=None):
    """Extracts database schemas from the path.

    Args:
      path (str): path of a SQLite 3 database file or storage media image
          containing SQLite 3 database files.
      options (Optional[dfvfs.VolumeScannerOptions]): volume scanner options. If
          None the default volume scanner options are used, which are defined in
          the dfVFS VolumeScannerOptions class.

    Yields:
      tuple[str, dict[str, str]]: known database type identifier or the name of
          the SQLite database file if not known and schema as an SQL query per
          table name.
    """
    entry_lister = file_entry_lister.FileEntryLister(mediator=self._mediator)

    base_path_specs = entry_lister.GetBasePathSpecs(path, options=options)
    if not base_path_specs:
      database_schema = self._GetDatabaseSchema(path)
      if database_schema is None:
        logging.warning(
            f'Unable to determine schema from database file: {path:s}')
      else:
        yield os.path.basename(path), database_schema

    else:
      for file_entry, path_segments in entry_lister.ListFileEntries(
          base_path_specs):
        if not file_entry.IsFile() or file_entry.size < 16:
          continue

        file_object = file_entry.GetFileObject()
        if not self._CheckSignature(file_object):
          continue

        display_path = self.GetDisplayPath(path_segments)
        # logging.info(
        #   f'Extracting schema from database file: {display_path:s}')

        database_schema = self._GetDatabaseSchemaFromFileObject(file_object)
        if database_schema is None:
          logging.warning((
              f'Unable to determine schema from database file: '
              f'{display_path:s}'))
          continue

        database_identifier = self._GetDatabaseIdentifier(path_segments)
        if not database_identifier:
          logging.warning((
              f'Unable to determine known database identifier of file: '
              f'{display_path:s}'))

          database_identifier = path_segments[-1]

        yield database_identifier, database_schema

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
