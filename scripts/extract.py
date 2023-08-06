#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script to extract the schema of SQLite database files."""

import argparse
import logging
import os
import sys

from dfvfs.helpers import command_line as dfvfs_command_line
from dfvfs.helpers import volume_scanner as dfvfs_volume_scanner
from dfvfs.lib import errors as dfvfs_errors

from dfimagetools import definitions as dfimagetools_definitions
from dfimagetools import file_entry_lister
from dfimagetools import helpers

from sqliterc import schema_extractor


def GetDisplayPath(path_segments, data_stream_name):
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


def Main():
  """The main program function.

  Returns:
    bool: True if successful or False if not.
  """
  argument_parser = argparse.ArgumentParser(description=(
      'Extracts the schema of SQLite database files.'))

  # TODO: add output group.
  argument_parser.add_argument(
      '--output', dest='output', action='store', metavar='./sqlite-kb/',
      default=None, help='directory to write the output to.')

  # TODO: add source group.
  argument_parser.add_argument(
      '--back_end', '--back-end', dest='back_end', action='store',
      metavar='NTFS', default=None, help='preferred dfVFS back-end.')

  argument_parser.add_argument(
      '--partitions', '--partition', dest='partitions', action='store',
      type=str, default=None, help=(
          'Define partitions to be processed. A range of partitions can be '
          'defined as: "3..5". Multiple partitions can be defined as: "1,3,5" '
          '(a list of comma separated values). Ranges and lists can also be '
          'combined as: "1,3..5". The first partition is 1. All partitions '
          'can be specified with: "all".'))

  argument_parser.add_argument(
      '--snapshots', '--snapshot', dest='snapshots', action='store', type=str,
      default=None, help=(
          'Define snapshots to be processed. A range of snapshots can be '
          'defined as: "3..5". Multiple snapshots can be defined as: "1,3,5" '
          '(a list of comma separated values). Ranges and lists can also be '
          'combined as: "1,3..5". The first snapshot is 1. All snapshots can '
          'be specified with: "all".'))

  argument_parser.add_argument(
      '--volumes', '--volume', dest='volumes', action='store', type=str,
      default=None, help=(
          'Define volumes to be processed. A range of volumes can be defined '
          'as: "3..5". Multiple volumes can be defined as: "1,3,5" (a list '
          'of comma separated values). Ranges and lists can also be combined '
          'as: "1,3..5". The first volume is 1. All volumes can be specified '
          'with: "all".'))

  # TODO: add support for single database file.

  argument_parser.add_argument(
      'source', nargs='?', action='store', metavar='image.raw',
      default=None, help='path of the storage media image.')

  options = argument_parser.parse_args()

  if not options.source:
    print('Source value is missing.')
    print('')
    argument_parser.print_help()
    print('')
    return False

  if options.output:
    if not os.path.exists(options.output):
      os.mkdir(options.output)

    if not os.path.isdir(options.output):
      print(f'{options.output:s} must be a directory')
      print('')
      return False

  helpers.SetDFVFSBackEnd(options.back_end)

  logging.basicConfig(
      level=logging.INFO, format='[%(levelname)s] %(message)s')

  mediator = dfvfs_command_line.CLIVolumeScannerMediator()

  volume_scanner_options = dfvfs_volume_scanner.VolumeScannerOptions()
  volume_scanner_options.partitions = mediator.ParseVolumeIdentifiersString(
      options.partitions)

  if options.snapshots == 'none':
    volume_scanner_options.snapshots = ['none']
  else:
    volume_scanner_options.snapshots = mediator.ParseVolumeIdentifiersString(
        options.snapshots)

  volume_scanner_options.volumes = mediator.ParseVolumeIdentifiersString(
      options.volumes)

  entry_lister = file_entry_lister.FileEntryLister(mediator=mediator)

  try:
    base_path_specs = entry_lister.GetBasePathSpecs(
        options.source, options=volume_scanner_options)
    if not base_path_specs:
      print('No supported file system found in source.')
      print('')
      return False

    extractor = schema_extractor.SQLiteSchemaExtractor()

    for file_entry, path_segments in entry_lister.ListFileEntries(
        base_path_specs):
      if file_entry.size < 16:
        continue

      file_object = file_entry.GetFileObject()
      if not extractor.CheckSignature(file_object):
        continue

      display_path = GetDisplayPath(path_segments, '')
      logging.info(f'Extracting schema from database file: {display_path:s}')

      database_schema = extractor.GetDatabaseSchemaFromFileObject(file_object)
      if not database_schema:
        logging.warning(
            f'Unable to determine schema from database file: {display_path:s}')
        continue

      # TODO: add support to set output format.
      output_text = extractor.FormatSchema(database_schema, 'yaml')
      if not options.output:
        print(output_text)
        continue

      output_file = os.path.join(options.output, f'{file_entry.name:s}.yaml')
      if os.path.exists(output_file):
        logging.warning(f'Output file: {output_file:s} already existst.')
        continue

      with open(output_file, 'w', encoding='utf-8') as output_file_object:
        output_file_object.write(output_text)

  except dfvfs_errors.ScannerError as exception:
    print(f'[ERROR] {exception!s}', file=sys.stderr)
    print('')
    return False

  except KeyboardInterrupt:
    print('Aborted by user.', file=sys.stderr)
    print('')
    return False

  return True


if __name__ == '__main__':
  if not Main():
    sys.exit(1)
  else:
    sys.exit(0)
