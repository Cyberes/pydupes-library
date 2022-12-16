import datetime
import logging
import os
import shutil
import subprocess
import sys
import time
from hashlib import sha256
from itertools import tee
from logging import Logger
from pathlib import Path
from typing import Union

from imohash import hashfile
from natsort import natsorted
from pydupes import (
    DupeFinder, FileCrawler, DuplicateComparator,
    FatalCrawlException, main, FutureFreeThreadPool)
from pydupes import traverse_paths, sizeof_fmt
from tqdm import tqdm


# pip install imohash

def generate_hash(file: Union[str, Path]) -> str:
    # block_size = 65536
    # filehash = sha256()
    try:
        filehash = hashfile(file, hexdigest=True)
        return filehash
        # with open(file, 'rb') as f:
        #     fileblock = f.read(block_size)
        #     while len(fileblock) > 0:
        #         filehash.update(fileblock)
        #         fileblock = f.read(block_size)
        #     filehash = filehash.hexdigest()
        # return filehash
    except Exception as e:
        tqdm.write(f'Exception {e} on {file}')
        return '-1'


def num_files(path) -> int:
    s = subprocess.run(f'ls -1q {path} | wc -l', shell=True, capture_output=True).stdout.decode().strip('\n')
    return int(s)


# def fdupes(input_path: Union[str, Path], recursive: bool = False, dry_run: bool = False):
#     are_there_files_to_delete = False
#     count_cleaned = 0
#     file_hashes = {}  # file to keep: {[to delete, to delete, ...]}
#
#     if not Path(input_path).exists():
#         raise FileNotFoundError
#     elif Path(input_path).is_file():
#         raise NotADirectoryError
#
#     if dry_run:
#         print('Dry run, not deleting files.')
#
#     if recursive:
#         files = Path(input_path).rglob('*')
#     else:
#         files = Path(input_path).iterdir()
#     # bar gets stuck doing the scan so we have to set it to a variable so we can call bar.___ later
#     bar = tqdm(total=num_files(input_path), desc='Scanning files', leave=False)
#     i = 0
#     for f in files:
#         # tqdm.write(str(f))
#         if f.is_dir():
#             continue
#         filehash = generate_hash(f)
#         if filehash == '-1':
#             continue
#         if filehash not in file_hashes.keys():
#             file_hashes[filehash] = []
#             file_hashes[filehash].append(f)  # the first element of the array is always the original file
#         else:
#             file_hashes[filehash].append(f)
#             are_there_files_to_delete = True
#         bar.update(1)
#         bar.refresh()
#         sys.stderr.flush()
#         if i == 20000:
#             break
#         i += 1
#     bar.close()
#
#     t = len(file_hashes.keys())
#     if not are_there_files_to_delete:
#         print('No duplicates found.')
#         return
#     for i, tp in tqdm(enumerate(file_hashes.items()), total=t, desc='Deleting files'):
#         original = tp[0]
#         original_path = None  # make pycharm happy
#         duplicates = tp[1]
#         to_delete = []
#         for i, d in enumerate(duplicates):
#             if i == 0:
#                 original_path = d
#                 continue
#             to_delete.append(d)
#             count_cleaned += 1
#         to_delete = natsorted(to_delete)
#         # Only print the '[+]' if there are duplicates
#         if len(to_delete) > 0:
#             tqdm.write(f'[+] {original_path}')
#             for d in to_delete:
#                 if not dry_run:
#                     tqdm.write(f'\t[-] {d}')
#                     os.remove(d)
#                 else:
#                     tqdm.write(f'\t[-d] {d}')
#     print(f'Deleted {count_cleaned} duplicate files.')


# def pydupes_cli(*input_paths, output,
#                 verbose: bool = False,
#                 progress: bool = False,
#                 read_concurrency: int = 4,
#                 traversal_concurrency: int = 1,
#                 traversal_checkpoint=None,
#                 min_size: int = 1
#                 ):
#     """
#     @click.command(help="A duplicate file finder that may be faster in environments with "
#                     "millions of files and terabytes of data or over high latency filesystems.")
# @click.argument('input_paths', type=click.Path(
#     exists=True, file_okay=False, readable=True), nargs=-1)
# @click.option('--output', type=click.File('w'),
#               help='Save null-delimited input/duplicate filename pairs. For stdout use "-".')
# @click.option('--verbose', is_flag=True, help='Enable debug logging.')
# @click.option('--progress', is_flag=True, help='Enable progress bars.')
# @click.option('--min-size', type=click.IntRange(min=0), default=1,
#               help='Minimum file size (in bytes) to consider during traversal.')
# @click.option('--read-concurrency', type=click.IntRange(min=1), default=4,
#               help='I/O concurrency for reading files.')
# @click.option('--traversal-concurrency', type=click.IntRange(min=1), default=1,
#               help='I/O concurrency for traversal (stat and listing syscalls).')
# @click.option('--traversal-checkpoint', type=click.Path(),
#               help='Persist the traversal index in jsonl format, or load an '
#                    'existing traversal if already exists. Use .gz extension to compress. Input paths are '
#                    'ignored if a traversal checkpoint is loaded.')
#     :param input_paths:
#     :param output:
#     :param verbose:
#     :param progress:
#     :param read_concurrency:
#     :param traversal_concurrency:
#     :param traversal_checkpoint:
#     :param min_size:
#     :return:
#     """
#     s = ''
#     for x in input_paths:
#         s = s + ' "' + x + '"'
#     input_paths = s
#     if verbose:
#         verbose_flag = '--verbose'
#     else:
#         verbose_flag = ''
#     if progress:
#         progress_flag = '--progress'
#     else:
#         progress_flag = ''
#     if traversal_checkpoint:
#         trav_ckpt_flag = f'--traversal-checkpoint {traversal_checkpoint}'
#     else:
#         trav_ckpt_flag = ''
#     subprocess.run(f'pydupes {input_paths} {progress_flag} {verbose_flag} --min-size {min_size} --read-concurrency {read_concurrency} --traversal-concurrency {traversal_concurrency} {trav_ckpt_flag} --output "{output}"', shell=True)

def delete_dupes(dupes, progress_bar: bool = True):
    if len(dupes) == 0:
        print('No duplicates found.')
        return
    for dupe in tqdm(dupes, disable=(not progress_bar), desc='Deleting files'):
        tqdm.write(f'[-] {dupe}')
        os.remove(dupe)
    print(f'Deleted {len(dupes)} duplicate files.')


def pydupes(*input_paths,
            progress: bool = True,
            verbose: bool = False,
            use_logging: bool = False,
            read_concurrency: int = 4,
            traversal_concurrency: int = 1,
            traversal_checkpoint=None,
            min_size: int = 1,
            delete: bool = False
            ) -> list:
    logger = logging.getLogger('pydupes')
    input_paths = [Path(p) for p in input_paths]
    traversal_checkpoint = pathlib.Path(traversal_checkpoint) if traversal_checkpoint else None
    if not input_paths and not traversal_checkpoint:
        click.echo(click.get_current_context().get_help())
        exit(1)
    comparator = DuplicateComparator(input_paths)

    time_start = datetime.datetime.now()

    # def do_log(msg, *args, **kwargs):
    #     # A SUPER hacky way of doing loggings % string formatting
    #     dont_eat_these = [' ', '[', ']', '(', ')']
    #     msg = list(msg)
    #     for a_i, a in enumerate(args):
    #         for c_i, c in enumerate(msg):
    #             if c == '%':
    #                 if isinstance(a, list):
    #                     r = a[0]
    #                 else:
    #                     r = a
    #                 msg[c_i] = str(r)
    #                 i = 1
    #                 while len(msg) > c_i + 1:
    #                     if msg[c_i + i] not in dont_eat_these:
    #                         del msg[c_i + i]
    #                     else:
    #                         break
    #     msg = ''.join(msg)
    #     tqdm.write(msg)
    #
    # old_logging_info = logger.info
    # if not use_logging:
    #     logger.info = do_log

    def no_log(msg, *args, **kwargs):
        return

    if not use_logging:
        logger.info = no_log
    else:
        logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    if traversal_checkpoint and traversal_checkpoint.exists():
        size_groups, num_potential_dupes, size_potential_dupes = load_traversal_checkpoint(
            traversal_checkpoint)
    else:
        size_groups, num_potential_dupes, size_potential_dupes = traverse_paths(
            progress, traversal_concurrency, input_paths, traversal_checkpoint, min_size)

    dt_filter_start = datetime.datetime.now()
    with FutureFreeThreadPool(threads=read_concurrency) as scheduler_pool, \
            FutureFreeThreadPool(threads=read_concurrency) as io_pool, \
            tqdm(smoothing=0, desc='Filtering', unit=' files',
                 position=0, mininterval=1,
                 total=num_potential_dupes,
                 disable=not progress) as file_progress, \
            tqdm(smoothing=0, desc='Filtering', unit='B',
                 position=1, unit_scale=True, unit_divisor=1024,
                 total=size_potential_dupes, mininterval=1,
                 disable=not progress) as bytes_progress:
        size_num_dupes = []
        dupe_finder = DupeFinder(pool=io_pool, output=None, comparator=comparator,
                                 file_progress=file_progress, byte_progress=bytes_progress)

        duplicate_files = []

        def callback(args):
            size, dupes = args
            size_num_dupes.append((size, len(dupes)))

        def return_with_size(size_bytes, group):
            dupes = dupe_finder.find(size_bytes, group)
            for d in dupes:
                duplicate_files.append(d)
            return size_bytes, dupes

        for size_bytes, group in size_groups:
            if size_bytes >= min_size:
                scheduler_pool.submit(return_with_size, size_bytes, group, callback=callback)
            else:
                file_progress.update(len(group))
                bytes_progress.update(len(group) * size_bytes)
        scheduler_pool.wait_until_complete()

        dupe_count = 0
        dupe_total_size = 0
        for size_bytes, num_dupes in size_num_dupes:
            dupe_count += num_dupes
            dupe_total_size += num_dupes * size_bytes

    # dt_complete = datetime.datetime.now()
    # print(f'Comparison time: {round((dt_complete - dt_filter_start).total_seconds())}')
    # print(f'Total time elapsed: {round((dt_complete - time_start).total_seconds())}')
    #
    # print(f'Number of duplicate files: {dupe_count}')
    # print(f'Size of duplicate content: {sizeof_fmt(dupe_total_size)}')
    if delete:
        delete_dupes(duplicate_files, progress)
    return duplicate_files
