import datetime
import logging
import os
from pathlib import Path

from pydupes import (
    DupeFinder, DuplicateComparator,
    FutureFreeThreadPool, load_traversal_checkpoint, traverse_paths, sizeof_fmt)
from tqdm import tqdm


def delete_dupes(dupes, progress_bar: bool = True):
    def do_print(msg):
        if progress_bar:
            print(msg)

    if len(dupes) == 0:
        do_print('No duplicates found.')
        return
    for dupe in tqdm(dupes, disable=(not progress_bar), desc='Deleting files'):
        tqdm.write(f'[-] {dupe}') if progress_bar else None
        os.remove(dupe)
    do_print(f'Deleted {len(dupes)} duplicate files.')


def pydupes(*input_paths,
            progress: bool = True,
            verbose: bool = False,
            read_concurrency: int = 4,
            traversal_concurrency: int = 1,
            traversal_checkpoint=None,
            min_size: int = 1,
            delete: bool = False
            ) -> list[Path]:
    logger = logging.getLogger('pydupes')
    input_paths = [Path(p) for p in input_paths]
    traversal_checkpoint = Path(traversal_checkpoint) if traversal_checkpoint else None
    if not input_paths and not traversal_checkpoint:
        raise ValueError('No paths specified!')
    comparator = DuplicateComparator(input_paths)
    time_start = datetime.datetime.now()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.CRITICAL)

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
                duplicate_files.append(Path(d))
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

    dt_complete = datetime.datetime.now()
    logger.info(f'Comparison time: {round((dt_complete - dt_filter_start).total_seconds())}')
    logger.info(f'Total time elapsed: {round((dt_complete - time_start).total_seconds())}')
    logger.info(f'Number of duplicate files: {dupe_count}')
    logger.info(f'Size of duplicate content: {sizeof_fmt(dupe_total_size)}')

    if delete:
        delete_dupes(duplicate_files, progress)
    return duplicate_files
