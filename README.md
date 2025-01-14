# pydupes-library

Forked from <https://github.com/erikreed/pydupes> and converted from a CLI tool into an importable library. It's pretty much the same implementation and imports as much as possible from the original code.

> `pydupes` is yet another duplicate file finder like rdfind/fdupes et al
> that may be faster in environments with millions of files and terabytes
> of data or over high latency filesystems (e.g. NFS).

If there's enough interest I'll publish this as a pip package.

### Setup

```bash
pip install -r requirements.txt
```

### Usage

```python
import pydupes_library

dupes = pydupes_library.pydupes(path_to_directory)

# pydupes() returns a list of pathlib.Path items
for file in dupes:
    print(file)

# You can also have it delete the duplicate files for you
pydupes_library.pydupes(path_to_directory, delete=True)
```

`pydupes()` has a number of options for you to use:

```python
progress: bool = True  # Show the progress bar.
verbose: bool = False  # Print extra information.
read_concurrency: int = 4  # I/O concurrency for reading files.
traversal_concurrency: int = 1  # I/O concurrency for traversal (stat and listing syscalls).
traversal_checkpoint = None  # Persist the traversal index in jsonl format, or load an existing traversal if already exists.
min_size: int = 1  # Minimum file size (in bytes) to consider during traversal.
delete: bool = False  # Delete duplicate files. Irreversible.
```

* * *

## Original README

The algorithm is similar to [rdfind](https://github.com/pauldreik/rdfind) with threading and consolidation of
filtering logic (instead of separate passes).

- traverse the input paths, collecting the inodes and file sizes
- for each set of files with the same size:
    - further split by matching 4KB on beginning/ends of files
    - for each non-unique (by size, boundaries) candidate set, compute the sha256 and emit pairs with matching hash

Constraints:

- traversals do not span multiple devices
- symlink following not implemented
- concurrent modification of a traversed directory could produce false duplicate pairs
  (modification after hash computation)

## Benchmarks

Hardware is a 6 spinning disk RAID5 ext4 with
250GB memory, Ubuntu 18.04. Peak memory and runtimes via:
`/usr/bin/time -v`.

### Dataset 1:

- Directories: ~33k
- Files: ~14 million, 1 million duplicate
- Total size: ~11TB, 300GB duplicate

#### pydupes

- Elapsed (wall clock) time (h:mm:ss or m:ss): 39:04.73
- Maximum resident set size (kbytes): 3356936 (~3GB)

  INFO:pydupes:Traversing input paths: ['/raid/erik']
  INFO:pydupes:Traversal time: 209.6s
  INFO:pydupes:Cursory file count: 14416742 (10.9TiB), excluding symlinks and dupe inodes
  INFO:pydupes:Directory count: 33376
  INFO:pydupes:Number of candidate groups: 720263
  INFO:pydupes:Size filter reduced file count to: 14114518 (7.3TiB)
  INFO:pydupes:Comparison time: 2134.6s
  INFO:pydupes:Total time elapsed: 2344.2s
  INFO:pydupes:Number of duplicate files: 936948
  INFO:pydupes:Size of duplicate content: 304.1GiB

#### rdfind

- Elapsed (wall clock) time (h:mm:ss or m:ss): 1:57:20
- Maximum resident set size (kbytes): 3636396 (~3GB)

  Now scanning "/raid/erik", found 14419182 files.
  Now have 14419182 files in total.
  Removed 44 files due to nonunique device and inode.
  Now removing files with zero size from list...removed 2396 files
  Total size is 11961280180699 bytes or 11 TiB
  Now sorting on size:removed 301978 files due to unique sizes from list.14114764 files left.
  Now eliminating candidates based on first bytes:removed 8678999 files from list.5435765 files left.
  Now eliminating candidates based on last bytes:removed 3633992 files from list.1801773 files left.
  Now eliminating candidates based on md5 checksum:removed 158638 files from list.1643135 files left.
  It seems like you have 1643135 files that are not unique
  Totally, 304 GiB can be reduced.

#### fdupes

Note that this isn't a fair comparison since fdupes additionally performs a byte-by-byte comparison on
MD5 match. Invocation with "fdupes --size --summarize --recurse --quiet".

- Elapsed (wall clock) time (h:mm:ss or m:ss): 2:58:32
- Maximum resident set size (kbytes): 3649420 (~3GB)

  939588 duplicate files (in 705943 sets), occupying 326547.7 megabytes
