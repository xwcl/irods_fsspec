irods_fsspec
============

Allows you to open files from iRODS servers using `irods://` URLs in [fsspec](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest). Requires fsspec >= 0.8.5 and python-irodsclient >= 0.8.6.

Usage
-----

If you already have the iRODS iCommands, log in with `iinit` to store your username and password. Subsequent use of irods_fsspec will use those credentials automatically. (You may also set `$IRODS_ENVIRONMENT_FILE` in your shell to point to an `irods_environment.json`.)

```python
import fsspec
import irods_fsspec  # note: must be imported to register irods:// handler

# Write to iRODS path
f1 = fsspec.open('irods://data.cyverse.org/iplant/home/myuser/test.txt', 'wb')
with f1 as fh:
    fh.write(b'test\n')

# Read from iRODS path
f2 = fsspec.open('irods://data.cyverse.org/iplant/home/myuser/test.txt', 'rb')
with f2 as fh:
    assert fh.read() == b'test\n'
```

The user, zone, and password can all be included in the URL, bypassing the need for `iinit` / iCommands:

```python
f3 = fsspec.open('irods://myuser%23iplant@data.cyverse.org/iplant/home/myuser/test.txt', 'rb')
with f3 as fh:
    assert fh.read() == b'test\n'
```

Note the percent-encoded `#` (written as `%23`) to separate user from zone in the username part of the URL.


To use the `IRODSFileSystem` API, either import it yourself or use fsspec:

```python
irodsfs = fsspec.filesystem('irods', user='myuser', zone='iplant', password='mypass', host='data.cyverse.org', port=1247)
irodsfs.mkdir('/iplant/home/myuser/_irods_fsspec_test/')
irodsfs.mv('/iplant/home/myuser/test.txt', '/iplant/home/myuser/_irods_fsspec_test/test2.txt')

f4 = irodsfs.open('/iplant/home/myuser/_irods_fsspec_test/test2.txt', 'rb')
with f4 as fh:
    assert fh.read() == b'test\n'
```
