irods_fsspec
============

Allows you to open files from iRODS servers using `irods://` URLs in [fsspec](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest).

Usage
-----

### Implicit authentication

If you already have the iRODS iCommands, log in with `iinit` to store your username and password. Subsequent use of irods_fsspec will use those credentials automatically. (You may also set `$IRODS_ENVIRONMENT_FILE` in your shell to point to an `irods_environment.json`.)

```python
import fsspec
import irods_fsspec
irods_fsspec.register() # register irods:// handler

# Write to iRODS path
f1 = fsspec.open('irods://data.cyverse.org/iplant/home/myuser/test.txt', 'wb')
with f1 as fh:
    fh.write(b'test\n')

# Read from iRODS path
f2 = fsspec.open('irods://data.cyverse.org/iplant/home/myuser/test.txt', 'rb')
with f2 as fh:
    assert fh.read() == b'test\n'
```

### Explicit authentication

The user, zone, password, and port can all be included in the URL, bypassing the need for `iinit` / iCommands:

```python
f3 = fsspec.open('irods://myuser+iplant:mypass@data.cyverse.org:1247/iplant/home/myuser/test.txt', 'rb')
with f3 as fh:
    assert fh.read() == b'test\n'
```

The format looks like this:

```
irods://<username>+<zone>:<password>@<hostname>[:<port>]/<path to data object>
```

Note the `+` to separate user name from the zone name. irods_fsspec will use the default port if `:<port>` is omitted, but all other components are required.

Filesystem API usage
--------------------

To use the `IRODSFileSystem` API, either import it yourself or use fsspec:

```python
irodsfs = fsspec.filesystem('irods', user='myuser', zone='iplant', password='mypass', host='data.cyverse.org', port=1247)
irodsfs.mkdir('/iplant/home/myuser/_irods_fsspec_test/')
irodsfs.mv('/iplant/home/myuser/test.txt', '/iplant/home/myuser/_irods_fsspec_test/test2.txt')

f4 = irodsfs.open('/iplant/home/myuser/_irods_fsspec_test/test2.txt', 'rb')
with f4 as fh:
    assert fh.read() == b'test\n'
```
