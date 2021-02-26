import json
import os
import pytest
import ssl
import time
import fsspec
import irods_fsspec
from fsspec.registry import get_filesystem_class
from irods.session import iRODSSession
from . import IRODSFileSystem
import logging

irods_fsspec.register()
log = logging.getLogger(__name__)


def test_registration():
    assert get_filesystem_class('irods') == IRODSFileSystem

@pytest.fixture
def session():
    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
    try:
        env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    except KeyError:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')
    try:
        session = iRODSSession(irods_env_file=env_file, ssl_context=ssl_context)
    except Exception as e:
        raise RuntimeError("Initialize iRODS connection with 'iinit' and/or set $IRODS_ENVIRONMENT_FILE\n{e}")
    yield session
    session.cleanup()

@pytest.fixture
def irods_env(session):
    return {
        'irods_host': session.host,
        'irods_zone_name': session.zone,
        'irods_user_name': session.username,
        'irods_port': session.port,
        'irods_password': session.get_irods_password()
    }

@pytest.fixture
def irodsfs(session):
    return fsspec.filesystem('irods', session=session)

@pytest.fixture
def scratch_collection_path(session):
    ts = time.time()
    path = f'/iplant/home/{session.username}/delete_me_test_irods_fsspec_{ts}'
    session.collections.create(path)
    yield path
    session.collections.remove(path)

def test_write_then_read_implicit_auth(session, irods_env, scratch_collection_path):
    the_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec.txt"
    the_url = f"irods://{irods_env['irods_host']}{the_path}"
    f1 = fsspec.open(the_url, "wb")
    with f1 as fh:
        fh.write(b'test\n')

    # confirm with underlying python-irodsclient
    f2 = session.data_objects.get(the_path)
    with f2.open("r") as fh:
        assert fh.read() == b'test\n'

def test_write_then_read_explicit_auth(session, irods_env, scratch_collection_path):
    the_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec.txt"
    the_url = (
        f"irods://{irods_env['irods_user_name']}+{irods_env['irods_zone_name']}:"
        f"{irods_env['irods_password']}@"
        f"{irods_env['irods_host']}:"
        f"{irods_env['irods_port']}"
        f"{the_path}"
    )
    f1 = fsspec.open(the_url, "wb")
    with f1 as fh:
        fh.write(b'test\n')

    # confirm with underlying python-irodsclient
    f2 = session.data_objects.get(the_path)
    with f2.open("r") as fh:
        assert fh.read() == b'test\n'

def test_kwargs_from_url():
    result = irods_fsspec.irods_config_from_url('irods://exao_dap+iplant:secret@data.cyverse.org')
    assert result['user'] == 'exao_dap'
    assert result['zone'] == 'iplant'
    assert result['password'] == 'secret'
    assert result['host'] == 'data.cyverse.org'
    assert result['port'] == irods_fsspec.IRODS_PORT

    result = irods_fsspec.irods_config_from_url('irods://exao_dap+iplant:secret@data.cyverse.org')
    assert result['user'] == 'exao_dap'
    assert result['zone'] == 'iplant'
    assert result['password'] == 'secret'
    assert result['host'] == 'data.cyverse.org'
    assert result['port'] == irods_fsspec.IRODS_PORT

    result = irods_fsspec.irods_config_from_url('irods://data.cyverse.org')
    assert result['user'] == None
    assert result['zone'] == None
    assert result['password'] == None
    assert result['host'] == 'data.cyverse.org'
    assert result['port'] == irods_fsspec.IRODS_PORT

    result = irods_fsspec.irods_config_from_url('irods://probably=invalid@data.cyverse.org')
    assert result['user'] == 'probably=invalid'
    assert result['zone'] == None
    assert result['password'] == None
    assert result['host'] == 'data.cyverse.org'
    assert result['port'] == irods_fsspec.IRODS_PORT

def test_mv(irodsfs, scratch_collection_path):
    the_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec.txt"
    f1 = irodsfs.open(the_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')

    new_path = the_path.replace('.txt', '.2.txt')
    irodsfs.mv(the_path, new_path)
    with irodsfs.open(new_path, 'rb') as fh:
        assert fh.read() == b'test\n'
    assert irodsfs.exists(new_path)
    assert not irodsfs.exists(the_path)

def test_copy(irodsfs, scratch_collection_path):
    the_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec.txt"
    # Copy a file
    f1 = irodsfs.open(the_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')
    new_path = the_path.replace('.txt', '.2.txt')
    irodsfs.cp_file(the_path, new_path)
    with irodsfs.open(new_path, 'rb') as fh:
        assert fh.read() == b'test\n'
    assert irodsfs.exists(new_path)
    assert irodsfs.exists(the_path)

    # copy a directory recursively
    destdir = f"{scratch_collection_path}/test_copy"
    irodsfs.mkdir(destdir)
    irodsfs.mv(the_path, destdir)

    irodsfs.mkdir(f"{destdir}/subcoll")
    irodsfs.mv(new_path, f"{destdir}/subcoll")

    copy_coll = f"{scratch_collection_path}/test_copy_2"
    irodsfs.copy(destdir, copy_coll, recursive=True)

    assert irodsfs.exists(f"{copy_coll}/delete_me_test_irods_fsspec.txt")
    assert irodsfs.exists(f"{copy_coll}/subcoll/delete_me_test_irods_fsspec.2.txt")


def test_mkdir(session, irodsfs, scratch_collection_path):
    dir_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec_dir"
    irodsfs.mkdir(dir_path)
    assert session.collections.exists(dir_path)

def test_ls(session, irodsfs, scratch_collection_path):
    dir_path = f"{scratch_collection_path}/delete_me_test_irods_fsspec_dir/"
    test_file_path = dir_path + "test.txt"
    subcoll_path = dir_path + "subcollection"

    # test
    irodsfs.mkdir(dir_path)
    f1 = irodsfs.open(test_file_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')

    result = irodsfs.ls(dir_path)
    assert result == [{'name': test_file_path, 'type': 'file', 'size': 5, 'checksum': 'd8e8fca2dc0f896fd7cb4cb0031ba249'}]

    irodsfs.mkdir(subcoll_path)
    result = irodsfs.ls(dir_path)
    result.sort(key=lambda x: x['name'])
    assert result == [
        {'name': subcoll_path, 'type': 'directory', 'size': 0},
        {'name': test_file_path, 'type': 'file', 'size': 5, 'checksum': 'd8e8fca2dc0f896fd7cb4cb0031ba249'},
    ]
