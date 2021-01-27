import json
import os
import pytest
import ssl
import fsspec
from fsspec.registry import get_filesystem_class
from irods.session import iRODSSession
from . import IRODSFileSystem
import logging

log = logging.getLogger(__name__)

_CAN_CONNECT = None
_ENV = None

def _check_connection():
    global _CAN_CONNECT, _ENV
    if _CAN_CONNECT is None:
        try:
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
            try:
                env_file = os.environ['IRODS_ENVIRONMENT_FILE']
            except KeyError:
                env_file = os.path.expanduser('~/.irods/irods_environment.json')
            session = iRODSSession(irods_env_file=env_file, ssl_context=ssl_context)
            with open(env_file, 'r') as f:
                _ENV = json.load(f)
            _ENV['irods_password'] = session.get_irods_password()
            session.cleanup()
            _CAN_CONNECT = True
        except ValueError:
            _CAN_CONNECT = False
    return _CAN_CONNECT

def test_registration():
    assert get_filesystem_class('irods') == IRODSFileSystem

@pytest.mark.skipif(
    not _check_connection(),
    reason="Initialize iRODS connection with 'iinit' and/or set $IRODS_ENVIRONMENT_FILE"
)
def test_write_then_read_implicit_auth():
    the_path = f"irods://{_ENV['irods_host']}/iplant/home/{_ENV['irods_user_name']}/delete_me_test_irods_fsspec.txt"
    f1 = fsspec.open(the_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')
    f2 = fsspec.open(the_path, "rb")
    with f2 as fh:
        assert fh.read() == b'test\n'
    
    # clean up
    f2.fs.rm(the_path)

@pytest.mark.skipif(
    not _check_connection(),
    reason="Initialize iRODS connection with 'iinit' and/or set $IRODS_ENVIRONMENT_FILE"
)
def test_write_then_read_explicit_auth():
    the_path = (
        f"irods://{_ENV['irods_user_name']}%23{_ENV['irods_zone_name']}:"
        f"{_ENV['irods_password']}@"
        f"{_ENV['irods_host']}:"
        f"{_ENV['irods_port']}"
        f"/iplant/home/{_ENV['irods_user_name']}/delete_me_test_irods_fsspec.txt"
    )
    f1 = fsspec.open(the_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')
    f2 = fsspec.open(the_path, "rb")
    with f2 as fh:
        assert fh.read() == b'test\n'

    # clean up
    f2.fs.rm(the_path)


@pytest.mark.skipif(
    not _check_connection(),
    reason="Initialize iRODS connection with 'iinit' and/or set $IRODS_ENVIRONMENT_FILE"
)
def test_mv():
    the_path = f"irods://{_ENV['irods_host']}/iplant/home/{_ENV['irods_user_name']}/delete_me_test_irods_fsspec.txt"
    f1 = fsspec.open(the_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')
    irodsfs = f1.fs
    new_path = the_path.replace('.txt', '.2.txt')
    irodsfs.mv(the_path, new_path)
    with irodsfs.open(new_path, 'rb') as fh:
        assert fh.read() == b'test\n'
    
    # clean up
    irodsfs.rm(new_path)


@pytest.mark.skipif(
    not _check_connection(),
    reason="Initialize iRODS connection with 'iinit' and/or set $IRODS_ENVIRONMENT_FILE"
)
def test_mkdir():
    irodsfs = fsspec.filesystem('irods')
    dir_path = f"/iplant/home/{_ENV['irods_user_name']}/delete_me_test_irods_fsspec_dir/"
    irodsfs.mkdir(dir_path)

    test_file_path = dir_path + "test.txt"
    f1 = irodsfs.open(test_file_path, "wb")
    with f1 as fh:
        fh.write(b'test\n')

    # clean up
    irodsfs.rm(test_file_path)
    irodsfs.rm(dir_path)
