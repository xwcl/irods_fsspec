import logging
import os
import ssl
from fsspec.spec import AbstractFileSystem
from fsspec.registry import register_implementation
from irods.session import iRODSSession
from urllib.parse import urlparse

IRODS_PORT = 1247

log = logging.getLogger(__name__)

def kwargs_from_url(url):
    result = urlparse(url)
    if result.username is not None:
        try:
            user, zone = result.username.split('%23')
        except ValueError:
            user = result.username
            zone = None
    else:
        user = None
        zone = None
    return {
        'user': user,
        'zone': zone,
        'password': result.password,
        'host': result.hostname,
        'port': result.port if result.port is not None else IRODS_PORT,
    }

class IRODSFileSystem(AbstractFileSystem):
    root_marker = "/"
    def __init__(self, *args, user=None, zone=None, password=None, host=None, port=IRODS_PORT, **storage_options):
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
        if all(x is None for x in (user, password, zone)):
            try:
                env_file = os.environ['IRODS_ENVIRONMENT_FILE']
            except KeyError:
                env_file = os.path.expanduser('~/.irods/irods_environment.json')
            log.info(f'Initializing iRODS session from {env_file}')
            self.session = iRODSSession(irods_env_file=env_file, ssl_context=ssl_context)
        else:
            log.info(f'Initializing iRODS session from specified ({host=}, {port=}, {user=}, password=****, {zone=})')
            self.session = iRODSSession(
                host=host,
                port=port if port is not None else IRODS_PORT,
                user=user,
                password=password,
                zone=zone
            )
        super().__init__(*args, **storage_options)

    def __del__(self):
        self.session.cleanup()

    @staticmethod
    def _get_kwargs_from_urls(path):
        return kwargs_from_url(path)

    @classmethod
    def _strip_protocol(cls, path):
        result = urlparse(path)
        return os.path.normpath(result.path) if len(path) else self.root_marker

    def mkdir(self, path, **kwargs):
        path = os.path.normpath(self._strip_protocol(path))
        if not self.session.collections.exists(path):
            log.info(f'Creating iRODS collection {path}')
            self.session.collections.create(path)
            self.invalidate_cache(self._parent(path))
        else:
            log.info(f'iRODS collection {path} exists')
        return path

    def rmdir(self, path, recurse=False):
        path = os.path.normpath(self._strip_protocol(path))
        self.session.collections.remove(path, recurse=recurse)
        self.invalidate_cache(self._parent(path))

    def _collections_or_data_objects(self, path):
        if self.session.data_objects.exists(path):
            return self.session.data_objects
        elif self.session.collections.exists(path):
            return self.session.collections
        else:
            raise FileNotFoundError(f"No such data object or collection: {path}")

    def _mv_data_object(self, path1, path2):
        self.session.data_objects.move(path1, path2)
    
    def _mv_collection(self, path1, path2):
        self.session.collections.move(path1, path2)

    def mv(self, path1, path2):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        proxy_obj = self._collections_or_data_objects(path1)
        proxy_obj.move(path1, path2)
        self.invalidate_cache(self._parent(path1))
        self.invalidate_cache(self._parent(path2))

    def _rm(self, path):
        proxy_obj = self._collections_or_data_objects(path)
        if proxy_obj is self.session.collections:
            proxy_obj.remove(path)
        else:
            proxy_obj.unlink(path)

    def _open(self, path, mode="rb", autocommit=True, **kwargs):
        mode = mode.replace('b', '')
        path = self._strip_protocol(path)
        if not autocommit:
            raise NotImplementedError("Only autocommit=True operations supported")
        if 'w' in mode and not self.session.data_objects.exists(path):
            log.info(f'Creating {path=}')
            data_obj = self.session.data_objects.create(path)
            self.invalidate_cache(self._parent(path))
        else:
            data_obj = self.session.data_objects.get(path)
        return data_obj.open(mode=mode)

def register():
    register_implementation('irods', IRODSFileSystem)
