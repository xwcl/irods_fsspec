import logging
import os
import ssl
from fsspec.spec import AbstractFileSystem
from fsspec.registry import register_implementation
from irods.session import iRODSSession
from urllib.parse import urlparse

IRODS_PORT = 1247

log = logging.getLogger(__name__)

def irods_config_from_url(url):
    result = urlparse(url)
    if result.username is not None:
        try:
            user, zone = result.username.split('+')
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
    def __init__(self, *args, session=None, user=None, zone=None, password=None, host=None, port=IRODS_PORT, **storage_options):
        if session is None:
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
            if all(x is None for x in (user, password, zone)):
                try:
                    env_file = os.environ['IRODS_ENVIRONMENT_FILE']
                except KeyError:
                    env_file = os.path.expanduser('~/.irods/irods_environment.json')
                log.debug(f'Initializing iRODS session from {env_file}')
                self.session = iRODSSession(irods_env_file=env_file, ssl_context=ssl_context)
            else:
                log.debug(f'Initializing iRODS session from specified ({host=}, {port=}, {user=}, password=****, {zone=})')
                self.session = iRODSSession(
                    host=host,
                    port=port if port is not None else IRODS_PORT,
                    user=user,
                    password=password,
                    zone=zone
                )
        else:
            log.debug(f'Reusing session {session}')
            self.session = session
        self.user_id = self.session.users.get(self.session.username)
        super().__init__(*args, **storage_options)

    def __del__(self):
        self.session.cleanup()

    @staticmethod
    def _get_kwargs_from_urls(path):
        return irods_config_from_url(path)

    @classmethod
    def _strip_protocol(cls, path):
        result = urlparse(path)
        return os.path.normpath(result.path) if len(path) else cls.root_marker

    def mkdir(self, path, **kwargs):
        path = os.path.normpath(self._strip_protocol(path))
        if not self.session.collections.exists(path):
            log.debug(f'Creating iRODS collection {path}')
            self.session.collections.create(path)
            self.invalidate_cache(self._parent(path))
        else:
            log.debug(f'iRODS collection {path} exists')
        return path

    def makedirs(self, path, exist_ok=False):
        """Recursively make directories

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If False, will error if the target already exists
        """
        path = os.path.normpath(self._strip_protocol(path))
        if self.session.data_objects.exists(path):
            raise FileExistsError(f"An iRODS data object exists at path {path}, cannot make a collection there")
        coll_exists = self.session.collections.exists(path)
        if not coll_exists:
            self.mkdir(path)
        elif not exist_ok:
            raise FileExistsError(f"An iRODS collection already exists at path {path}")
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
        path = self._strip_protocol(path)
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
            log.debug(f'Creating {path=}')
            data_obj = self.session.data_objects.create(path)
            self.invalidate_cache(self._parent(path))
        else:
            data_obj = self.session.data_objects.get(path)
        return data_obj.open(mode=mode)

    def _data_object_info(self, data_object):
        '''Given iRODSDataObject instance return dict with keys
        giving path, size, and type info

        Parameters
        ----------
        data_object : iRODSDataObject
            Instance about which to collect info

        Returns
        -------
        result : dict
            File metadata used by fsspec
        '''
        return {
            'name': data_object.path,
            'size': data_object.size,
            'type': 'file',
            'checksum': data_object.checksum
        }

    def _collection_info(self, collection):
        '''Given iRODSDataObject instance return dict with keys
        giving path, size, and type info

        Parameters
        ----------
        collection : iRODSCollection
            Instance about which to collect info

        Returns
        -------
        result : dict
            Directory metadata used by fsspec
        '''
        return {
            'name': collection.path,
            'size': 0,
            'type': 'directory'
        }

    def exists(self, path):
        path = self._strip_protocol(path)
        return (
            self.session.collections.exists(path) or
            self.session.data_objects.exists(path)
        )

    def ls(self, path, detail=False):
        '''List data objects and subcollections at path.

        When `detail=True`, `entries` is a list of dicts containing
        (at least) the keys {'name', 'size', 'type'} where size is
        in bytes, and type is either 'file' (data object) or
        'directory' (collection)

        Parameters
        ----------
        path : str
            iRODS path
        detail : bool (default: True)
            Whether to return size and type information as a
            dict or just a list of names

        Returns
        -------
        entries : list
            Either a list of dicts (`detail=True`) or of str paths
            (`detail=False`) with information on the data objects and
            subcollections of `path`
        '''
        # info dict keys:
        #   name (path)
        #   size (bytes)
        #   type ('file', 'directory')
        #   ... ?
        entries = []
        path = self._strip_protocol(path)
        if self.session.data_objects.exists(path):
            data_object = self.session.data_objects.get(path)
            entries.append(self._data_object_info(data_object))
        elif self.session.collections.exists(path):
            collection = self.session.collections.get(path)
            # unify data_objects and subcollections
            for subcoll in collection.subcollections:
                entries.append(self._collection_info(subcoll))
            for data_object in collection.data_objects:
                entries.append(self._data_object_info(data_object))
        else:
            raise FileNotFoundError(f"No collection or data object readable by {self.session.username} at {path}")
        if detail:
            return entries
        else:
            return [x['name'] for x in entries]

    def cp_file(self, path1, path2):
        '''
        Copy a single data object (see `copy` for recursive)
        '''
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        proxy_obj = self._collections_or_data_objects(path1)
        if proxy_obj is self.session.collections:
            proxy_obj.create(path2)
        else:
            proxy_obj.copy(path1, path2)

def register():
    register_implementation('irods', IRODSFileSystem)
