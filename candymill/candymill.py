
import datetime
import hashlib
import os
from collections import namedtuple
import subprocess

from hashfs import HashFS, HashAddress
from hashfs.hashfs import Stream
from contextlib import contextmanager, closing
from hashfs._compat import to_bytes
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()

from sqlalchemy import Column, Integer, String, Float, DateTime


FILETYPE_FPATH = os.getenv('FILETYPE_FPATH', default='/usr/bin/file')
XDGMIME_FPATH = os.getenv('XDGMIME_FPATH', default='/usr/bin/xdg-mime')


def setup_database(engine_url):
    # todo: check if url encoding needed
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)


class CandyStorage(HashFS):

    INTERESTING_MIMETYPES = {
        'application/msword',
        'application/pdf',
        'application/vnd.ms-access',
        'application/vnd.ms-excel',
        'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
        'application/vnd.ms-excel.sheet.macroEnabled.12',
        'application/vnd.ms-excel.template.macroEnabled.12',
        'application/vnd.ms-powerpoint',
        'application/vnd.ms-powerpoint.addin.macroEnabled.12',
        'application/vnd.ms-powerpoint.presentation.macroEnabled.12',
        'application/vnd.ms-powerpoint.slideshow.macroEnabled.12',
        'application/vnd.ms-powerpoint.template.macroEnabled.12',
        'application/vnd.ms-word.document.macroEnabled.12',
        'application/vnd.ms-word.template.macroEnabled.12',
        'application/vnd.oasis.opendocument.chart',
        'application/vnd.oasis.opendocument.database',
        'application/vnd.oasis.opendocument.formula',
        'application/vnd.oasis.opendocument.graphics',
        'application/vnd.oasis.opendocument.graphics-template',
        'application/vnd.oasis.opendocument.image',
        'application/vnd.oasis.opendocument.presentation',
        'application/vnd.oasis.opendocument.presentation-template',
        'application/vnd.oasis.opendocument.spreadsheet',
        'application/vnd.oasis.opendocument.spreadsheet-template',
        'application/vnd.oasis.opendocument.text',
        'application/vnd.oasis.opendocument.text-master',
        'application/vnd.oasis.opendocument.text-template',
        'application/vnd.oasis.opendocument.text-web',
        'application/vnd.openofficeorg.extension',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'application/vnd.openxmlformats-officedocument.presentationml.slideshow',
        'application/vnd.openxmlformats-officedocument.presentationml.template',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
        'pplication/vnd.ms-excel.addin.macroEnabled.12'
    }
    WANTED_ALGOS = ('md5', 'sha1', 'sha256')

    def __init__(self, *args, **kwargs):
        self._engine_url = kwargs.pop('engine_url')
        # just for candymill-hashfilter
        if self._engine_url is not None:
            self._engine = create_engine(self._engine_url)
        super().__init__(*args, **kwargs)

    @classmethod
    def _wanted_file(cls, file_path):
        """"""
        file_filetype = subprocess.run(
            [FILETYPE_FPATH, '--brief', file_path], stdout=subprocess.PIPE
        ).stdout.decode('utf-8').strip()
        if file_filetype.startswith('Microsoft '):
            return True
        # NOTE: most docs filetypes will be evaluated to application/zip by xdg-mime
        file_xdgmime = subprocess.run(
            [XDGMIME_FPATH, 'query', 'filetype', file_path],
            stdout=subprocess.PIPE
        ).stdout.decode('utf-8').strip()
        return file_xdgmime if file_xdgmime in cls.INTERESTING_MIMETYPES else False

    @classmethod
    def computehashes(cls, stream):
        """Compute hash of file using :attr:`algorithm`."""
        hashes = {}
        for algo in cls.WANTED_ALGOS:
            hashes.update({algo: hashlib.new(algo)})
        for data in stream:
            for algo in cls.WANTED_ALGOS:
                hashes[algo].update(to_bytes(data))
        for algo in cls.WANTED_ALGOS:
            hashes[algo] = hashes[algo].hexdigest()
        return hashes

    def put(self, file, extension=None):
        """Store contents of `file` on disk using its content hash for the
        address.

        Args:
            file (mixed): Readable object or path to file.
            extension (str, optional): Optional extension to append to file
                when saving.

        Returns:
            HashAddress: File's hash address.
        """
        wanted_type = self._wanted_file(file)
        if not wanted_type:
            return None
        stream = Stream(file)
        with closing(stream):
            hashes = self.computehashes(stream)
            file_id = hashes.get(self.algorithm)
            self._store_metadata(hashes)
            filepath, is_duplicate = self._copy(stream, file_id, extension)
        return HashAddress(file_id, self.relpath(filepath), filepath, is_duplicate)

    def _store_metadata(self, hashes):
        """

        :param hashes:
        :return:
        """
        stored_metadata = StoredFileMetadata()
        for algo in self.WANTED_ALGOS:
            if not hasattr(stored_metadata, algo):
                raise AttributeError(f'{type(self)} does not have "{algo}" attribute')
            setattr(stored_metadata, algo, hashes.get(algo))
        with Session(self._engine) as session:
            session.merge(stored_metadata)
            session.commit()

    def delete(self, sha256):
        """

        :param sha256:
        :return:
        """
        super().delete(sha256)
        with Session(self._engine) as session:
            ob = session.query(StoredFileMetadata).get(sha256)
            session.delete(ob)
            session.commit()

    def count(self, use_database=True):
        """

        :param use_database:
        :return:
        """
        if use_database:
            with Session(self._engine) as session:
                return session.query(StoredFileMetadata).count()
        else:
            return super().count()

    def check_storage_consistency(self):
        """"""
        data_consistency = namedtuple('CandyStorageConsistency', 'fs_count db_count consistent')
        fs_count = self.count(use_database=False)
        db_count = self.count(use_database=True)
        consistent = fs_count == db_count
        return data_consistency(consistent=consistent, fs_count=fs_count, db_count=db_count)


class StoredFileMetadata(Base):
    __tablename__ = 'stored_files_metadata'
    sha256 = Column(String, primary_key=True)
    md5 = Column(String, nullable=False)
    sha1 = Column(String, nullable=False)
    stored_date = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f'{type(self)}({self.sha1}, {self.stored_date})'

    def __str__(self):
        return self.sha256
