import typing
from pathlib import Path
import event_model
import numpy as np


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class IngestorPlugin():

    def __init__(self, handler):
        self.handler = handler

    def getStartDoc(self, paths: typing.List[str]):

        metadata = self.handler(paths[0]).metadata()
        descriptor_keys = getattr(self, 'descriptor_keys', [])
        metadata = dict([(key, metadata.get(key)) for key in descriptor_keys])
        metadata = self._setTitle(metadata, paths)

        run_bundle = event_model.compose_run(metadata=metadata)

        return run_bundle

    def getDataDocs(self, compose_event, compose_resource, paths: typing.List[str]):

        for path in paths:
            data = self.handler(path).metadata()

            resource_bundle = compose_resource(spec=self.handler.name,
                                               root='/',
                                               resource_path=path[1:],
                                               resource_kwargs={})
            yield 'resource', resource_bundle.resource_doc
            datum_doc = resource_bundle.compose_datum(datum_kwargs={})
            yield 'datum', datum_doc
            data.update({'image': datum_doc['datum_id']})
            filled = {'image': False}
            timestamps = {key: 0 for key in data}
            yield 'event', compose_event(data=data, timestamps=timestamps, filled=filled)

    def getDescriptorDocs(self, compose_descriptor, paths: typing.List[str]):
        metadata = self.handler(paths[0]).metadata()


        data_keys = dict()
        for key in metadata.keys():
            data_keys.update({key: {'dtype': data_type(metadata[key]),  # type-st
                                    'shape': [],  # req
                                    'source': paths[0]}})  # req    })

        shape = self.handler(paths[0])().shape  # Assumes each frame has same shape
        data_keys.update({'image': {'dtype': 'array',  # type-st
                               'external': 'FS:',  #
                               'shape': list(shape),  # req
                               'source': paths[0]}})  # req    }

        object_keys = {'files': list(data_keys.keys())}

        # TODO: metadata -> configuration (schema undocumented)
        configuration_keys = self.handler.configuration_keys.intersection(metadata.keys())

        yield compose_descriptor(name='primary', data_keys=data_keys, object_keys=object_keys)

    def getStopDoc(self, compose_stop):
        return compose_stop()

    def title(self, paths: typing.List[str]):
        if len(paths) > 1:
            return f'Series: {Path(paths[0]).resolve().stem}…'
        return Path(paths[0]).resolve().stem

    def _setTitle(self, startdoc, paths: typing.List[str]):
        startdoc['sample_name'] = self.title(paths)
        return startdoc

    def ingest(self, paths: typing.List[str]):
        run_bundle = self.getStartDoc(paths)
        yield 'start', run_bundle.start_doc

        descriptor_bundles = list(self.getDescriptorDocs(run_bundle.compose_descriptor, paths))

        for descriptor_bundle in descriptor_bundles:
            yield 'descriptor', descriptor_bundle.descriptor_doc
            yield from self.getDataDocs(descriptor_bundle.compose_event, run_bundle.compose_resource, paths)

        yield 'stop', self.getStopDoc(run_bundle.compose_stop)


# From ophyd/epics_pvs.py

_type_map = {'number': (float, np.floating),
             'array': (np.ndarray, list, tuple),
             'string': (str,),
             'integer': (int, np.integer),
             }


def data_type(val):
    '''Determine the JSON-friendly type name given a value
    Returns
    -------
    str
        One of {'number', 'integer', 'array', 'string'}
    Raises
    ------
    ValueError if the type is not recognized
    '''
    bad_iterables = (str, bytes, dict)
    if isinstance(val, typing.Iterable) and not isinstance(val, bad_iterables):
        return 'array'

    for json_type, py_types in _type_map.items():
        if isinstance(val, py_types):
            return json_type

    raise ValueError(
        f'Cannot determine the appropriate bluesky-friendly data type for '
        f'value {val} of Python type {type(val)}. '
        f'Supported types include: int, float, str, and iterables such as '
        f'list, tuple, np.ndarray, and so on.'
    )


FileSeriesIngestor = IngestorPlugin


class FileHandlerPlugin():
    """
    This base class defines a reader/writer for an on-disk file format. This interface will be structured such that the
    format definition is registered with FabIO at activation, and will mirror the FabIO API structure. Subclass
    instances should not depend on other plugins. Example: A reader/writer for the *.fits file format.

    """

    description = ""

    extensions = set()

    magic_numbers = set()

    name = ''

    ingestor_class = FileSeriesIngestor

    configuration_keys = set()

    def __call__(self, *args, **kwargs):
        ...

    def __init__(self, path):
        self.path = path

    def metadata(self, *args, **kwargs) -> typing.Dict:
        return {}

    @classproperty
    def ingestor(cls):
        return cls.ingestor_class(cls)

    @classmethod
    def ingest(cls, files):
        yield from cls.ingestor.ingest(files)

    def validate(self):
        assert self()
        ...


############# TESTS ############
import tifffile
import numpy as np
import pytest
import os


@pytest.fixture
def data_files(tmp_path):
    paths = []
    for i in range(3):
        for j in range(4):
            data = np.random.random((1000, 1000))
            path = os.path.join(tmp_path, f'sample_{i}_frame_{j}.tif')
            paths.append(path)
            tifffile.imsave(path, data)
    return paths


def test_ingest(data_files):
    docs = list(FileHandlerPlugin.ingestor.ingest(data_files))
    assert len(docs) == 39  # 1 start, 1 descriptor, 12 resources, 12 datums, 12 events, 1 stop
