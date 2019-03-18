import event_model
import intake
import intake.catalog
import intake.catalog.local
import intake.source.base
import re
from functools import partial

class FilesCatalog(intake.catalog.Catalog):
    def __init__(self, file_list, handler, *,
                 query=None, **kwargs):
        """
        This Catalog is backed by a newline-delimited JSON (jsonl) file.

        Each line of the file is expected to be a JSON list with two elements,
        the document name (type) and the document itself. The documents are
        expected to be in chronological order.

        Parameters
        ----------
        file_list : list
            list of filepaths
        handler_registry : dict, optional
            Maps each asset spec to a handler class or a string specifying the
            module name and class name, as in (for example)
            ``{'SOME_SPEC': 'module.submodule.class_name'}``.
        **kwargs :
            Additional keyword arguments are passed through to the base class,
            Catalog.
        """
        name = 'bluesky-files-catalog'  # noqa
        self._runs = {}  # This maps run_start_uids to file paths.
        self._run_starts = {}  # This maps run_start_uids to run_start_docs.

        self._query = query or {}
        self.handler = handler


        self.filler = event_model.Filler({handler.name: handler})
        self._update_index(file_list)
        super().__init__(**kwargs)

    frame_pattern = r"(?P<base>^|\r?\n|.*_|.*?\.)(?P<frame>\d{1,}).*(?=\..*?)"

    @classmethod
    def _parse_frame_num(cls, path):

        match = re.match(cls.frame_pattern,path)
        if match:
            return match.group('base'), match.group('frame')
        return None, None

    @classmethod
    def _separate_runs(cls, file_list):
        runs = dict()
        for path in file_list:
            base, frame = cls._parse_frame_num(path)
            if base in runs:
                runs[base].append(path)
            else:
                runs[base]=[path]
        return runs.values()


    def _update_index(self, file_list):
        # Split files into unique runs
        file_list_by_runs = self._separate_runs(file_list)

        runs_list = [list(self.handler.ingest(files)) for files in file_list_by_runs]
        self._runs = {run[0][1]['uid']:run for run in runs_list}
        self._run_starts = {run[0][1]['uid']:run[0][1] for run in runs_list}

    def _get_run_stop(self, run_start_uid):
        name, doc = self._runs[run_start_uid][-1]
        if name == 'stop':
            return doc
        else:
            return None

    def _get_event_descriptors(self, run_start_uid):
        for name, doc in self._runs[run_start_uid]:
            if name == 'descriptor':
                yield doc
        # return descriptors

    def _get_event_cursor(self, run_start_uid, descriptor_uids, skip=0, limit=None):
        skip_counter = 0
        descriptor_set = set(descriptor_uids)
        for name, doc in self._runs[run_start_uid]:
            if name == 'event' and doc['descriptor'] in descriptor_set:
                if skip_counter >= skip and (limit is None or skip_counter < limit):
                    yield doc
                skip_counter += 1
                if limit is not None and skip_counter >= limit:
                    break

    def _get_event_count(self, run_start_uid, descriptor_uids):
        event_count = 0
        descriptor_set = set(descriptor_uids)
        for name, doc in self._runs[run_start_uid]:
            if name == 'event' and doc['descriptor'] in descriptor_set:
                event_count += 1
        return event_count

    def _get_resource(self, run_start_uid, uid):
        for name, doc in self._runs[run_start_uid]:
            if name == 'resource' and doc['uid'] == uid:
                return doc
        raise ValueError(f"Resource uid {uid} not found.")

    def _get_datum(self, run_start_uid, datum_id):
        for name, doc in self._runs[run_start_uid]:
            if name == 'datum' and doc['datum_id'] == datum_id:
                return doc
        raise ValueError(f"Datum_id {datum_id} not found.")

    def _get_datum_cursor(self, run_start_uid, resource_uid, skip=0, limit=None):
        skip_counter = 0
        for name, doc in self._runs[run_start_uid]:
            if name == 'datum' and doc['resource'] == resource_uid:
                if skip_counter >= skip and (limit is None or skip_counter < limit):
                    yield doc
                skip_counter += 1
                if limit is not None and skip_counter >= limit:
                    return

    def _make_entries_container(self):
        catalog = self

        class Entries:
            "Mock the dict interface around a MongoDB query result."

            def _docs_to_entry(self, run_start_doc):
                uid = run_start_doc['uid']
                entry_metadata = {'start': run_start_doc,
                                  'stop': catalog._get_run_stop(uid)}

                args = dict(get_run_start=lambda *_, **__: run_start_doc,
                            get_run_stop=partial(catalog._get_run_stop, uid),
                            get_event_descriptors=partial(catalog._get_event_descriptors, uid),
                            get_event_cursor=partial(catalog._get_event_cursor, uid),
                            get_event_count=partial(catalog._get_event_count, uid),
                            get_resource=partial(catalog._get_resource, uid),
                            get_datum=partial(catalog._get_datum, uid),
                            get_datum_cursor=partial(catalog._get_datum_cursor, uid),
                            filler=catalog.filler)
                return intake.catalog.local.LocalCatalogEntry(
                    name=uid,
                    description={},  # TODO
                    driver='intake_bluesky_files.FilesCatalog',
                    direct_access='forbid',  # ???
                    args=args,
                    cache=None,  # ???
                    parameters=[],
                    metadata=entry_metadata,
                    catalog_dir=None,
                    getenv=True,
                    getshell=True,
                    catalog=catalog)

            def __iter__(self):
                yield from self.keys()

            def keys(self):
                yield from catalog._runs

            def values(self):
                for run_start_doc in catalog._run_starts.values():
                    yield self._docs_to_entry(run_start_doc)

            def items(self):
                for uid, run_start_doc in catalog._run_starts.items():
                    yield uid, self._docs_to_entry(run_start_doc)

            def __getitem__(self, name):
                # If this came from a client, we might be getting '-1'.
                try:
                    name = int(name)
                except ValueError:
                    pass
                if isinstance(name, int):
                    raise NotImplementedError
                else:
                    run_start_doc = catalog._run_starts[name]
                    return self._docs_to_entry(run_start_doc)

            def __contains__(self, key):
                return key in catalog._runs

        return Entries()

    def search(self, query):
        """
        Return a new Catalog with a subset of the entries in this Catalog.

        Parameters
        ----------
        query : dict
        """
        if self._query:
            query = {'$and': [self._query, query]}
        cat = type(self)(
            jsonl_filelist=list(self._runs.values()),
            query=query,
            name='search results',
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options)
        return cat

