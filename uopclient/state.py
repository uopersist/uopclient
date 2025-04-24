from collections import defaultdict
from functools import partial
from sjautils import dicts, decorations
from uop.connect.uop_connect import ConnectionWrapper
from uop.connect import direct
from uopmeta.schemas.predefined import pkm_schema
from uopmeta.schemas import meta
from uopmeta import attr_info
from uopmeta.schemas.meta import as_dict, as_object
import copy
import asyncio

def type_default(a_type):
    info = attr_info.attribute_types[a_type]
    return info.default()

set_dict = lambda: defaultdict(set)


class MetaMappings:
    def __init__(self, meta_items):
        self.id_map = {i['id']: i for i in meta_items}
        self.name_map = {i['name']: i for i in meta_items}
        self.id_to_name = {i['id']: i['name'] for i in meta_items}
        self.name_to_id = {i['name']: i['id'] for i in meta_items}

    def add_meta(self, meta):
        mid, name = meta.id, meta.name
        self.id_map[mid] = meta
        self.name_map[name] = meta
        self.id_to_name[mid] = name
        self.name_to_id[name] = mid

    def remove_meta(self, meta):
        mid, name = meta['id'], meta['name']
        self.id_map.pop(mid, None)
        self.name_map.pop(name, None)
        self.id_to_name.pop(mid, None)
        self.name_to_id.pop(name, None)

def as_object(item):
    if type(item) == dict:
        return dicts.DictObject(**item)
    return item

class ObjectAssociated:
    """
    A lazy cache for associations.  Caches visited assoc_name->object ids
    and visited object_id -> assoc_names
    Relationship is special case in that accon mapping is 
    assoc_name -> object_id -> other_ids where other_ids is a set
    """

    def __init__(self, state, kind, meta_target=set):
        """
        :param connect: ConnectionWrapper
        :param kind: one of "tags", "groups", "roles"
        :param meta_target: type of contents in mappings

        Produces maps
        _by_object: obj_id -> (assoc_name -> object_objects with this association)
        _by_meta: assoc_name -> objects with this association
        NOTE:
        Relationhisp are special case:
        A mapping of role relative to an object (_by_object) is potentially
        two mappings as all relationships are 2-way.
        in _by_order a key->set of both forward and reverse related objects will be
        produced with normal and reverse role name.
        in _by_meta similarly both role name and reversed role name primary mapping
        may be present.
        The value in _by_meta will be a dict of obj -> set(objects) on other side of
        relationship.
        """
        self._state = state
        self._connect:ConnectionWrapper = state.connect
        self._kind = kind
        self._items = [as_object(i) for i in state.cached_items(kind)]
        self._by_object = defaultdict(set_dict)
        self._by_meta = defaultdict(meta_target)
        self._map = MetaMappings(self._items)

    def remove_meta(self, meta):
        known = meta['name'] in self._map.name_map
        if known:
            name = meta['name']
            self._map.remove_meta(meta)
            self._by_meta.pop(name, None)
            for used in self._by_object.values():
                if isinstance(used, set):
                    used.discard(name)
                elif isinstance(used, dict):
                    used.pop(name, None)

    def all_assocs(self):
        return set(self._map.name_to_id.keys())

    def assoc_present(self, assoc):
        oid, name = assoc[:2]
        return (oid in self._by_object) and \
            (name in self._by_meta) and \
            (oid in self._by_meta[name]) and \
            (name in self._by_object[oid])

    def contains(self, assoc):
        return self.assoc_present(assoc)

    def meta_delete_obj(self, oid):
        for name, subs in self.by_meta.items():
            if isinstance(subs, set):
                subs.discard(oid)
            elif isinstance(subs, dict):
                subs.pop(oid, None)
                for k_oid, oids in subs.items():
                    oids.discard(oid)

    def objects_delete_obj(self, oid):
        self.by_object.pop(oid, None)
        for k_oid, meta_objs in self.by_object.items():
            for name, oids in meta_objs.items():
                oids.discard(oid)

    def object_present(self, oid):
        if oid in self.by_object:
            return True
        for k_oid, meta_objs in self._by_object.items():
            for name, oids in meta_objs.items():
                if oid in oids:
                    return True
        for name, subs in self.by_meta.items():
            if isinstance(subs, set):
                if oid in subs:
                    return True
            elif isinstance(subs, dict):
                if oid in subs:
                    return True
                for k_oid, oids in subs.items():
                    if oid in oids:
                        return True
        return False

    def delete_object(self, oid):
        self.meta_delete_obj(oid)
        self.objects_delete_obj(oid)


    @property
    def by_meta(self):
        return self._by_meta

    @property
    def by_object(self):
        return self._by_object

    def get_object(self, oid):
        return self._by_object.get(oid)

    def meta_names(self, assoc_name):
        return [assoc_name, None]

    @decorations.abstract
    def db_all_associated(self):
        "raw sssociation records"
        return []

    def clear(self):
        self._by_object.clear()
        self._by_meta.clear()

    def _meta_neighbors(self, meta_collection, object_id):
        copy = None
        if isinstance(meta_collection, set):
            copy = set(meta_collection)
            copy.discard(object_id)
            return copy
        if isinstance(meta_collection, dict):
            return meta_collection.get(object_id, set())

    def for_object(self, object_id):
        return dicts.get(self._by_object, object_id, self.get_by_object)

    def add_object(self, oid):
        self.for_object(oid) # side affect does the work

    def _meta_without_oid(self, meta, oid):
        if isinstance(meta, set):
            meta.discard(oid)
            return meta
        elif isinstance(meta, dict):
            meta.pop(oid, None)
            for name, oids in meta.items():
                oids.discard(oid)
            return meta

    def object_persisted(self, oid):
        return self._state.is_persisted('objects', oid)

    @decorations.abstract
    def get_by_object(self, object_id):
        pass

    def _mapping(self, obj_id):
        return self._by_object.get(obj_id)

    def ensure_meta(self, meta_name):
        "Returns whether assoc meta item with name already exists."
        meta = self._map.name_map.get(meta_name)
        if not meta:
            meta = self._connect.ensure_meta_named(self._kind, meta_name)
            self._map.add_meta(meta)
        return meta

    def ensure_association(self, oid, meta_name):
        self.ensure_meta(meta_name)
        if oid not in self._by_object:
            self._by_object[oid] = self.get_by_object(oid)



    @decorations.abstract
    def db_associate(self, obj_id, assoc_id, other_obj_id=None):
        pass

    @decorations.abstract
    def db_disassociate(self, obj_id, assoc_id, other_obj_id=None):
        pass

    def _meta_reversed(self, meta_name):
        return False

    def maybe_reorder(self, obj_id, assoc_name, other_obj_id):
        if other_obj_id and self._meta_reversed(assoc_name):
            return other_obj_id, obj_id
        return obj_id, other_obj_id

    def db_associated_objects(self, assoc_id, related_to=None, reverse=False):
        meta = self._map.id_map[assoc_id]
        fn = None
        name = meta.name
        if related_to:
            fn = partial(self.get_by_meta, dict(related_to=related_to))
            if reverse:
                name = meta.reverse_name
        return self.for_meta(name)

    def mod_metas_on_associate(self, obj_id, assoc_name, other_id):
        meta = self.get_meta(assoc_name)


        def add_assoc(name, oid, other_oid):
            fn = self.get_by_meta
            if other_id:
                fn = partial(fn, related_to=oid)


            if other_id:
                named = self.by_meta[name]
                if oid not in named:
                    named[oid] = fn(name)

                named[oid].add(other_oid)
            else:
                named = self.for_meta(name)
                named.add(oid)



        add_assoc(meta.name, obj_id, other_id)
        if other_id:
            add_assoc(meta.reverse_name, other_id, obj_id)




    def get_meta(self, assoc_name):
        return self._map.name_map.get(assoc_name)

    def mod_metas_on_disassociate(self, obj_id, assoc_name, other_id):
        def drop_assoc(meta_data, dropped_id, other_id):
            if meta_data:
                if other_id:
                    meta_data[other_id].discard(dropped_id)
                else:
                    meta_data.discard(dropped_id)

        meta = self.get_meta(assoc_name)
        drop_assoc(self._by_meta.get(meta.name), obj_id, other_id)
        if other_id:
            drop_assoc(self._by_meta.get(meta.reverse_name), other_id, obj_id)


    def associate(self, obj_id, assoc_name, other_obj_id=None):
        """
        Creates new association and interns it properly in self.
        Ensures the association exists.
        Ensures database portion is done and that _by_object is
        updated properly.
        Ensure _by_meta is updated properly.
        :param obj_id:
        Id of object to be tagged, grouped or related to other_object_id
        :param assoc_name: name of the association
        :param other_obj_id: id of other object if we are forming a relationship
        :return: None
        """
        def add_obj_metas():
            pass

        obj_id, other_obj_id = self.maybe_reorder(obj_id, assoc_name, other_obj_id)
        meta = self.ensure_meta(assoc_name)
        mid = meta.id
        self.db_associate(obj_id, mid, other_obj_id)
        self.add_assoc(obj_id, assoc_name, other_obj_id)

    def remove_assoc(self, obj_id, assoc_name, other_obj_id=None):
        obj_data = self._by_object.get(obj_id)
        if obj_data:
            obj_data.pop(assoc_name, None)
        self._by_object[obj_id].pop(assoc_name, None)

    def mod_objects_on_associate(self, oid, name, other):
        meta = self.get_meta(name)
        def from_meta(for_obj, name):
            meta_data = self.by_meta[name]
            if isinstance(meta_data, dict):
                meta_data = meta_data[for_obj]
            return meta_data - {for_obj}

        def fix_object(obj, name, other):
            if not name in self.by_object[obj]:
                self.by_object[obj][name] = from_meta(obj, name)
            elif other:
                self.by_object[obj][name].add(other)
        fix_object(oid, meta.name, other)
        if other:
            fix_object(other, meta.reverse_name, oid)


    def add_assoc(self, obj_id, assoc_name, other_obj_id):
        meta = self.get_meta(assoc_name)
        self.mod_metas_on_associate(obj_id, assoc_name, other_obj_id)
        self.mod_objects_on_associate(obj_id, assoc_name, other_obj_id)

    def disassociate(self, obj_id, assoc_name, other_obj_id=None):
        obj_id, other_obj_id = self.maybe_reorder(obj_id, assoc_name, other_obj_id)
        meta = self.get_meta(assoc_name)
        if meta:
            mid = meta.id
            self.db_disassociate(obj_id, mid, other_obj_id)
            self.remove_assoc(obj_id, meta.name, other_obj_id)
            if other_obj_id:
                self.remove_assoc(other_obj_id, meta.reverse_name, obj_id)
            self.mod_metas_on_disassociate(obj_id, meta.name, other_obj_id)

    @decorations.abstract
    def get_by_meta(self, meta_name, related_to=None):
        pass

    def for_meta (self, meta_name, fn=None):
        fn = fn or self.get_by_meta
        return dicts.get(self._by_meta, meta_name, fn)

class AssociatedTags(ObjectAssociated):

    disassociate_name = 'untag'

    def __init__(self, state):
        super().__init__(state, 'tags')

    def db_all_associated(self):
        return self._connect.tagged.find()


    def db_associated_objects(self, an_id, related_to=None, reverse=False):
        name = self._map.id_to_name[an_id]
        fn = self.get_by_meta
        if related_to:
            fn = partial(fn, related_to=related_to, reverse=reverse)

        return self.for_meta(name, fn)


    def get_by_object(self, object_id):
        res = defaultdict(set)
        if self.object_persisted(object_id):  # is in database
            tags = self._connect.get_object_tags(object_id)
            for tid in tags:
                name = self._map.id_to_name[tid]
                res[name] = self.db_associated_objects(tid)
        return res

    def db_associate(self, obj_id, assoc_id, other_obj_id=None):
        return self._connect.tag(obj_id, assoc_id)

    def db_disassociate(self, obj_id, assoc_id, other_id=None):
        return self._connect.untag(obj_id, assoc_id)

    def get_by_meta(self, meta_name, **kwargs):
        tid = self._map.name_to_id[meta_name]
        return self._connect.get_tagset(tid, recursive=True, **kwargs)


class AssociatedGroups(ObjectAssociated):
    disassociate_name = 'ungroup'

    def __init__(self, context):
        super().__init__(context, 'groups')

    def db_all_associated(self):
        return self._connect.grouped.find()

    def db_associate(self, obj_id, assoc_id, other_obj_id=None):
        return self._connect.group(obj_id, assoc_id)

    def db_disassociate(self, obj_id, assoc_id, other_id=None):
        return self._connect.ungroup(obj_id, assoc_id)


    def get_by_object(self, object_id):
        res = defaultdict(set)
        if self.object_persisted(object_id):
            group_ids = self._connect.get_object_groups(object_id)
            id_names = self._map.id_to_name
            res = dict()
            for gid in group_ids:
                name = id_names[gid]
                associated = self.db_associated_objects(gid)
                res[name] = self._meta_neighbors(associated, object_id)
        return res

    def get_by_meta(self, meta_name, **kwargs):
        meta = self.get_meta(meta_name)
        gid = meta.id
        return self._state.get_groupset(gid, recursive=True)

class Relationships(ObjectAssociated):
    disassociate_name = 'unrelate'

    def __init__(self, context):
        super().__init__(context, 'roles')

    def all_assocs(self):
        assocs = self._map.name_map.values()
        res = set()
        for assoc in assocs:
            res.add(assoc.name)
            res.add(assoc.reverse_name)
        return res

    def assoc_present(self, assoc):
        def reverse_assoc(assoc):
            oid, c_name, other = assoc
            forward, reverse = self.meta_names(assoc[1])
            t_name = reverse if (c_name == forward) else forward
            return other, t_name, oid
        def extended_meta_check(assoc):
            oid, name, other = assoc
            return other in self._by_meta[name][oid]
        def extended_obj_check(assoc):
            oid, name, other = assoc
            return other in self._by_object[oid][name]

        def extended_check(assoc):
            return extended_obj_check(assoc) and extended_meta_check(assoc)

        reversed = reverse_assoc(assoc)
        base_ok = super().assoc_present(assoc)
        rev_base_ok = super().assoc_present(reversed)
        return base_ok and \
            rev_base_ok and \
            extended_check(assoc) and extended_check(reversed)

    def contains(self, assoc):
        return self.assoc_present(assoc)
    def get_meta(self, name):
        meta = self._map.name_map.get(name)
        if not meta:
            for k,v in self._map.name_map.items():
                if v.reverse_name == name:
                    meta = v
                    break
        return meta

    def meta_names(self, assoc_name):
        meta = self.get_meta(assoc_name)
        return meta.name, meta.reverse_name

    def __init__(self, state):
        super().__init__(state, 'roles')
        set_dict = lambda: defaultdict(set)
        self._by_meta = defaultdict(set_dict)

    def db_all_associated(self):
        return self._connect.related.find()

    def _meta_reversed(self, meta_name):
        if not self._map.name_map.get(meta_name):
            if meta_name in self._connect.reverse_role_names():
                return True
            raise Exception(f'{meta_name} in not role name or reverse role name')
        return False


    def _remove_by_object(self, obj_id, assoc_name, other_obj_id=None):
        present = self._mapping(obj_id).get(assoc_name)
        if present:
            self._mapping(obj_id).pop(assoc_name)

    def db_associate(self, obj_id, assoc_id, other_obj_id=None):
        return self._connect.relate(obj_id, assoc_id, other_obj_id)

    def db_disassociate(self, obj_id, assoc_id, other_id=None):
        return self._connect.unrelate(obj_id, assoc_id, other_id)

    def db_associated(self):
        return self._connect.related.find()

    def get_by_object(self, object_id):
        res = defaultdict(set)
        if self.object_persisted(object_id):
            res = self._connect.get_related_by_name(object_id)
        return res

    def get_by_meta(self, meta_name, related_to=None):
        role = self.get_meta(meta_name)
        reversed = meta_name == role.reverse_name
        return self._connect.get_roleset(related_to, role.id, reverse=reversed)

class CachedByNameId(meta.ByNameId):
    original: dict = {}


    def get_changes(self):
        active = self.by_id
        inserted = {k:v for k,v in active.items() if k not in self.original}
        deleted = {k:v for k,v in self.original.items() if k not in active}
        modified = {}
        for k,v in self.original.items():
            v = as_dict(v)
            active_item = active.get(k)
            if not active_item:
                continue
            mod = {}
            for vk, val in as_dict(active_item).items():
                if val != v[vk]:
                    mod[vk] = val
            if mod:
                modified[k] = mod
        return dict(inserted=inserted, deleted=deleted, modified=modified)

    def has_changes(self):
        for k in self.by_id:
            if k not in self.original:
                return True
        for k in self.original:
            if k not in self.by_id:
                return True
        for k,v in self.original.items():
            active = self.by_id.get(k)
            if not active:
                continue
            for vk, val in active.items():
                if val != v.get(vk):
                    return True

    def __setitem__(self, key, value):
        self.add_item(as_object(value))
        self.original[key] = copy.deepcopy(value)

    def __contains__(self, an_id):
        return  an_id in self.by_id

    def clear(self):
        self.original.clear()
        super().clear()

    def __getitem__(self, an_id):
        return self.by_id.get(an_id)

    def get(self, an_id):
        return self.by_id.get(an_id)

    def is_original(self, an_id):
        return an_id in self.original

    def remove_item(self, item):
        super().remove_item(item)

    def delete(self, an_id):
        item = self.get(an_id)
        if item:
            self.remove_item(item)

    def add_originals(self, data):
        for k, v in data.items():
            self.add_original(k, v)


    def add_original(self, an_id, data):
        if an_id not in self.by_id:
            self[an_id] = data

    def modifiable(self, an_id):
        if an_id in self.original:
            return self.by_id[an_id]

    def all(self):
        return self.by_id


    def get_all(self):
        return list(self.by_id.values())

    
    def mods(self):
        res = {}
        for obj in self.get_all():
            oid = obj['id']
            orig = self.original.get(oid)
            diffs = {k:v for k,v in obj.items() if orig.get(k) != v}
            if diffs:
                res[oid] = diffs
        return res

    def inserts(self):
        return {k:v for k,v in self._active.items() if k not in self.original}


class ClientState:
    # TODO need means to track changes to object assocs or derive it from changeset. Or do we??
    @classmethod
    def get_local_pkm_state(cls, dbtype='mongo'):
        loop = asyncio.get_event_loop()
        connect = direct.DirectConnection.connect(dbtype, db_name='pkm_app', schemas=[pkm_schema])
        return cls(ConnectionWrapper(connect))

    def __init__(self, connect:ConnectionWrapper):
        self._connect = connect
        self._context = self._connect.metacontext()
        self._objects = CachedByNameId()
        self._classes = CachedByNameId()
        self._attributes = CachedByNameId()
        self._tags = CachedByNameId()
        self._groups = CachedByNameId()
        self._roles = CachedByNameId()
        self._queries = CachedByNameId()
        self.tags = AssociatedTags(self)
        self.groups = AssociatedGroups(self)
        self.roles = Relationships(self)
        self._meta_context = self.refresh_metacontext()
        self.tagged_objects = partial(self._associated_objects, self.tags)
        self.grouped_objects = partial(self._associated_objects, self.groups)
        self.related_objects = partial(self._associated_objects, self.roles)
        self.object_tag_neighbors = partial(self._object_assocs, self.tags)
        self.object_group_neighbors = partial(self._object_assocs, self.groups)
        self.object_role_neighbors = partial(self._object_assocs, self.roles)

    def refresh_metacontext(self):
        kwargs = {}
        for kind, values in self._connect.meta_map().items():
            cached = getattr(self, f'_{kind}')
            cached.clear()
            cached.add_originals(values)
            if kind != 'objects':
                kwargs[kind] = cached
        return meta.MetaContext(**kwargs)

    @property
    def metacontext(self):
        return self._meta_context


    def add_meta(self, meta):
        key = f'_{meta.kind}'
        cache = getattr(self, key)
        cache.add_item(meta)

    def group_children(self, gid):
        res = set()
        group = self._groups.get(gid)
        name = group.name
        direct_children = {g.id for g in self._groups.get_all() if name in g.contained_in}
        for g_id in direct_children:
            res.add(g_id)
            res |= self.group_children(g_id)
        return res

    def possible_group_parents(self, gid):
        all_gids = set(self._groups.all().keys())
        to_remove = self.group_children(gid) | set([gid])
        return all_gids - to_remove

    def delete_meta(self, meta):
        key = f'_{meta.kind}'
        cache = getattr(self, key)
        cache.delete(meta.id)


    @property
    def has_changes(self):
        cached = (self._objects, self._classes, self._attributes, self._tags,
                  self._groups, self._roles, self._queries)
        for c in cached:
            if c.has_changes():
                return True


    def is_persisted(self, kind, an_id):
        cached = getattr(self, f'_{kind}')
        return cached.is_original(an_id)

    def cached_items(self, kind):
        cached = getattr(self, f'_{kind}')
        persisted = {k: as_dict(v) for k,v in self._connect.id_map(kind).items()}
        cached.add_originals(persisted)
        return list(cached.all().values())

    @property
    def connect(self):
        return self._connect

    def get_meta_editable(self):
        res = {}
        for kind, values in self._connect.meta_map().items():
            res[kind] = CachedByNameId()
            res[kind].add_originals(values)
        return  res

    def metas_of_kind(self, kind):
        return self.metacontext.metas_of_kind(kind)


    def object_present(self, oid):
        if oid in self._objects:
            return True
        for assoc_group in (self.tags, self.groups, self.roles):
            if assoc_group.object_present(oid):
                return True
        return False


    @property
    def context(self):
        return self._connect

    def delete_object(self, oid):
        self.connect.delete_object(oid)
        if oid in self._objects:
            self._objects.delete(oid)
            self.tags.delete_object(oid)
            self.groups.delete_object(oid)
            self.roles.delete_object(oid)

    def queries(self):
        res = dict(self._original_queries)
        res.update(self._live_queries)
        return res

    def new_query(self, name):
        pass

    def get_query(self, name, create_if_missing=True):
        """
        fetch query and ensure it is in live queries for tracking changes
        """
        query = self._live_queries.get(name)
        if not query:
            query = self._original_queries.get(name)
            if query:
                self._live_queries[name] = query

        return query


    def _associated_objects(self, assoc, name):
        return assoc.for_meta(name)

    def _object_assocs(self, assoc_type, oid, names_only=False):
        raw = assoc_type.for_object(oid)
        if names_only:
            return list(raw.keys())
        return raw

    def begin_transaction(self):
        self._connect.begin_transaction()

    def abort(self):
        self._connect.abort()
        self._objects.clear()
        self.txn_clear()

    def push_object_changes(self):
        """
        This type of local state gathers objects modified in transaction at end
        and pushes thes modifications to the db_context.
        :return: None
        """
        changes = self._objects.get_changes()
        for obj in changes['inserted']:
            self.connect.add_object(obj)
        for k,mods in changes['modified'].items():
            self.connect.modify_object(k, mods)
        for k in changes['deleted']:
            self.connect.delete_object(k)


    def push_mods(self):
        """
        Push any modifications not yet pushed to database.
        :return:
        """
        for kind in ['roles', 'groups', 'tags', 'objects']:
            meta = getattr(self, f'_{kind}')
            changes = meta.get_changes()
            for k,mods in changes['modified'].items():
                self.connect.meta_modify(kind, k, **mods)
            for k in changes['deleted']:
                self.connect.meta_delete(kind, k)
            for new_meta in changes['inserted'].values():
                self.connect.meta_insert(new_meta)


    def commit(self):
        self.push_mods()
        self._connect.commit()
        self.txn_clear()

    def txn_clear(self):
        self._objects.clear()
        self.refresh_metacontext()
        self.tags.clear()
        self.groups.clear()
        self.roles.clear()

    def get_object(self, oid):
        known = self._objects.get(oid)
        if not known:
            known = self._connect.get_object(oid)
            if known:
                self._objects[oid] = known
        return known

    def ensure_object(self, oid):
        if oid not in self._objects:
            self.get_object_and_associations(oid)

    def load_objects(self, oids):
        for oid in oids:
            self.ensure_object(oid)

    def load_instances(self, objects):
        for obj in objects:
            if obj['id'] not in self._objects:
                self.add_object(obj)

    def get_object_and_associations(self, oid):
        object = self.get_object(oid)
        self.add_object(object)
        assoc_data = dict(
            tags = self.tags.for_object(oid),
            groups = self.groups.for_object(oid),
            roles = self.roles.for_object(oid)
        )
        return object, assoc_data

    def create_class_instance(self, cls_name, **data) -> dict:
        obj = self._connect.create_instance_of(cls_name, use_defaults=True)
        return self.add_object(obj, is_new=True)

    def add_object(self, object_data, is_new=False):
        # TODO maybe ensure in database here
        oid = object_data['id']
        if oid in self._objects:
            return object_data
        self._objects[oid] = object_data
        if is_new:
            self._objects.original.pop(oid)
        self.tags.add_object(oid)
        self.groups.add_object(oid)
        self.roles.add_object(oid)
        return object_data

    def untag(self, oid, name):
        self.tags.disassociate(oid, name)

    def tag(self, oid, name):
        self.tags.associate(oid, name)

    def group(self, oid, name):
        self.groups.associate(oid, name)

    def ungroup(self, oid, name):
        self.groups.disassociate(oid, name)

    def relate(self, oid, name, other_id):
        self.roles.associate(oid, name, other_id)

    def unrelate(self, oid, name, other_id):
        self.roles.disassociate(oid, name, other_id)


    def __getattr__(self, name):
        return getattr(self._connect, name)

    @property
    def kind_map(self):
        return dict(
            tags = dict(
                neighbors = self.object_tag_neighbors,
                associated = self.tagged_objects,
                add = self.tag,
                remove = self.untag
            ),
            groups = dict(
                neighbors = self.object_group_neighbors,
                associated = self.grouped_objects,
                add = self.group,
                remove = self.ungroup
            ),
            roles = dict(
                neighbors = self.object_role_neighbors,
                associated = self.related_objects,
                add = self.relate,
                remove = self.unrelate
            )

        )

    def add_assocs(self, labeled_assocs):
        for kind, assoc in labeled_assocs:
            self.kind_map[kind]['add'](*assoc)

    def remove_assocs(self, labeled_assocs):
        for kind, assoc in labeled_assocs:
            self.kind_map[kind]['remove'](*assoc)

    def assocs_present(self, labeled_assocs):
        present = True
        for kind, assoc in labeled_assocs:
            target = getattr(self, kind)
            ok = target.assoc_present(assoc)
            if not ok:
                print(f'{kind} assoc {assoc} not present')
                present = False
        return present

    def assoc_obj_present(self, oid):
        present = False
        for kind in ['tags', 'groups', 'roles']:
            target = getattr(self, kind)
            present = present or  target.object_present(oid)
        return present

    def assocs_not_present(self, labeled_assocs):
        not_present = True
        for kind, assoc in labeled_assocs:
            target = getattr(self, kind)
            ok = not target.assoc_present(assoc)
            if not ok:
                print(f'{kind} assoc {assoc} is present')
                not_present = False
        return not_present
