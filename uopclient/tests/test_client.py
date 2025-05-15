from uop.connect.uop_connect import register_adaptor, ConnectionWrapper
from uopclient import state
from uop.connect import direct
from uopmeta.schemas.meta import ByNameId, WorkingContext
from sjautils import dicts
from sqluop import adaptor
from uopmeta.oid import oid_class
import random
from collections import defaultdict

set_dict = lambda: defaultdict(set)
dict_dict = lambda: defaultdict(set_dict)

db_name = f'testdb_{random.randint(10000, 99999)}'
wrapper:ConnectionWrapper = ConnectionWrapper()
local_state:state.ClientState
base_data:WorkingContext

def set_connection(connect):
    global wrapper, base_data, local_state
    wrapper.set_connection(connect)
    base_data = wrapper.dataset(num_assocs=10, persist=True)
    local_state = state.ClientState(wrapper)

class DataSource():
    def __init__(self):
        self.tags = ByNameId()
        self.groups = ByNameId()
        self.roles = ByNameId()
        self.tagged = defaultdict(set) # name -> {oids}
        self.grouped = defaultdict(set) # name -> {oids}
        self.related = defaultdict(set_dict) # name -> oid -> {oids}
        self.object_tags = defaultdict(set_dict) # oid -> name -> {oids}
        self.object_groups = defaultdict(set_dict) # oid -> name -> {oids}
        self.object_related = defaultdict(set_dict) # oid -> name -> {oids}
        self.load_data()

    def associated_oids(self):
        return set(self.object_tags) | set(self.object_groups) | set(self.object_related)

    def assoc_obj_present(self, oid):
        if oid in self.associated_oids():
            return True
        for kind in 'tags', 'groups', 'roles':
            for name_oids in self.kind_map[kind]['object'].values():
                for name, oids in name_oids.items():
                    if oid in oids:
                        return True
            for name, subs in self.kind_map[kind]['meta'].items():
                if oid in subs:
                    return True
                if isinstance(subs, dict):
                    for oids in subs.values():
                        if oid in oids:
                            return True
        return False

    def _assoc_present(self, meta_attr, object_attr, assoc):
        oid, name = assoc[:2]
        return (name in meta_attr) and (oid in meta_attr[name]) and \
            (oid in object_attr) and (name in object_attr[oid])

    def is_tagged(self, assoc):
        return self._assoc_present(self.tagged, self.object_tags, assoc)

    def is_grouped(self, assoc):
        return self._assoc_present(self.grouped, self.object_groups, assoc)

    def is_related(self, assoc):
        oid, name, other = assoc
        base =  self._assoc_present(self.related, self.object_related, assoc)
        return base and (other in self.related[name][oid]) and \
            (other in self.object_related[oid][name] if (other != oid) else True)

    @property
    def kind_map(self):
        return dict(
            tags = dict(
                has_assoc= self.is_tagged,
                meta = self.tagged,
                object = self.object_tags),
            groups = dict(
                has_assoc = self.is_grouped,
                meta = self.grouped,
                object = self.object_groups),
            roles = dict(
                has_assoc = self.is_related,
                meta = self.related,
                object = self.object_related)
        )

    def assocs_present(self, assocs):
        all_ok = True
        for kind, assoc in assocs:
            ok = self.kind_map[kind]['has_assoc'](assoc)
            if not ok:
                all_ok = False
                print(f'{kind} assoc {assoc} is not present')
        return all_ok

    def assocs_not_present(self, assocs):
        all_ok = True
        for kind, assoc in assocs:
            ok = not self.kind_map[kind]['has_assoc'](assoc)
            if not ok:
                all_ok = False
                print(f'{kind} assoc {assoc} is present')
        return all_ok

    def load_metas(self):
        pass

    def load_associated(self):
        pass



    def load_data(self):
        self.load_metas()
        self.load_associated()

class SubsetOK:
    def __init__(self, keys=True, subs=True):
        self.keys = keys
        self.sets = subs

class EquivalenceCheck(SubsetOK):
    def __init__(self, source1:DataSource, source2:DataSource, key_subset_ok=True, set_subset_ok=False):
        super().__init__(key_subset_ok, set_subset_ok)
        self._source1 = source1
        self._source2 = source2


    def set_compare(self, set1, set2, subsets=None):
        subsets = subsets or self
        diff = set1 - set2
        if diff:
            if subsets.sets:
                return not (set2 & diff)  # set2 must not have elements not in set1
            return False
        return True

    def dict_compare(self, d1, d2, subsets=None):
        subsets = subsets or self
        s1, s2 = set(d1.keys()), set(d2.keys())
        ok = self.set_compare(s1, s2)
        for k,v in d1.items():
            if k not in d2:
                if subsets.keys:
                    continue
                return False
            v2 = d2[k]
            if not self._compare_pair(v, v2, subsets):
                return False
        return True



    def by_name_id_compare(self, b1, b2):
        def as_name_dict(bni):
            raw = bni.by_name
            _, v = dicts.first_kv(raw)
            if isinstance(v, ByNameId):
                return {k: v.dict() for k,v in raw.items()}
            return raw

        d1, d2 = as_name_dict(b1), as_name_dict(b2)
        return self.dict_compare(d1, d2, SubsetOK(False, False))

    def _pair(self, name):
        return getattr(self._source1, name), getattr(self._source2, name)

    def _compare_pair(self, p1, p2, subsets=None):
        subsets = subsets or self
        if isinstance(p1, set):
            return self.set_compare(p1, p2, subsets)
        elif isinstance(p1, ByNameId):
            return self.by_name_id_compare(p1, p2)
        elif isinstance(p1, dict):
            return self.dict_compare(p1, p2, subsets)
        else:
            return p1 == p2

    def __call__(self):
        for meta in 'tags', 'groups', 'roles':
            d1, d2 = self._pair(meta)
            assert self._compare_pair(d1, d2)
        for associated in 'tagged', 'grouped', 'related':
            d1, d2 = self._pair(associated)
            assert self._compare_pair(d1, d2)

def set_without(s, item):
    return s - {item}

class FromDB(DataSource):

    def load_all_associated(self, fn, assocs, assoc_objects, object_assocs):

        for name, meta in assocs.by_name.items():
            raw = fn(meta.id) # set of oids
            if not raw:
                continue

            assoc_objects[name] = raw
            for oid in raw:
                object_assocs[oid][name] = raw - {oid}


    def load_metas(self):
        self.tags = base_data.tags
        self.groups = base_data.groups
        self.roles = base_data.roles

    def load_related(self):
        for name, meta in self.roles.by_name.items():
            forward, reverse = wrapper.get_role_related(meta.id)
            self.related[name] = forward
            self.related[meta.reverse_name] = reverse
            for oid, oids in forward.items():
                self.object_related[oid][meta.name] = oids - {oid}
            for oid, oids in reverse.items():
                self.object_related[oid][meta.reverse_name]= oids - {oid}


    def load_associated(self):
        self.load_all_associated(wrapper.get_tagset, self.tags, self.tagged, self.object_tags)
        self.load_all_associated(wrapper.get_groupset, self.groups, self.grouped, self.object_groups)
        self.load_related()

    def load_data(self):
        self.load_metas()
        self.load_associated()

class FromWorkingContext(DataSource):
    def __init__(self, wd: WorkingContext):
        self._data = wd
        super().__init__()

    def load_metas(self):
        self.tags = self._data.tags
        self.groups = self._data.groups
        self.roles = self._data.roles

    def load_relations(self):
        id_map = wrapper.id_map('roles')
        for assoc in self._data.related:
            meta = id_map[assoc.assoc_id]
            subject = assoc.subject_id
            object = assoc.object_id
            self.related[meta.name][subject].add(object)
            self.related[meta.reverse_name][object].add(subject)
            self.object_related[subject][meta.name].add(object)
            self.object_related[object][meta.reverse_name].add(subject)

    def load_associated(self):
        def to_sets(kind, assocs):
            res = defaultdict(set)
            id_to_name = wrapper.id_to_name(kind)
            for assoc in assocs:
                name = id_to_name(assoc.assoc_id)
                res[name].add(assoc.object_id)
            return res

        def object_assocs(assoc_objects):
            res = defaultdict(set_dict)
            for name, oids in assoc_objects.items():
                for oid in oids:
                    res[oid][name] = oids - {oid}
            return res

        def assoc_pair(kind, source):
            metas = to_sets(kind, source)
            return metas, object_assocs(metas)

        self.tagged, self.object_tags = assoc_pair('tags', self._data.tagged)
        self.grouped, self.object_groups = assoc_pair('groups', self._data.grouped)
        self.load_relations()


class FromState(DataSource):
    def __init__(self, client: state.ClientState):
        self._state = client
        super().__init__()

    def load_metas(self):
        mc = self._state.metacontext   # TODO should include uncommitted?
        self.tags = mc.tags
        self.groups = mc.groups
        self.roles = mc.roles

    def load_associated(self):
        def load_assocs(assoc_objects, object_assocs, source):
            for name, data in source.by_meta.items():
                if isinstance(data, set):
                    assoc_objects[name] = set(data)
                else:
                    assoc_objects[name] = dict(data)
            for oid, data in source.by_object.items():
                for name, oids in data.items():
                    object_assocs[oid][name] = set(oids)


        load_assocs(self.tagged, self.object_tags, self._state.tags)
        load_assocs(self.grouped, self.object_groups, self._state.groups)
        load_assocs(self.related, self.object_related, self._state.roles)

def is_related_ds(source, assoc):
    reverse_assoc = wrapper.reverse_relation(assoc)
    return source.is_related(assoc) and source.is_related(reverse_assoc)

def not_related_ds(source, assoc):
    reverse_assoc = wrapper.reverse_relation(assoc)
    return not (source.is_related(assoc) or source.is_related(reverse_assoc))



def check_persisted():
    "check that all of base_data made it to the database"

    EquivalenceCheck(FromDB(), FromWorkingContext(base_data))

def check_associate():
    local_state.abort() # clear previous test data
    local_state.begin_transaction()
    from_working = FromWorkingContext(base_data)

    def get_free(kind, object_assocs, associated):
        def bound_metas():
            return {a.assoc_id for a in associated}
        bound = set(object_assocs.keys())
        oids = {o['id'] for o in base_data.instances}
        free = oids
        metas = {i for i in getattr(base_data, kind).by_name}
        free_metas = metas - bound_metas()
        if not (free_metas and free):
            return None
        else:
            oid, name = random_member(free), random_member(free_metas)
            t = (oid, name, random_member(free)) if (kind == 'roles') else (oid, name)
            return kind, t

    params = []
    def add_param(kind, object_assoc, data_assoc):
        pick = get_free(kind, object_assoc, data_assoc)
        if pick:
            params.append(pick)
        else:
            print(f'found nothing for {kind}')

    add_param('tags', from_working.object_tags, base_data.tagged)
    add_param('groups', from_working.object_groups, base_data.tagged)
    add_param('roles', from_working.object_related, base_data.related)

    name_map = wrapper.name_map('roles')
    p = params[-1][1]
    meta = name_map[p[1]]
    params.append(('roles',(p[2], meta.reverse_name, p[0])))

    local_state.add_assocs(params)
    assert local_state.assocs_present(params)
    assert FromState(local_state).assocs_present(params)

    local_state.commit()
    assert FromDB().assocs_present(params)

def random_member(what):
    return random.choice(list(what))

def check_disassociate():
    from_db = FromDB()
    local_state.abort()
    local_state.begin_transaction()
    oids = from_db.associated_oids()
    local_state.load_objects(oids)

    def check_not_associated(kind, assoc):
        target = getattr(local_state, kind)
        assert not target.assoc_present(assoc)


    def random_assoc(kind, *disallowed_keys):
        source = from_db.kind_map[kind]['meta']
        usable = lambda k: len(source[k]) and (k not in disallowed_keys)
        allowed_keys = [k for k in source.keys() if usable(k)]
        name = random_member(allowed_keys)
        subs = source[name]
        if isinstance(subs, dict):
            oid = random_member(subs.keys())
            oid2 = random_member(subs[oid])
            return kind, (oid, name, oid2)
        else:
            oid = random_member(subs)
            return kind, (oid, name)

    param_name = lambda assoc: assoc[1][1]
    params = [random_assoc(n) for n in ('tags', 'groups', 'roles')]
    related_name = param_name(params[2][1])
    rev_related = random_assoc('roles', related_name)
    rev_name = param_name(rev_related)
    rev_related = wrapper.reverse_relation(rev_related[1])
    params.append(('roles', rev_related))
    local_state.remove_assocs(params)
    assert local_state.assocs_not_present(params)
    assert FromState(local_state).assocs_not_present(params)

    local_state.commit()
    assert FromDB().assocs_not_present(params)

def check_deletes():
    local_state.abort()
    oids = [o['id'] for o in base_data.instances]
    assoc_oids = base_data.assoc_oids()
    local_state.begin_transaction()
    local_state.load_objects(oids)
    to_delete = random.sample(list(assoc_oids),3)
    for oid in to_delete:
        local_state.delete_object(oid)
        assert not local_state.object_present(oid)
    local_state.commit()
    from_db = FromDB()
    for oid in to_delete:
        assert not from_db.assoc_obj_present(oid)

def check_mods():
    local_state.abort()
    local_state.begin_transaction()
    local_state.load_instances(base_data.instances)
    desc = wrapper.class_named('DescribedComponent')
    sub_ids = wrapper.metacontext().subclasses(desc.id)
    oids = [b['id'] for b in base_data.instances]
    oid_map = {b['id']:b for b in base_data.instances}
    moids = [oid for oid in oids if oid_class(oid) in  sub_ids]
    to_mod = random.sample(moids, 3)
    for mod_id in to_mod:
        obj = local_state.get_object(mod_id)
        mod = oid_map[mod_id]
        obj['description'] = 'modded object'
        assert(mod['description'] != obj['description'])
    local_state.commit()
    local_state.load_objects(oids)
    for mod_id in to_mod:
        obj = local_state.get_object(mod_id)
        assert obj['description'] == 'modded object'

def check_fetches():
    oids = {o['id'] for o in base_data.instances}
    local_state.load_objects(oids)
    EquivalenceCheck(FromWorkingContext(base_data), FromState(local_state))()

def run_state_tests():

    check_persisted()  # check newly created state was properly persisted
    check_fetches() # check what is in database against what is put in state when fetched
    check_associate()  # create more objects and associations and check correctness
    check_disassociate()  # remove some associations and check correctness
    check_mods()  # modify some objects, persist, and check correctness
    check_deletes() # delete some objects and check correct propagation in state and database


async def test_with_sqlite():
    register_adaptor(adaptor.AlchemyDatabase, 'sqlite')
    dc = await direct.DirectConnection.get_connection('sqlite', db_name)
    set_connection(dc)
    run_state_tests()