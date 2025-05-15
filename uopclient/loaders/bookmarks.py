from collections import defaultdict, deque
import json


class JSONLoader:
    def __init__(self, path, *filters):
        self._path = path
        with open(path) as f:
            self._data = json.load(f)
        self._by_title = self.by_title()
        self.prefix_merge_titles(self._by_title, filters)
        self._by_url = self.by_url()

    def prefix_merge_titles(self, filters):
        grouping = defaultdict(list)
        ordered = []
        for f in filters:
            grouping[filter[0]].append(filter)
        for group, g_filters in grouping.items():
            if len(g_filters) > 1:
                s_order = sorted(g_filters, key=lambda g: len(g))[::-1]
                for f in s_order:
                    ordered.append(f)
            else:
                ordered.append(g_filters)
        def heal_dict(the_keys, rest):
            the_dict = rest
            for key in the_keys[::-1]:
                rest = {key: rest}
            self._by_title.update(rest)


        to_merge = []
        for f in ordered:
            working = self._by_title
            keys = []
            for k in f[:-1]:
                working = working.get(k, None)
                if not next:
                    break
            if working:
                working = working.pop(f[-1], None)
                if working:
                    self._by_title.update(working)


    def load(self, dbi):
        pass

    def by_title(self):
        "index by title for items that have children"
        res = {}

        def handle_one(item, in_dict):
            title = item.get('title')
            children = item.get('children')
            if title.startswith('Imported '):
                return
            if children:
                contents = {}
                for child in children:
                    handle_one(child, contents)
                in_dict[title] = contents
            elif 'uri' in item:
                in_dict[title] = item

        handle_one(self._data, res)
        return res

    def by_url(self):
        "url based dict of title paths"
        res = defaultdict(list)
        path = deque()

        def walk_path(data):
            def wrapped_walk(last):
                path.append(k)
                walk_path(v)
                path.pop(k)

            uri = data.get('uri')
            if uri:
                res[uri].append('/'.join(path))
            else:
                for k, v in data.items():

                    tc = v.get('typeCode')
                    if tc and tc > 1:
                        continue  # empty container
                    if (v.get('uri')):
                        walk_path(v)
                    else:
                        path.append(k)
                        walk_path(v)
                        path.pop()

        walk_path(self._by_title)
        return res


class HTMLLoader:
    def __init__(self, path):
        self._path = path
        with open(path) as f:
            self._data = f.read()\


