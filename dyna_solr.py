import logging
import pysolr
import sys
from abc import ABCMeta
from datetime import datetime
from dateutil.parser import parse as parse_datetime
from dateutil.tz import tzutc

VERSION = (0,0,2)
__version__ = '.'.join([str(i) for i in VERSION])
LOG = logging.getLogger(__name__)


AND = ' AND '
OR = ' OR '


class Config(dict):

    _configured = False

    def from_dict(self, **conf):
        self.update(conf)
        self._configured = True

    def from_object(self, obj):
        for key in dir(obj):
            if key.isupper():
                self[key] = getattr(obj, key)
        self._configured = True

    def is_configured(self):
        return self._configured


class ConfigurableSolr(object):

#    __metaclass__ = ?
    _api_methods = ('search', 'more_like_this', 'suggest_terms', 'add', 'delete', 'commit', 'optimize', 'extract')

    def __init__(self, config=None):
        self.config = config or Config()

    @property
    def index(self):
        if not self.config.is_configured():
            raise Exception('Attempt to access solr without configuration')

        elif not hasattr(self, '_solr'):
            try:
                url = self.config['URL']
            except KeyError:
                raise Exception('Sorl config must at least contain URL')
            else:
                decoder = self.config.get('DECODER')
                timeout=self.config.get('TIMEOUT', 60)
                self._solr = pysolr.Solr(url, decoder=decoder, timeout=timeout)

        return self._solr


solr = ConfigurableSolr()


class Query(dict):

    def __init__(self, document_class=None, **kwargs):
        super(Query, self).__init__(**kwargs)
        self.document_class = document_class
        self.facet_fields = {}

    def __getattr__(self, key):
        if key in self.keys():
            return self[key]
        else:
            return super(Query, self).__getattribute__(key)

    def __setattr__(self, key, value):
        if key in self.keys():
            self[key] = value
        else:
            return super(Query, self).__setattr__(key, value)

    def __iter__(self):
        result = self._select()
        return iter(result.docs)

    def __getitem__(self, item):
        if isinstance(item, int):
            i = int(item)
            clone = self._clone()
            return clone[i:i+1].docs[0]
        else:
            return super(Query, self).__getitem__(item)

    def __getslice__(self, i, j):
        clone = self._clone()

        if i > 0:
            clone['start'] = i

        if j != sys.maxint:
            clone['rows'] = j

        return clone._select()

    def _clone(self):
        clone = Query(self.document_class, **self)
        clone.facet_fields = self.facet_fields
        return clone

    def _select(self):
        # TODO: add filter query for doc type

        result = solr.index.search(self.pop('q') or '*:*', **self)

        if 'group' in self:
            # Build result.docs from simple or grouped groups
            documents = []
            simple = self['group.format'] == 'simple'
            for grouped in result.grouped.values():
                if simple:
                    for document in grouped['doclist']['docs']:
                        documents.append(document)
                else:
                    for group in grouped['groups']:
                        for document in group['doclist']['docs']:
                            documents.append(document)
        else:
            documents = result.docs

        # Wrap raw doc with Document type
        result.docs = []
        for document in documents:
            if 'doc_type_s' in document:
                Doc = DocumentType.get(document['doc_type_s'])
                result.docs.append(Doc(document))
            else:
                LOG.warning('No doc_type field for document %s, skipping.', result['id'])

        # Prettify facets
        if 'facet' in self:
            facets = result.facets.get('facet_fields', {})
            result.facets = {}
            for field_name, counts in facets.iteritems():
                name = self.facet_fields[field_name].name
                facets = [(facet, count) for facet, count in zip(counts[::2], counts[1::2])]
                result.facets[name] = facets

        return result

    def _get_field(self, field):
        if isinstance(field, Field):
            return field
        elif isinstance(field, basestring) and self.document_class:
            return self.document_class.field(field)
        else:
            raise Exception('Unknown field %s for Query, no Document reference' % field)

    def _sort_syntax(self, *fields):
        for field in fields:
            order = 'asc'
            if field.startswith('-'):
                field = field[1:]
                order = 'desc'
            field_name = self._get_field(field).field_name
            yield ' '.join((field_name, order))

    @property
    def q(self):
        return self.get('q', str) or u''

    def get(self, key, default=None):
        if isinstance(default, type):
            if key in self:
                value = self[key]
            else:
                value = default()
                self[key] = value
            return value
        else:
            return super(Query, self).get(key, default)

    def filter(self, **kwargs):
        return self.filter_and(**kwargs)

    def filter_and(self, **kwargs):
        return self._extend_query(AND, **kwargs)

    def filter_or(self, **kwargs):
        return self._extend_query(OR, **kwargs)

    def exclude(self, **kwargs):
        return self._extend_query(AND, negate=True, **kwargs)

    def _extend_query(self, operator, negate=False, **kwargs):
        clone = self._clone()
        query = clone.q

        if query:
            query = '(%s)%s' % (query, operator)

        filter_fields = []
        cond_format = '-%s:%s' if negate else '%s:%s'
        for key, value in kwargs.iteritems():
            if ' ' in value:
                value = '"%s"' % value
            field = self._get_field(key)
            field_name = field.field_name if field else key
            filter_fields.append(cond_format % (field_name, value or '""'))

        filter_fields = operator.join(filter_fields)
        if query:
            filter_fields = '(%s)' % filter_fields

        clone['q'] = query + filter_fields

        return clone

    def facet(self, *fields, **kwargs):
        if not fields:
            return self

        clone = self._clone()
        clone['facet'] = 'true'

        facet_field = clone.get('facet.field', list)
        for field in fields:
            field = clone._get_field(field)
            clone.facet_fields[field.field_name] = field
            facet_field.append(field.field_name)

        sort = kwargs.pop('sort', False)
        if sort:
            clone['facet.sort'] = 1

        for k, v in kwargs.iteritems():
            clone['facet.%s' % k] = v

        return clone

    def group_by(self, *fields, **kwargs):
        if not fields:
            return self

        clone = self._clone()
        clone['group'] = 'true'

        group_field = clone.get('group.field', list)
        for field in fields:
            group_field.append(self._get_field(field).field_name)

        sort_fields = kwargs.pop('sort', None)
        if sort_fields:
            group_sort = clone.get('group.sort', list)
            if not hasattr(sort_fields, '__iter__'):
                sort_fields = [sort_fields]
            group_sort.extend(clone._sort_syntax(*sort_fields))

        facet = kwargs.pop('facet', False)
        if facet:
            clone['group.facet'] = 'true'

        format = kwargs.pop('format', 'simple')
        clone['group.format'] = format

        return clone

    def order_by(self, *fields):
        if not fields:
            return self

        clone = self._clone()
        sort = clone.get('sort', list)
        sort.extend(self._sort_syntax(*fields))

        return clone

    def search(self, q=None, operator=AND):
        """
        Optionally set/override q parameter and triggers index search
        """
        clone = self._clone()

        if clone.q:
            clone['q'] = '(%s)%s(%s)' % (clone.q, operator, q)
        else:
            clone['q'] = q

        return clone

    def all(self):
        clone = self._clone()
        clone['q'] = None
        return clone

    def add(self, *data):
        Doc = self.document_class
        docs = []
        for d in data:
            if isinstance(d, dict):
                d = Doc(d)
            docs.append(d)
        solr.index.add(docs)


def dig_bases(*bases):
    mro = list(bases)
    for clazz in bases:
        mro.extend(dig_bases(*clazz.__bases__))
    return mro


class Field(object):

    _dynamic_types = {
        'i': int,
        's': unicode,
        't': unicode,
        'dt': datetime,
    }

    dynamic_suffix = None

    def __init__(self, type=None, dynamic=True):
        if dynamic:
            self.type = self._dynamic_types[self.dynamic_suffix]
        elif type:
            self.type = type
        self.dynamic = dynamic

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)

    def parse(self, value):
        return value

    @property
    def field_name(self):
        if self.dynamic:
            return self.dynamic_name
        else:
            return self.name


class IntegerField(Field):
    dynamic_suffix = 'i'

class CharField(Field):
    dynamic_suffix = 's'

class TextField(Field):
    dynamic_suffix = 't'

class DateField(Field):
    dynamic_suffix = 'dt'

    def parse(self, value):
        if isinstance(value, basestring):
            return parse_datetime(value).replace(tzinfo=tzutc())
        else:
            return super(DateField, self).parse(value)


class DocumentMeta(object):

    def __init__(self, fields):
        self.fields = {unicode(name): field for name, field in fields.iteritems()}
        self.dynamic_fields = {}

        for name, field in fields.iteritems():
            field.name = name
            if field.dynamic:
                field.dynamic_name = u'_'.join((name, field.dynamic_suffix))
                self.dynamic_fields[field.dynamic_name] = field

        self.all = dict(self.fields)
        self.all.update(self.dynamic_fields)

    def __repr__(self):
        return self.fields.__repr__()

    def get_field(self, name):
        if name in self.all:
            return self.all[name]

    def get_field_name(self, name):
        return self.get_field(name).field_name


class DocumentType(ABCMeta):

    _doc_types = dict()

    def __new__(mcls, name, bases, namespace):
        # Reversed super classes, except dict and object
        mro = dig_bases(*bases)[:-2][::-1]

        # Fetch fields from parents
        fields = dict()
        for clazz in filter(lambda c: hasattr(c, '_meta'), mro):  # Reversed super classes, except dict and object
            fields.update(clazz._meta.fields)

        # Fetch class fields
        class_fields = {attr: value for attr, value in namespace.iteritems() if isinstance(value, Field)}
        fields.update(class_fields)

        # Remove declared fields
        for key in class_fields.keys():
            del namespace[key]

        # Init meta with fields
        namespace['_meta'] = DocumentMeta(fields)

        # Create document class
        cls = super(DocumentType, mcls).__new__(mcls, name, bases, namespace)
        cls.docs = Query(cls)

        # Register document type
        mcls._doc_types[name] = cls

        return cls

    def __call__(cls, *args, **kwargs):
        return super(DocumentType, cls).__call__(*args, **kwargs)

    @classmethod
    def get(mcls, type):
        return mcls._doc_types[type]


class Document(dict):

    __metaclass__ = DocumentType

    id = CharField(dynamic=False)
    doc_type = CharField()
    docs = Query()  # Placeholder set by metaclass

    def __init__(self, document=None, **kwargs):
        # Default all fields to None
        defaults = {field.field_name: None for field in self._meta.fields.values()}
        super(Document, self).__init__(defaults)

        # Auto set doc_type
        self.doc_type = self.__class__.__name__

        # Init fields with document data
        self.raw = {}
        self._set_fields(document)
        self._set_fields(kwargs)

    def _set_fields(self, data):
        if data:
            self.raw.update(data)
            for name, value in data.iteritems():
                field = self._meta.get_field(name)
                if field:
#                    self.raw[field.field_name] = value
                    self[field.field_name] = field.parse(value)

    def __getattr__(self, key):
        field = self._meta.all.get(key)
        if field:
            return self[field.field_name] if field.field_name in self else None
        else:
            return super(Document, self).__getattribute__(key)

    def __setattr__(self, key, value):
        field = self._meta.all.get(key)
        if field:
            self[field.field_name] = value
        else:
            super(Document, self).__setattr__(key, value)

    def get(self, k, d=None):
        return getattr(self, k, d)

    @classmethod
    def field(cls, name):
        return cls._meta.get_field(name)

    def _json_value(self, field):
        value = self[field]
        if isinstance(value, datetime):
            return self.raw[field]
        else:
            return value

    def jsonify(self):
        return {self._meta.get_field(field).name: self._json_value(field) for field in self}

    def save(self):
        solr.index.add([self])