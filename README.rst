dyna-solr
=========

Simple queryset-like api for pysolr with dynamic field support.

Configure
---------

.. code:: python

    from dyna_solr import solr
    solr.config.from_dict(URL='http://127.0.0.1:8983/solr/')

Documents
---------

.. code:: python

    from dyna_solr import Document, CharField, DateField

    class Book(Document):

        title = CharField()
        author = CharField()
        genre = CharField()
        pub_date = DateField()

Query
-----

Document queries are chained...

.. code:: python

    # Iterate all books
    for book in Book.docs.all():
        print book.title

    books = Book.docs.all()

    # Filter
    books = books.filter(author='Jonas')

    # Facet
    books = books.facet('author')

    # Group by field and optionally affect facets
    books = books.group_by('genre', facet=True)

    # Offset and limit
    books = books[5:10]
