DOCUMENT_CANONICAL_TYPES = """
	SELECT DISTINCT a.document_id, b.type FROM
	annotated_docs a JOIN entity_types b
	ON a.entity = b.entity
	WHERE a.document_id = '{document_id}'
"""

FILTER_QUERIES = """
	SELECT * FROM queries WHERE
	target_type IN {types}
"""

DOCUMENT_ENTITY_CANONICAL_TYPES = """
	SELECT * FROM
	annotated_docs a JOIN entity_types b
	ON a.entity = b.entity
	WHERE a.document_id = '{document_id}'
"""

WORD_IDF = """
	SELECT * FROM word_idf
	WHERE word in {words}
"""