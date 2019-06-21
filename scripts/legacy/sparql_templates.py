from soweego.wikidata.sparql_queries import ITEM_BINDING, PROPERTY_BINDING

VALUES_QUERY_TEMPLATE = (
    'SELECT * WHERE { VALUES '
    + ITEM_BINDING
    + ' { %s } . '
    + ITEM_BINDING
    + ' %s }'
)
CATALOG_QID_QUERY_TEMPLATE = (
    'SELECT '
    + ITEM_BINDING
    + ' WHERE { wd:%s wdt:P1629 '
    + ITEM_BINDING
    + ' . }'
)
PROPERTIES_WITH_URL_DATATYPE_QUERY = (
    'SELECT '
    + PROPERTY_BINDING
    + ' WHERE { '
    + PROPERTY_BINDING
    + ' a wikibase:Property ; wikibase:propertyType wikibase:Url . }'
)
