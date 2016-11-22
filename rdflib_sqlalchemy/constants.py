"""Constant definitions"""
from rdflib import BNode, Literal, URIRef, Variable


COUNT_SELECT = 0
CONTEXT_SELECT = 1
TRIPLE_SELECT = 2
TRIPLE_SELECT_NO_ORDER = 3

ASSERTED_NON_TYPE_PARTITION = 3
ASSERTED_TYPE_PARTITION = 4
QUOTED_PARTITION = 5
ASSERTED_LITERAL_PARTITION = 6

FULL_TRIPLE_PARTITIONS = [QUOTED_PARTITION, ASSERTED_LITERAL_PARTITION]

INTERNED_PREFIX = "kb_"

TERM_COMBINATIONS = dict([(term, index) for index, term in enumerate([
     "UUUU", "UUUB", "UUUF", "UUVU", "UUVB", "UUVF", "UUBU", "UUBB", "UUBF",
     "UULU", "UULB", "UULF", "UUFU", "UUFB", "UUFF",
     #
     "UVUU", "UVUB", "UVUF", "UVVU", "UVVB", "UVVF", "UVBU", "UVBB", "UVBF",
     "UVLU", "UVLB", "UVLF", "UVFU", "UVFB", "UVFF",
     #
     "VUUU", "VUUB", "VUUF", "VUVU", "VUVB", "VUVF", "VUBU", "VUBB", "VUBF",
     "VULU", "VULB", "VULF", "VUFU", "VUFB", "VUFF",
     #
     "VVUU", "VVUB", "VVUF", "VVVU", "VVVB", "VVVF", "VVBU", "VVBB", "VVBF",
     "VVLU", "VVLB", "VVLF", "VVFU", "VVFB", "VVFF",
     #
     "BUUU", "BUUB", "BUUF", "BUVU", "BUVB", "BUVF", "BUBU", "BUBB", "BUBF",
     "BULU", "BULB", "BULF", "BUFU", "BUFB", "BUFF",
     #
     "BVUU", "BVUB", "BVUF", "BVVU", "BVVB", "BVVF", "BVBU", "BVBB", "BVBF",
     "BVLU", "BVLB", "BVLF", "BVFU", "BVFB", "BVFF",
     #
     "FUUU", "FUUB", "FUUF", "FUVU", "FUVB", "FUVF", "FUBU", "FUBB", "FUBF",
     "FULU", "FULB", "FULF", "FUFU", "FUFB", "FUFF",
     #
     "FVUU", "FVUB", "FVUF", "FVVU", "FVVB", "FVVF", "FVBU", "FVBB", "FVBF",
     "FVLU", "FVLB", "FVLF", "FVFU", "FVFB", "FVFF",
])])

REVERSE_TERM_COMBINATIONS = dict([
    (value, key)
    for key, value in TERM_COMBINATIONS.items()
])

TERM_INSTANTIATION_DICT = {
    "U": URIRef,
    "B": BNode,
    "V": Variable,
    "L": Literal
}
