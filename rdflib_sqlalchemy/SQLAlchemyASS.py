import re
import sys
import logging
import sqlalchemy
import hashlib
from rdflib import BNode
from rdflib import Literal
from rdflib import RDF
from rdflib import URIRef
from rdflib.graph import Graph
from rdflib.graph import QuotedGraph
from rdflib.store import Store #, NodePickler
from rdfextras.store.REGEXMatching import PYTHON_REGEX
from rdfextras.store.REGEXMatching import REGEXTerm
from rdfextras.utils.termutils import REVERSE_TERM_COMBINATIONS
from rdfextras.utils.termutils import TERM_INSTANTIATION_DICT
from rdfextras.utils.termutils import constructGraph
from rdfextras.utils.termutils import type2TermCombination
from rdfextras.utils.termutils import statement2TermCombination

logging.basicConfig(level=logging.ERROR,format="%(message)s")
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.ERROR)
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

COUNT_SELECT   = 0
CONTEXT_SELECT = 1
TRIPLE_SELECT  = 2
TRIPLE_SELECT_NO_ORDER = 3

ASSERTED_NON_TYPE_PARTITION = 3
ASSERTED_TYPE_PARTITION     = 4
QUOTED_PARTITION            = 5
ASSERTED_LITERAL_PARTITION  = 6

FULL_TRIPLE_PARTITIONS = [QUOTED_PARTITION,ASSERTED_LITERAL_PARTITION]

INTERNED_PREFIX = 'kb_'

Any = None

# Stolen from Will Waites' py4s

def skolemise(statement):
    def _sk(x):
        if isinstance(x, BNode):
            return URIRef("bnode:%s" % x)
        return x
    return tuple(map(_sk, statement))

def deskolemise(statement):
    def _dst(x):
        if isinstance(x, URIRef) and x.startswith("bnode:"):
            _unused, bnid = x.split(":", 1)
            return BNode(bnid)
        return x
    return tuple(map(_dst, statement))

def _parse_rfc1738_args(name):
    import urllib
    import cgi
    """ parse url str into options
    code orig from sqlalchemy.engine.url """
    pattern = re.compile(r'''
            (\w+)://
            (?:
                ([^:/]*)
                (?::([^/]*))?
            @)?
            (?:
                ([^/:]*)
                (?::([^/]*))?
            )?
            (?:/(.*))?
            '''
            , re.X)

    m = pattern.match(name)
    if m is not None:
        (name, username, password, host, port, database) = m.group(1, 2, 3, 4, 5, 6)
        if database is not None:
            tokens = database.split(r"?", 2)
            database = tokens[0]
            query = (len(tokens) > 1 and dict( cgi.parse_qsl(tokens[1]) ) or None)
            if query is not None:
                query = dict([(k.encode('ascii'), query[k]) for k in query])
        else:
            query = None
        opts = {'username':username,'password':password,'host':host,'port':port,'database':database, 'query':query}
        if opts['password'] is not None:
            opts['password'] = urllib.unquote_plus(opts['password'])
        return (name, opts)
    else:
        raise ValueError("Could not parse rfc1738 URL from string '%s'" % name)

# User-defined REGEXP operator
def regexp(expr, item):
    r = re.compile(expr)
    return r.match(item) is not None

def queryAnalysis(query, store, cursor):
    """
    Helper function for executing EXPLAIN on all dispatched SQL statements - 
    for the pupose of analyzing index usage
    """
    cursor.execute(store._normalizeSQLCmd('explain ' + query))
    rt = cursor.fetchall()[0]
    table, joinType, posKeys, _key, key_len, comparedCol, rowsExamined, extra = rt
    if not _key:
        assert joinType == 'ALL'
        if not hasattr(store, 'queryOptMarks'):
            store.queryOptMarks = {}
        hits = store.queryOptMarks.get(('FULL SCAN', table), 0)
        store.queryOptMarks[('FULL SCAN', table)] = hits + 1
    
    if not hasattr(store, 'queryOptMarks'):
        store.queryOptMarks = {}
    hits = store.queryOptMarks.get((_key,table), 0)
    store.queryOptMarks[(_key, table)] = hits + 1

def unionSELECT(selectComponents, distinct=False, selectType=TRIPLE_SELECT):
    """
    Terms: u - uri refs  v - variables  b - bnodes l - literal f - formula

    Helper function for building union all select statement
    Takes a list of:
     - table name
     - table alias
     - table type (literal, type, asserted, quoted)
     - where clause string
    """
    selects = []
    for tableName,tableAlias,whereClause,tableType in selectComponents:

        if selectType == COUNT_SELECT:
            selectString = "select count(*)"
            tableSource = " from %s " % tableName
        elif selectType == CONTEXT_SELECT:
            selectString = "select %s.context" % tableAlias
            tableSource = " from %s as %s " % (tableName,tableAlias)
        elif tableType in FULL_TRIPLE_PARTITIONS:
            selectString = "select *"#%(tableAlias)
            tableSource = " from %s as %s " % (tableName, tableAlias)
        elif tableType == ASSERTED_TYPE_PARTITION:
            selectString =\
            """select %s.member as subject, "%s" as predicate, %s.klass as object, %s.context as context, %s.termComb as termComb, NULL as objLanguage, NULL as objDatatype""" % (tableAlias, RDF.type, tableAlias, tableAlias, tableAlias)
            tableSource = " from %s as %s " % (tableName, tableAlias)
        elif tableType == ASSERTED_NON_TYPE_PARTITION:
            selectString =\
            """select *,NULL as objLanguage, NULL as objDatatype"""
            tableSource = " from %s as %s "%(tableName, tableAlias)
        

        #selects.append('('+selectString + tableSource + whereClause+')')
        selects.append(selectString + tableSource + whereClause)
    
    orderStmt = ''
    if selectType == TRIPLE_SELECT:
        orderStmt = ' order by subject,predicate,object'
    if distinct:
        return ' union '.join(selects) + orderStmt
    else:
        return ' union all '.join(selects) + orderStmt

def extractTriple(tupleRt, store, hardCodedContext=None):
    """
    Takes a tuple which represents an entry in a result set and
    converts it to a tuple of terms using the termComb integer
    to interpret how to instantiate each term
    """
    try:
        subject, predicate, obj, rtContext, termComb, objLanguage, objDatatype = tupleRt
        termCombString = REVERSE_TERM_COMBINATIONS[termComb]
        subjTerm, predTerm, objTerm, ctxTerm = termCombString
    except ValueError:
        subject, subjTerm, predicate, predTerm, obj, objTerm, rtContext, ctxTerm, objLanguage, objDatatype = tupleRt
    context = rtContext is not None and rtContext or hardCodedContext.identifier
    s = createTerm(subject, subjTerm, store)
    p = createTerm(predicate, predTerm, store)
    o = createTerm(obj, objTerm, store, objLanguage, objDatatype)
    
    graphKlass, idKlass = constructGraph(ctxTerm)
    return s, p, o, (graphKlass, idKlass, context)

def createTerm(termString, termType, store, objLanguage=None, objDatatype=None):
    """
    #TODO: Stuff
    Takes a term value, term type, and store intsance
    and creates a term object.

    QuotedGraphs are instantiated differently
    """
    if termType == 'L':
        cache = store.literalCache.get((termString, objLanguage, objDatatype))
        if cache is not None:
            #store.cacheHits += 1
            return cache
        else:
            #store.cacheMisses += 1
            # rt = Literal(termString, objLanguage, objDatatype)
            # store.literalCache[((termString, objLanguage, objDatatype))] = rt
            if objLanguage and not objDatatype:
                rt = Literal(termString, objLanguage)
                store.literalCache[((termString, objLanguage))] = rt
            elif objDatatype and not objLanguage:
                rt = Literal(termString, objDatatype)
                store.literalCache[((termString, objDatatype))] = rt
            elif not objLanguage and not objDatatype:
                rt = Literal(termString)
                store.literalCache[((termString))] = rt
            else:
                rt = Literal(termString, objDatatype)
                store.literalCache[((termString, objDatatype))] = rt
            return rt
    elif termType == 'F':
        cache = store.otherCache.get((termType, termString))
        if cache is not None:
            #store.cacheHits += 1
            return cache
        else:
            #store.cacheMisses += 1
            rt = QuotedGraph(store, URIRef(termString))
            store.otherCache[(termType, termString)] = rt
            return rt
    elif termType == 'B':
        cache = store.bnodeCache.get((termString))
        if cache is not None:
            #store.cacheHits += 1
            return cache
        else:
            #store.cacheMisses += 1
            rt = TERM_INSTANTIATION_DICT[termType](termString)
            store.bnodeCache[(termString)] = rt
            return rt
    elif termType == 'U':
        cache = store.uriCache.get((termString))
        if cache is not None:
            #store.cacheHits += 1
            return cache
        else:
            #store.cacheMisses += 1
            rt = URIRef(termString)
            store.uriCache[(termString)] = rt
            return rt
    else:
        cache = store.otherCache.get((termType, termString))
        if cache is not None:
            #store.cacheHits += 1
            return cache
        else:
            #store.cacheMisses += 1
            rt = TERM_INSTANTIATION_DICT[termType](termString)
            store.otherCache[(termType, termString)] = rt
            return rt

class SQLGenerator(object):
    def executeSQL(self, cursor, qStr, params=None, paramList=False):
        """
        This takes the query string and parameters and (depending on the 
        SQL implementation) either fill in the parameter in-place or pass 
        it on to the Python DB impl (if it supports this). The default 
        (here) is to fill the parameters in-place surrounding each param 
        with quote characters
        """
        # print("SQLGenerator", qStr,params)
        if not params:
            cursor.execute(unicode(qStr))
        elif paramList:
            raise Exception("Not supported!")
        else:
            params = tuple([not isinstance(item,int) and u"'%s'" % item or item 
                                for item in params])
            querystr = qStr.replace('"',"'")
            cursor.execute(querystr%params)

    # FIXME:  This *may* prove to be a performance bottleneck and should
    # perhaps be implemented in C (as it was in 4Suite RDF)
    # def EscapeQuotes(self,qstr):
    #     return escape_quotes(qstr)

    def EscapeQuotes(self, qstr):
        """
        Ported from Ft.Lib.DbUtil
        """
        if qstr is None:
            return ''
        tmp = qstr.replace("\\","\\\\")
        tmp = tmp.replace("'", "\\'")
        return tmp

    # Normalize a SQL command before executing it.  Commence unicode black magic
    def _normalizeSQLCmd(self,cmd):
        import types
        if not isinstance(cmd, types.UnicodeType):
            cmd = unicode(cmd, 'ascii')

        return cmd.encode('utf-8')

    # This is overridden to leave unicode terms as is
    # Instead of converting them to ascii (the default behavior)

    def normalizeTerm(self, term):
        """
        Takes a term and 'normalizes' it.
        Literals are escaped, Graphs are replaced with just their identifiers
        """
        if isinstance(term, (QuotedGraph, Graph)):
            return term.identifier.encode('utf-8')
        elif isinstance(term, Literal):
            return self.EscapeQuotes(term).encode('utf-8')
        elif term is None or isinstance(term, (tuple, list, REGEXTerm)):
            return term
        else:
            return term.encode('utf-8')

    def buildTypeSQLCommand(self, member, klass, context, storeId):
        """
        Builds an insert command for a type table
        """
        #columns: member,klass,context
        rt = "INSERT INTO %s_type_statements" % storeId + " (member,klass,context,termComb) VALUES (%s, %s, %s,%s)"
        return rt,[
            self.normalizeTerm(member),
            self.normalizeTerm(klass),
            self.normalizeTerm(context.identifier),
            int(type2TermCombination(member, klass, context))]
    
    def buildLiteralTripleSQLCommand(self, subject, predicate, obj, context, storeId):
        """
        Builds an insert command for literal triples (statements where the 
        object is a Literal)
        """
        triplePattern = int(statement2TermCombination(subject, predicate, obj, context))
        literal_table = "%s_literal_statements" % storeId
        command = "INSERT INTO %s " % literal_table +" (subject,predicate,object,context,termComb,objLanguage,objDatatype) VALUES (%s, %s, %s, %s, %s,%s,%s)"
        return command, [
            self.normalizeTerm(subject),
            self.normalizeTerm(predicate),
            self.normalizeTerm(obj),
            self.normalizeTerm(context.identifier),
            triplePattern,
            isinstance(obj, Literal) and obj.language or 'NULL',
            isinstance(obj, Literal) and obj.datatype or 'NULL']
    
    def buildTripleSQLCommand(self, subject, predicate, obj, context, storeId, quoted):
        """
        Builds an insert command for regular triple table
        """
        stmt_table = quoted and "%s_quoted_statements" % storeId or "%s_asserted_statements" % storeId
        triplePattern = statement2TermCombination(subject, predicate, obj, context)
        if quoted:
            command = "INSERT INTO %s" % stmt_table +" (subject,predicate,object,context,termComb,objLanguage,objDatatype) VALUES (%s, %s, %s, %s, %s,%s,%s)"
            params = [
                self.normalizeTerm(subject),
                self.normalizeTerm(predicate),
                self.normalizeTerm(obj),
                self.normalizeTerm(context.identifier),
                triplePattern,
                isinstance(obj, Literal) and  obj.language or 'NULL',
                isinstance(obj, Literal) and obj.datatype or 'NULL']
        else:
            command = "INSERT INTO %s" % stmt_table + " (subject,predicate,object,context,termComb) VALUES (%s, %s, %s, %s, %s)"
            params = [
                self.normalizeTerm(subject),
                self.normalizeTerm(predicate),
                self.normalizeTerm(obj),
                self.normalizeTerm(context.identifier),
                triplePattern]
        return command,params

    def buildClause(self, tableName, subject, predicate, obj, context=None, typeTable=False):
        """
        Builds WHERE clauses for the supplied terms and, context
        """
        parameters = []
        if typeTable:
            rdf_type_memberClause = rdf_type_contextClause = rdf_type_contextClause = None

            
            clauseParts = self.buildTypeMemberClause(self.normalizeTerm(subject), tableName)
            if clauseParts is not None:
                rdf_type_memberClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildTypeClassClause(self.normalizeTerm(obj), tableName)
            if clauseParts is not None:
                rdf_type_klassClause = clauseParts[0]
                parameters.extend(clauseParts[-1])

            
            clauseParts = self.buildContextClause(context, tableName)
            if clauseParts is not None:
                rdf_type_contextClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            typeClauses = [rdf_type_memberClause, rdf_type_klassClause, rdf_type_contextClause]
            clauseString = ' and '.join([clause for clause in typeClauses if clause])
            clauseString = clauseString and 'where ' + clauseString or ''
        else:
            subjClause = predClause = objClause = contextClause = litDTypeClause = litLanguageClause = None

            
            clauseParts = self.buildSubjClause(self.normalizeTerm(subject), tableName)
            if clauseParts is not None:
                subjClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildPredClause(self.normalizeTerm(predicate), tableName)
            if clauseParts is not None:
                predClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildObjClause(self.normalizeTerm(obj), tableName)
            if clauseParts is not None:
                objClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildContextClause(context, tableName)
            if clauseParts is not None:
                contextClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildLitDTypeClause(obj, tableName)
            if clauseParts is not None:
                litDTypeClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauseParts = self.buildLitLanguageClause(obj, tableName)
            if clauseParts is not None:
                litLanguageClause = clauseParts[0]
                parameters.extend([param for param in clauseParts[-1] if param])

            
            clauses=[subjClause, predClause, objClause, contextClause, litDTypeClause, litLanguageClause]
            clauseString = ' and '.join([clause for clause in clauses if clause])
            clauseString = clauseString and 'where ' + clauseString or ''
        

        return clauseString, [p for p in parameters if p]

    def buildLitDTypeClause(self, obj, tableName):
        if isinstance(obj,Literal):
            return obj.datatype is not None and (
                "%s.objDatatype=" % (tableName) + "%s", [obj.datatype.encode('utf-8')]
                ) or None
        else:
            return None

    def buildLitLanguageClause(self,obj,tableName):
        if isinstance(obj,Literal):
            return obj.language is not None and (
                "%s.objLanguage=" % (tableName) + "%s",[obj.language.encode('utf-8')]
                ) or None
        else:
            return None

    # Where Clause  utility Functions
    # The predicate and object clause builders are modified in order
    # to optimize subjects and objects utility functions which can
    # take lists as their last argument (object, predicate - 
    # respectively)
    def buildSubjClause(self,subject,tableName):
        if isinstance(subject,REGEXTerm):
            return " REGEXP (%s,"+" %s)" % (
                tableName and '%s.subject' % tableName or 'subject'), [subject]
        elif isinstance(subject, list):
            clauseStrings=[]
            paramStrings = []
            for s in subject:
                if isinstance(s, REGEXTerm):
                    clauseStrings.append(" REGEXP (%s,"+" %s)" % (
                            tableName and '%s.subject' % tableName or 'subject') + " %s")
                    paramStrings.append(self.normalizeTerm(s))
                elif isinstance(s, (QuotedGraph, Graph)):
                    clauseStrings.append(
                        "%s=" % (
                            tableName and '%s.subject' % tableName or 'subject') + "%s")
                    paramStrings.append(self.normalizeTerm(s.identifier))
                else:
                    clauseStrings.append(
                        "%s=" % (
                        tableName and '%s.subject' % tableName or 'subject') + "%s")
                    paramStrings.append(self.normalizeTerm(s))
            return '('+ ' or '.join(clauseStrings) + ')', paramStrings
        elif isinstance(subject, (QuotedGraph, Graph)):
            return "%s=" % (
                    tableName and '%s.subject' % tableName or 'subject') \
                    + "%s", [self.normalizeTerm(subject.identifier)]
        else:
            return subject is not None and "%s=" % (
                tableName and '%s.subject' % tableName or 'subject') + \
                "%s", [subject] or None

    # Capable of taking a list of predicates as well (in which case 
    # subclauses are joined with 'OR')
    def buildPredClause(self, predicate, tableName):
        if isinstance(predicate, REGEXTerm):
            return " REGEXP (%s," + " %s)" % (
                tableName and '%s.predicate' % tableName or 'predicate'), [predicate]
        elif isinstance(predicate, list):
            clauseStrings=[]
            paramStrings = []
            for p in predicate:
                if isinstance(p, REGEXTerm):
                    clauseStrings.append(" REGEXP (%s,"+" %s)" % (
                        tableName and '%s.predicate' % tableName or 'predicate'))
                else:
                    clauseStrings.append("%s=" % (
                        tableName and '%s.predicate' % tableName or 'predicate') + \
                    "%s")
                paramStrings.append(self.normalizeTerm(p))
            return '('+ ' or '.join(clauseStrings) + ')', paramStrings
        else:
            return predicate is not None and "%s=" % (
                tableName and '%s.predicate' % tableName or 'predicate') + \
                    "%s", [predicate] or None

    # Capable of taking a list of objects as well (in which case subclauses
    # are joined with 'OR')
    def buildObjClause(self, obj, tableName):
        if isinstance(obj, REGEXTerm):
            return " REGEXP (%s,"+" %s)" % (
                tableName and '%s.object' % tableName or 'object'), [obj]
        elif isinstance(obj, list):
            clauseStrings=[]
            paramStrings = []
            for o in obj:
                if isinstance(o, REGEXTerm):
                    clauseStrings.append(" REGEXP (%s,"+" %s)" % (
                        tableName and '%s.object' % tableName or 'object'))
                    paramStrings.append(self.normalizeTerm(o))
                elif isinstance(o, (QuotedGraph, Graph)):
                    clauseStrings.append("%s=" % (
                        tableName and '%s.object' % tableName or 'object') + "%s")
                    paramStrings.append(self.normalizeTerm(o.identifier))
                else:
                    clauseStrings.append("%s=" % (
                        tableName and '%s.object' % tableName or 'object') + "%s")
                    paramStrings.append(self.normalizeTerm(o))
            return '('+ ' or '.join(clauseStrings) + ')', paramStrings
        elif isinstance(obj, (QuotedGraph, Graph)):
            return "%s=" % (
                tableName and '%s.object' % tableName or 'object') + \
                    "%s", [self.normalizeTerm(obj.identifier)]
        else:
            return obj is not None and "%s=" % (
                tableName and '%s.object' % tableName or 'object') + \
                    "%s", [obj] or None

    def buildContextClause(self, context, tableName):
        context = context is not None \
                    and self.normalizeTerm(context.identifier) \
                    or context
        if isinstance(context, REGEXTerm):
            return " REGEXP (%s," + " %s)" % (
                tableName and '%s.context' % tableName or 'context'), [context]
        else:
            return context is not None and "%s=" % (
                tableName and '%s.context' % tableName or 'context') + \
                    "%s", [context] or None
    
    def buildTypeMemberClause(self, subject, tableName):
        if isinstance(subject,REGEXTerm):
            return " REGEXP (%s," + " %s)" % (
                tableName and '%s.member' % tableName or 'member'), [subject]
        elif isinstance(subject, list):
            clauseStrings=[]
            paramStrings = []
            for s in subject:
                clauseStrings.append("%s.member=" % tableName + "%s")
                if isinstance(s,(QuotedGraph,Graph)):
                    paramStrings.append(self.normalizeTerm(s.identifier))
                else:
                    paramStrings.append(self.normalizeTerm(s))
            return '('+ ' or '.join(clauseStrings) + ')', paramStrings
        else:
            return subject and u"%s.member = "% (
                tableName) + "%s", [subject]

    def buildTypeClassClause(self, obj, tableName):
        if isinstance(obj,REGEXTerm):
            return " REGEXP (%s," + " %s)" % (
                tableName and '%s.klass' % tableName or 'klass'), [obj]
        elif isinstance(obj,list):
            clauseStrings=[]
            paramStrings = []
            for o in obj:
                clauseStrings.append("%s.klass=" % tableName + "%s")
                if isinstance(o,(QuotedGraph,Graph)):
                    paramStrings.append(self.normalizeTerm(o.identifier))
                else:
                    paramStrings.append(self.normalizeTerm(o))
            return '('+ ' or '.join(clauseStrings) + ')', paramStrings
        else:
            return obj is not None and "%s.klass = "%tableName+"%s",[obj] or None

class SQLAlchemy(Store, SQLGenerator):
    """
    SQL-92 formula-aware implementation of an rdflib Store.
    It stores its triples in the following partitions:

    - Asserted non rdf:type statements
    - Asserted literal statements
    - Asserted rdf:type statements (in a table which models Class membership)
        The motivation for this partition is primarily query speed and 
        scalability as most graphs will always have more rdf:type statements 
        than others
    - All Quoted statements
    
    In addition it persists namespace mappings in a separate table
    """
    context_aware = True
    formula_aware = True
    transaction_aware = True
    regex_matching = PYTHON_REGEX
    autocommit_default = False

    def __init__(self, identifier=None, configuration=None):
        """
        identifier: URIRef of the Store. Defaults to CWD
        configuration: string containing infomation open can use to
        connect to datastore.
        """
        self.identifier = identifier and identifier or 'hardcoded'
        # Use only the first 10 bytes of the digest
        self._internedId = INTERNED_PREFIX + \
                                hashlib.sha1(
                                    self.identifier.encode('utf8')).hexdigest()[:10]
        
        super(SQLAlchemy, self).__init__(identifier=self.identifier, configuration=None)

        # This parameter controls how exlusively the literal table is searched
        # If true, the Literal partition is searched *exclusively* if the 
        # object term in a triple pattern is a Literal or a REGEXTerm.  Note, 
        # the latter case prevents the matching of URIRef nodes as the objects
        # of a triple in the store.
        # If the object term is a wildcard (None)
        # Then the Literal paritition is searched in addition to the others
        # If this parameter is false, the literal partition is searched 
        # regardless of what the object of the triple pattern is
        self.STRONGLY_TYPED_TERMS = False

        self.cacheHits = 0
        self.cacheMisses = 0
        self.literalCache = {}
        self.uriCache = {}
        self.bnodeCache = {}
        self.otherCache = {}
        self._db = None
        self.__node_pickler = None

        if configuration is not None:
            self.open(configuration)

    def __get_node_pickler(self):
        if getattr(self, '__node_pickler', False) or self.__node_pickler is None:
            from rdflib.term import URIRef
            from rdflib.graph import GraphValue
            from rdflib.term import Variable
            from rdflib.term import Statement
            from rdflib.store import NodePickler
            self.__node_pickler = np = NodePickler()
            np.register(self, "S")
            np.register(URIRef, "U")
            np.register(BNode, "B")
            np.register(Literal, "L")
            np.register(Graph, "G")
            np.register(QuotedGraph, "Q")
            np.register(Variable, "V")
            np.register(Statement, "s")
            np.register(GraphValue, "v")
        return self.__node_pickler
    node_pickler = property(__get_node_pickler)

    def open(self, configuration, create=True):
        """
        Opens the store specified by the configuration string. If
        create is True a store will be created if it does not already
        exist. If create is False and a store does not already exist
        an exception is raised. An exception is also raised if a store
        exists, but there is insufficient permissions to open the
        store."""
        name, opts = _parse_rfc1738_args(configuration)
        self.engine = sqlalchemy.create_engine(configuration)
        self._db = self.engine.connect().connection
        if create:
            c = self._db.cursor()
            c.execute(CREATE_ASSERTED_STATEMENTS_TABLE%(self._internedId))
            c.execute(CREATE_ASSERTED_TYPE_STATEMENTS_TABLE%(self._internedId))
            c.execute(CREATE_QUOTED_STATEMENTS_TABLE%(self._internedId))
            c.execute(CREATE_NS_BINDS_TABLE%(self._internedId))
            c.execute(CREATE_LITERAL_STATEMENTS_TABLE%(self._internedId))
            for tblName,indices in [
                (
                    "%s_asserted_statements",
                    [
                        ("%s_A_termComb_index",('termComb',)),
                        ("%s_A_s_index",('subject',)),
                        ("%s_A_p_index",('predicate',)),
                        ("%s_A_o_index",('object',)),
                        ("%s_A_c_index",('context',)),
                    ],
                ),
                (
                    "%s_type_statements",
                    [
                        ("%s_T_termComb_index",('termComb',)),
                        ("%s_member_index",('member',)),
                        ("%s_klass_index",('klass',)),
                        ("%s_c_index",('context',)),
                    ],
                ),
                (
                    "%s_literal_statements",
                    [
                        ("%s_L_termComb_index",('termComb',)),
                        ("%s_L_s_index",('subject',)),
                        ("%s_L_p_index",('predicate',)),
                        ("%s_L_c_index",('context',)),
                    ],
                ),
                (
                    "%s_quoted_statements",
                    [
                        ("%s_Q_termComb_index",('termComb',)),
                        ("%s_Q_s_index",('subject',)),
                        ("%s_Q_p_index",('predicate',)),
                        ("%s_Q_o_index",('object',)),
                        ("%s_Q_c_index",('context',)),
                    ],
                ),
                (
                    "%s_namespace_binds",
                    [
                        ("%s_uri_index",('uri',)),
                    ],
                )]:
                for indexName,columns in indices:
                    c.execute("CREATE INDEX %s on %s (%s)" % (
                        indexName % self._internedId, 
                        tblName % (self._internedId), 
                        ','.join(columns)))
            c.close()
        self._db.create_function("regexp", 2, regexp)
        if configuration:
            from sqlalchemy.engine import reflection
            insp = reflection.Inspector.from_engine(self.engine)
            tbls = insp.get_table_names()
            for tn in [tbl%(self._internedId) for tbl in table_name_prefixes]:
                if tn not in tbls:
                    sys.stderr.write("table %s Doesn't exist\n" % (tn));
                    # The database exists, but one of the partitions doesn't exist
                    return 0
            # Everything is there (the database and the partitions)
            return 1
        # The database doesn't exist - nothing is there
        return -1

    def close(self, commit_pending_transaction=False):
        """
        FIXME:  Add documentation!!
        """
        if commit_pending_transaction:
            self._db.commit()
        try:
            self._db.close()
        except:
            pass

    def destroy(self, configuration):
        """
        FIXME: Add documentation
        """
        name, opts = _parse_rfc1738_args(configuration)
        self.engine = sqlalchemy.create_engine(configuration)
        self._db = self.engine.connect().connection
        c = self._db
        for tblsuffix in table_name_prefixes:
            try:
                c.execute('DROP table %s' % tblsuffix % (self._internedId))
            except:
                # print "unable to drop table: %s" % (tblsuffix % (self._internedId))
                pass
        # Note, this only removes the associated tables for the closed
        # world universe given by the identifier
        # print "Destroyed Close World Universe %s" % (uri)
        c.commit()
        c.close()


    # Triple Methods
    def add(self, (subject, predicate, obj), context=None, quoted=False):
        """ Add a triple to the store of triples. """
        c = self._db.cursor()
        if self.autocommit_default:
            c.execute("""SET AUTOCOMMIT=0""")
        if quoted or predicate != RDF.type:
            # Quoted statement or non rdf:type predicate
            # check if object is a literal
            if isinstance(obj,Literal):
                addCmd, params = self.buildLiteralTripleSQLCommand(
                    subject, predicate, obj, context, self._internedId)
            else:
                addCmd, params = self.buildTripleSQLCommand(
                    subject, predicate, obj, context, self._internedId, quoted)
        elif predicate == RDF.type:
            #asserted rdf:type statement
            addCmd, params = self.buildTypeSQLCommand(
                subject, obj, context, self._internedId)
        self.executeSQL(c, addCmd, params)
        c.close()

    def addN(self,quads):
        c = self._db.cursor()
        if self.autocommit_default:
            c.execute("""SET AUTOCOMMIT=0""")
        literalTriples = []
        typeTriples = []
        otherTriples = []
        literalTripleInsertCmd = None
        typeTripleInsertCmd = None
        otherTripleInsertCmd = None
        for subject, predicate, obj, context in quads:
            if isinstance(context, QuotedGraph) or predicate != RDF.type:
                # Quoted statement or non rdf:type predicate
                # check if object is a literal
                if isinstance(obj,Literal):
                    cmd, params = self.buildLiteralTripleSQLCommand(
                        subject, predicate, obj, context, self._internedId)
                    literalTripleInsertCmd = literalTripleInsertCmd is not None and literalTripleInsertCmd or cmd
                    literalTriples.append(params)
                else:
                    cmd, params = self.buildTripleSQLCommand(
                        subject, predicate, obj, context, self._internedId, isinstance(context, QuotedGraph))
                    otherTripleInsertCmd = otherTripleInsertCmd is not None and otherTripleInsertCmd or cmd
                    otherTriples.append(params)
            elif predicate == RDF.type:
                #asserted rdf:type statement
                cmd, params = self.buildTypeSQLCommand(
                    subject, obj, context, self._internedId)
                typeTripleInsertCmd = typeTripleInsertCmd is not None and typeTripleInsertCmd or cmd
                typeTriples.append(params)

        if literalTriples:
            self.executeSQL(
                c, literalTripleInsertCmd, literalTriples, paramList=True)
        if typeTriples:
            self.executeSQL(
                c, typeTripleInsertCmd, typeTriples, paramList=True)
        if otherTriples:
            self.executeSQL(
                c, otherTripleInsertCmd, otherTriples, paramList=True)
        

        c.close()

    def remove(self, (subject, predicate, obj), context):
        """ Remove a triple from the store """
        if context is not None:
            if subject is None and predicate is None and object is None:
                self._remove_context(context)
                return
        c = self._db.cursor()
        if self.autocommit_default:
            c.execute("""SET AUTOCOMMIT=0""")
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        if not predicate or predicate != RDF.type:
            #Need to remove predicates other than rdf:type

            
            if not self.STRONGLY_TYPED_TERMS or isinstance(obj, Literal):
                #remove literal triple
                clauseString,params = self.buildClause(
                    literal_table, subject, predicate, obj, context)
                if clauseString:
                    cmd ="DELETE FROM " + " ".join([literal_table, clauseString])
                else:
                    cmd ="DELETE FROM " + literal_table
                self.executeSQL(c, self._normalizeSQLCmd(cmd), params)
            
            for table in [quoted_table, asserted_table]:
                # If asserted non rdf:type table and obj is Literal, don't do 
                # anything (already taken care of)
                if table == asserted_table and isinstance(obj, Literal):
                    continue
                else:
                    clauseString, params = self.buildClause(
                        table, subject, predicate, obj, context)
                    if clauseString:
                        cmd="DELETE FROM " + " ".join([table, clauseString])
                    else:
                        cmd = "DELETE FROM " + table

                    
                    self.executeSQL(c,self._normalizeSQLCmd(cmd), params)
        

        if predicate == RDF.type or not predicate:
            # Need to check rdf:type and quoted partitions (in addition perhaps)
            clauseString,params = self.buildClause(
                asserted_type_table, subject, RDF.type, obj, context, True)
            if clauseString:
                cmd="DELETE FROM " + " ".join([asserted_type_table, clauseString])
            else:
                cmd='DELETE FROM '+asserted_type_table

            
            self.executeSQL(c, self._normalizeSQLCmd(cmd), params)
            
            clauseString, params = self.buildClause(
                quoted_table, subject, predicate, obj, context)
            if clauseString:
                cmd=clauseString and "DELETE FROM " + " ".join([quoted_table, clauseString])
            else:
                cmd = "DELETE FROM " + quoted_table

            
            self.executeSQL(c, self._normalizeSQLCmd(cmd), params)
        c.close()

    def triples(self, (subject, predicate, obj), context=None):
        """
        A generator over all the triples matching pattern. Pattern can
        be any objects for comparing against nodes in the store, for
        example, RegExLiteral, Date? DateRange?

        quoted table:                <id>_quoted_statements
        asserted rdf:type table:     <id>_type_statements
        asserted non rdf:type table: <id>_asserted_statements

        triple columns: subject,predicate,object,context,termComb,objLanguage,objDatatype
        class membership columns: member,klass,context termComb

        FIXME:  These union all selects *may* be further optimized by joins

        """
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        c = self._db.cursor()

        parameters = []

        if predicate == RDF.type:
            # select from asserted rdf:type partition and quoted table 
            # (if a context is specified)
            clauseString, params = self.buildClause(
                'typeTable', subject, RDF.type, obj, context, True)
            parameters.extend(params)
            selects = [
                (
                  asserted_type_table,
                  'typeTable',
                  clauseString,
                  ASSERTED_TYPE_PARTITION
                ),
            ]

        elif isinstance(predicate, REGEXTerm) \
                and predicate.compiledExpr.match(RDF.type) \
                or not predicate:
            # Select from quoted partition (if context is specified), 
            # Literal partition if (obj is Literal or None) and asserted
            # non rdf:type partition (if obj is URIRef or None)
            selects = []
            if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                clauseString, params = self.buildClause(
                        'literal', subject, predicate, obj, context)
                parameters.extend(params)
                selects.append((
                  literal_table,
                  'literal',
                  clauseString,
                  ASSERTED_LITERAL_PARTITION
                ))
            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) \
                    and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                clauseString, params = self.buildClause(
                    'asserted', subject, predicate, obj, context)
                parameters.extend(params)
                selects.append((
                  asserted_table,
                  'asserted',
                  clauseString,
                  ASSERTED_NON_TYPE_PARTITION
                ))

            clauseString, params = self.buildClause(
                'typeTable', subject, RDF.type, obj, context, True)
            parameters.extend(params)
            selects.append(
                (
                  asserted_type_table,
                  'typeTable',
                  clauseString,
                  ASSERTED_TYPE_PARTITION
                )
            )

        elif predicate:
            # select from asserted non rdf:type partition (optionally), 
            # quoted partition (if context is specified), and literal
            # partition (optionally)
            selects = []
            if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                clauseString, params = self.buildClause(
                    'literal', subject, predicate, obj, context)
                parameters.extend(params)
                selects.append((
                  literal_table,
                  'literal',
                  clauseString,
                  ASSERTED_LITERAL_PARTITION
                ))
            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) \
                    and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                clauseString, params = self.buildClause(
                    'asserted', subject, predicate, obj, context)
                parameters.extend(params)
                selects.append((
                  asserted_table,
                  'asserted',
                  clauseString,
                  ASSERTED_NON_TYPE_PARTITION
                ))

        if context is not None:
            clauseString, params = self.buildClause(
                'quoted', subject, predicate, obj, context)
            parameters.extend(params)
            selects.append(
                (
                  quoted_table,
                  'quoted',
                  clauseString,
                  QUOTED_PARTITION
                )
            )

        q = self._normalizeSQLCmd(unionSELECT(
                    selects, selectType=TRIPLE_SELECT_NO_ORDER))
        self.executeSQL(c, q, parameters)
        # NOTE: SQLite does not support ORDER BY terms that aren't integers,
        # so the entire result set must be iterated in order to be able to
        # return a generator of contexts
        tripleCoverage = {}
        result = c.fetchall()
        c.close()
        for rt in result:
            s, p, o, (graphKlass, idKlass, graphId) = \
                            extractTriple(rt, self, context)
            contexts = tripleCoverage.get((s,p,o), [])
            contexts.append(graphKlass(self, idKlass(graphId)))
            tripleCoverage[(s,p,o)] = contexts

        for (s,p,o),contexts in tripleCoverage.items():
            yield (s,p,o), (c for c in contexts)

    def triples_choices(self, (subject, predicate, object_),context=None):
        """
        A variant of triples that can take a list of terms instead of a single
        term in any slot.  Stores can implement this to optimize the response time
        from the import default 'fallback' implementation, which will iterate
        over each term in the list and dispatch to tripless
        """
        if isinstance(object_, list):
            assert not isinstance(subject, list), "object_ / subject are both lists"
            assert not isinstance(predicate, list), "object_ / predicate are both lists"
            if not object_:
                object_ = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_), context):
                yield (s1, p1, o1), cg

        
        elif isinstance(subject, list):
            assert not isinstance(predicate, list), "subject / predicate are both lists"
            if not subject:
                subject = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_) ,context):
                yield (s1, p1, o1), cg

        
        elif isinstance(predicate, list):
            assert not isinstance(subject, list), "predicate / subject are both lists"
            if not predicate:
                predicate = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_) ,context):
                yield (s1, p1, o1), cg

    def __repr__(self):
        c = self._db.cursor()
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        

        selects = [
            (
              asserted_type_table,
              'typeTable',
              '',
              ASSERTED_TYPE_PARTITION
            ),
            (
              quoted_table,
              'quoted',
              '',
              QUOTED_PARTITION
            ),
            (
              asserted_table,
              'asserted',
              '',
              ASSERTED_NON_TYPE_PARTITION
            ),
            (
              literal_table,
              'literal',
              '',
              ASSERTED_LITERAL_PARTITION
            ),
        ]
        q = unionSELECT(selects, distinct=False, selectType=COUNT_SELECT)
        self.executeSQL(c, self._normalizeSQLCmd(q))
        rt = c.fetchall()
        typeLen, quotedLen, assertedLen, literalLen = [rtTuple[0] for rtTuple in rt]
        try:
            return "<Partitioned SQL N3 Store: %s contexts, %s classification assertions, %s quoted statements, %s property/value assertions, and %s other assertions>" % (
                len([c for c in self.contexts()]), typeLen, quotedLen, literalLen, assertedLen)
        except Exception:
            return "<Partitioned MySQL N3 Store>"

    def __len__(self, context=None):
        """ Number of statements in the store. """
        c = self._db.cursor()
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        

        parameters = []
        quotedContext = assertedContext = typeContext = literalContext = None

        
        clauseParts = self.buildContextClause(context, quoted_table)
        if clauseParts:
            quotedContext, params = clauseParts
            parameters.extend([p for p in params if p])

        
        clauseParts = self.buildContextClause(context, asserted_table)
        if clauseParts:
            assertedContext, params = clauseParts
            parameters.extend([p for p in params if p])

        
        clauseParts = self.buildContextClause(context, asserted_type_table)
        if clauseParts:
            typeContext, params = clauseParts
            parameters.extend([p for p in params if p])

        
        clauseParts = self.buildContextClause(context, literal_table)
        if clauseParts:
            literalContext, params = clauseParts
            parameters.extend([p for p in params if p])

        if context is not None:
            selects = [
                (
                  asserted_type_table,
                  'typeTable',
                  typeContext and 'where ' + typeContext or '',
                  ASSERTED_TYPE_PARTITION
                ),
                (
                  quoted_table,
                  'quoted',
                  quotedContext and 'where ' + quotedContext or '',
                  QUOTED_PARTITION
                ),
                (
                  asserted_table,
                  'asserted',
                  assertedContext and 'where ' + assertedContext or '',
                  ASSERTED_NON_TYPE_PARTITION
                ),
                (
                  literal_table,
                  'literal',
                  literalContext and 'where ' + literalContext or '',
                  ASSERTED_LITERAL_PARTITION
                ),
            ]
            q = unionSELECT(selects, distinct=True, selectType=COUNT_SELECT)
        else:
            selects = [
                (
                  asserted_type_table,
                  'typeTable',
                  typeContext and 'where ' + typeContext or '',
                  ASSERTED_TYPE_PARTITION
                ),
                (
                  asserted_table,
                  'asserted',
                  assertedContext and 'where ' + assertedContext or '',
                  ASSERTED_NON_TYPE_PARTITION
                ),
                (
                  literal_table,
                  'literal',
                  literalContext and 'where ' + literalContext or '',
                  ASSERTED_LITERAL_PARTITION
                ),
            ]
            q = unionSELECT(selects, distinct=False, selectType=COUNT_SELECT)
        
        self.executeSQL(c, self._normalizeSQLCmd(q), parameters)
        rt = c.fetchall()
        c.close()
        return reduce(lambda x,y: x + y,  [rtTuple[0] for rtTuple in rt])

    def contexts(self, triple=None):
        c = self._db.cursor()
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        

        parameters = []

        if triple is not None:
            subject, predicate, obj = triple
            if predicate == RDF.type:
                # Select from asserted rdf:type partition and quoted table
                # (if a context is specified)
                clauseString, params = self.buildClause(
                    'typeTable', subject, RDF.type, obj, Any, True)
                parameters.extend(params)
                selects = [
                    (
                      asserted_type_table,
                      'typeTable',
                      clauseString,
                      ASSERTED_TYPE_PARTITION
                    ),
                ]
            
            elif isinstance(predicate, REGEXTerm) \
                and predicate.compiledExpr.match(RDF.type) \
                or not predicate:
                # Select from quoted partition (if context is specified), 
                # literal partition if (obj is Literal or None) and 
                # asserted non rdf:type partition (if obj is URIRef
                # or None)
                clauseString,params = self.buildClause(
                        'typeTable', subject, RDF.type, obj, Any, True)
                parameters.extend(params)
                selects = [
                    (
                      asserted_type_table,
                      'typeTable',
                      clauseString,
                      ASSERTED_TYPE_PARTITION
                    ),
                ]

                if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                    clauseString,params = self.buildClause(
                            'literal', subject, predicate, obj)
                    parameters.extend(params)
                    selects.append((
                      literal_table,
                      'literal',
                      clauseString,
                      ASSERTED_LITERAL_PARTITION
                    ))
                if not isinstance(obj,Literal) \
                    and not (isinstance(obj, REGEXTerm) \
                    and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                    clauseString,params = self.buildClause(
                            'asserted', subject, predicate, obj)
                    parameters.extend(params)
                    selects.append((
                      asserted_table,
                      'asserted',
                      clauseString,
                      ASSERTED_NON_TYPE_PARTITION
                    ))

            elif predicate:
                # select from asserted non rdf:type partition (optionally), 
                # quoted partition (if context is speciied), and literal
                # partition (optionally)
                selects = []
                if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                    clauseString,params = self.buildClause(
                            'literal', subject, predicate, obj)
                    parameters.extend(params)
                    selects.append((
                      literal_table,
                      'literal',
                      clauseString,
                      ASSERTED_LITERAL_PARTITION
                    ))
                if not isinstance(obj,Literal) \
                    and not (isinstance(obj, REGEXTerm) \
                    and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                    clauseString,params = self.buildClause(
                        'asserted', subject, predicate, obj)
                    parameters.extend(params)
                    selects.append((
                      asserted_table,
                      'asserted',
                      clauseString,
                      ASSERTED_NON_TYPE_PARTITION
                ))

            
            clauseString,params = self.buildClause(
                    'quoted', subject, predicate, obj)
            parameters.extend(params)
            selects.append(
                (
                  quoted_table,
                  'quoted',
                  clauseString,
                  QUOTED_PARTITION
                )
            )
            q = unionSELECT(selects, distinct=True, selectType=CONTEXT_SELECT)
        else:
            selects = [
                (
                  asserted_type_table,
                  'typeTable',
                  '',
                  ASSERTED_TYPE_PARTITION
                ),
                (
                  quoted_table,
                  'quoted',
                  '',
                  QUOTED_PARTITION
                ),
                (
                  asserted_table,
                  'asserted',
                  '',
                  ASSERTED_NON_TYPE_PARTITION
                ),
                (
                  literal_table,
                  'literal',
                  '',
                  ASSERTED_LITERAL_PARTITION
                ),
            ]
            q = unionSELECT(selects, distinct=True, selectType=CONTEXT_SELECT)
        
        self.executeSQL(c, self._normalizeSQLCmd(q), parameters)
        rt=c.fetchall()
        for context in [rtTuple[0] for rtTuple in rt]:
            yield context
        c.close()

    def _remove_context(self, identifier):
        """ """
        assert identifier
        c = self._db.cursor()
        if self.autocommit_default:
            c.execute("""SET AUTOCOMMIT=0""")
        quoted_table = "%s_quoted_statements" % self._internedId
        asserted_table = "%s_asserted_statements" % self._internedId
        asserted_type_table = "%s_type_statements" % self._internedId
        literal_table = "%s_literal_statements" % self._internedId
        for table in [quoted_table, asserted_table, 
                      asserted_type_table, literal_table]:
            clauseString, params = self.buildContextClause(identifier, table)
            self.executeSQL(
                c,
                self._normalizeSQLCmd("DELETE from %s WHERE %s" % (table, clauseString)),
                [p for p in params if p]
            )
        c.close()

    # Optional Namespace methods
    # Placeholder optimized interfaces (those needed in order to port Versa)
    def subjects(self, predicate=None, obj=None):
        """
        A generator of subjects with the given predicate and object.
        """
        raise Exception("Not implemented")

    # Capable of taking a list of predicate terms instead of a single term
    def objects(self, subject=None, predicate=None):
        """
        A generator of objects with the given subject and predicate.
        """
        raise Exception("Not implemented")
    
    # Optimized interfaces (others)
    def predicate_objects(self, subject=None):
        """
        A generator of (predicate, object) tuples for the given subject
        """
        raise Exception("Not implemented")

    def subject_objects(self, predicate=None):
        """
        A generator of (subject, object) tuples for the given predicate
        """
        raise Exception("Not implemented")

    def subject_predicates(self, object=None):
        """
        A generator of (subject, predicate) tuples for the given object
        """
        raise Exception("Not implemented")

    def value(self, subject, predicate=u'http://www.w3.org/1999/02/22-rdf-syntax-ns#value', object=None, default=None, any=False):
        """
        Get a value for a subject/predicate, predicate/object, or
        subject/object pair -- exactly one of subject, predicate,
        object must be None. Useful if one knows that there may only
        be one value.

        It is one of those situations that occur a lot, hence this
        'macro' like utility
        
        :param subject: 
        :param predicate:
        :param object:  -- exactly one must be None
        :param default: -- value to be returned if no values found
        :param any: -- if true, return any value in the case there is more than one, else raise a UniquenessError
        """
        raise Exception("Not implemented")

    # Namespace persistence interface implementation
    def bind(self, prefix, namespace):
        """ """
        c = self._db.cursor()
        try:
            c.execute("INSERT INTO %s_namespace_binds (prefix,uri) VALUES ('%s', '%s')" % (
                self._internedId,
                prefix,
                namespace)
            )
        except:
            pass
        c.close()

    def prefix(self, namespace):
        """ """
        c = self._db.cursor()
        c.execute("SELECT prefix FROM %s_namespace_binds WHERE uri = '%s'"%(
            self._internedId,
            namespace)
        )
        rt = [rtTuple[0] for rtTuple in c.fetchall()]
        c.close()
        return rt and rt[0] or None

    def namespace(self, prefix):
        """ """
        c = self._db.cursor()
        try:
            c.execute("SELECT uri FROM %s_namespace_binds WHERE prefix = '%s'"%(
                self._internedId,
                prefix)
                      )
        except:
            return None
        rt = [rtTuple[0] for rtTuple in c.fetchall()]
        c.close()
        return rt and rt[0] or None

    def namespaces(self):
        """ """
        c = self._db.cursor()
        c.execute("SELECT prefix, uri FROM %s_namespace_binds;"%(
            self._internedId
            )
        )
        rt = c.fetchall()
        c.close()
        for prefix, uri in rt:
            yield prefix, uri
    
    # Transactional interfaces
    def commit(self):
        """ """
        self._db.commit()

    def rollback(self):
        """ """
        self._db.rollback()


table_name_prefixes = [
    '%s_asserted_statements',
    '%s_type_statements',
    '%s_quoted_statements',
    '%s_namespace_binds',
    '%s_literal_statements'
]

CREATE_ASSERTED_STATEMENTS_TABLE = """
CREATE TABLE %s_asserted_statements (
    subject       text not NULL,
    predicate     text not NULL,
    object        text not NULL,
    context       text not NULL,
    termComb      tinyint unsigned not NULL)"""

CREATE_ASSERTED_TYPE_STATEMENTS_TABLE = """
CREATE TABLE %s_type_statements (
    member        text not NULL,
    klass         text not NULL,
    context       text not NULL,
    termComb      tinyint unsigned not NULL)"""

CREATE_LITERAL_STATEMENTS_TABLE = """
CREATE TABLE %s_literal_statements (
    subject       text not NULL,
    predicate     text not NULL,
    object        text,
    context       text not NULL,
    termComb      tinyint unsigned not NULL,
    objLanguage   varchar(3),
    objDatatype   text)"""

CREATE_QUOTED_STATEMENTS_TABLE = """
CREATE TABLE %s_quoted_statements (
    subject       text not NULL,
    predicate     text not NULL,
    object        text,
    context       text not NULL,
    termComb      tinyint unsigned not NULL,
    objLanguage   varchar(3),
    objDatatype   text)"""

CREATE_NS_BINDS_TABLE = """
CREATE TABLE %s_namespace_binds (
    prefix        varchar(20) UNIQUE not NULL,
    uri           text,
    PRIMARY KEY (prefix))"""

