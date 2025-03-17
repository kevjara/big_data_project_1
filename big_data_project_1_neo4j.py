from neo4j import GraphDatabase

'''
Given a disease ID:
    * What is its name?
    * What are drug names that can treat or palliate this disease?
    * What are gene names that cause this disease?
    * Where does this disease occur?
'''
def createDatabaseNodes(session):
    
    def createIndexQuery(label): 
        
        return f"""
            CREATE INDEX FOR (n:{label}) ON (n.id)
        """

    nodesQuery = """
        LOAD CSV WITH HEADERS FROM 'file:///nodes.tsv' AS row
        FIELDTERMINATOR '\t'
        CALL (row) {
            FOREACH (_ IN CASE WHEN row.kind = 'Compound' THEN [1] ELSE [] END | CREATE (:Compound {id: row.id, name: row.name}))
            FOREACH (_ IN CASE WHEN row.kind = 'Gene' THEN [1] ELSE [] END | CREATE (:Gene {id: row.id, name: row.name}))
            FOREACH (_ IN CASE WHEN row.kind = 'Anatomy' THEN [1] ELSE [] END | CREATE (:Anatomy {id: row.id, name: row.name}))
            FOREACH (_ IN CASE WHEN row.kind = 'Disease' THEN [1] ELSE [] END | CREATE (:Disease {id: row.id, name: row.name}))
        } IN TRANSACTIONS OF 5000 ROWS;
    """

    relationshipsQuery = """
        LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row
        FIELDTERMINATOR '\t'
        MATCH (source {id: row.source})
        MATCH (target {id: row.target})
        CALL (row, source, target) {
            FOREACH (_ IN CASE WHEN row.metaedge IN ['CrC', 'DrD'] THEN [1] ELSE [] END | CREATE (source)-[:RESEMBLES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'CtD' THEN [1] ELSE [] END | CREATE (source)-[:TREATS]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'CpC' THEN [1] ELSE [] END | CREATE (source)-[:PALLIATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge IN ['CuG', 'DuA', 'AuG', 'DuG'] THEN [1] ELSE [] END | CREATE (source)-[:UPREGULATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'CbC' THEN [1] ELSE [] END | CREATE (source)-[:BINDS]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge IN ['CdC', 'DdA', 'AdG', 'DdG'] THEN [1] ELSE [] END | CREATE (source)-[:DOWNREGULATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'DlA' THEN [1] ELSE [] END | CREATE (source)-[:LOCALIZES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'DaG' THEN [1] ELSE [] END | CREATE (source)-[:ASSOCIATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'AeG' THEN [1] ELSE [] END | CREATE (source)-[:EXPRESSES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'GrG' THEN [1] ELSE [] END | CREATE (source)-[:REGULATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'GcG' THEN [1] ELSE [] END | CREATE (source)-[:COVARIES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'GiG' THEN [1] ELSE [] END | CREATE (source)-[:INTERACTS]->(target))
        } IN TRANSACTIONS OF 5000 ROWS;
    """

    labels = ['Compound', 'Gene', 'Anatomy', 'Disease']
    #for i in labels:
        #session.run(createIndexQuery(i))
    session.run(nodesQuery)
    session.run(relationshipsQuery)

def getDiseaseQuery(diseaseId):
    query = """
        MATCH (d:Disease {id: '""" + diseaseId + """'})
        OPTIONAL MATCH (c:Compound)-[:TREATS]->(d)
        OPTIONAL MATCH (c:Compound)-[:PALLIATES]->(d)
        OPTIONAL MATCH (d)-[:ASSOCIATES]->(g:Gene)
        OPTIONAL MATCH (d)-[LOCALIZES]->(a:Anatomy)
        RETURN d.name, collect(c.name) AS drug_names, collect(g.name) AS gene_names, collect(a.name) AS disease_locations
    """
    return query

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'bigdatatechnology'))
session = driver.session()

createDatabaseNodes(session)

diseaseId = input("Enter Disease ID: ")
print (session.run(getDiseaseQuery(diseaseId)))

session.close()

'''
Find all compounds that can treat a NEW DISEASE
A compound can treat a new disease if:
    * The compound up-regulates/down-regulates a gene
    * The location of the disease down-regulates/up-regulates the gene respectively
'''