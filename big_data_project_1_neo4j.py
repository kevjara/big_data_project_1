from neo4j import GraphDatabase

'''
Given a disease ID:
    * What is its name?
    * What are drug names that can treat or palliate this disease?
    * What are gene names that cause this disease?
    * Where does this disease occur?
'''
def createDatabase(driver, database_name):
    driver.execute_query(f"CREATE DATABASE {database_name}")



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
            FOREACH (_ IN CASE WHEN row.metaedge = 'CpD' THEN [1] ELSE [] END | CREATE (source)-[:PALLIATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge IN ['CuG', 'AuG', 'DuG'] THEN [1] ELSE [] END | CREATE (source)-[:UPREGULATES]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge = 'CbC' THEN [1] ELSE [] END | CREATE (source)-[:BINDS]->(target))
            FOREACH (_ IN CASE WHEN row.metaedge IN ['CdC', 'AdG', 'DdG'] THEN [1] ELSE [] END | CREATE (source)-[:DOWNREGULATES]->(target))
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

def getDiseaseQuery(session, diseaseId):
    query = """
        MATCH (d:Disease {id: '""" + diseaseId + """'})
        OPTIONAL MATCH (c:Compound)-[:TREATS|PALLIATES]->(d)
        OPTIONAL MATCH (d)-[:ASSOCIATES]->(g:Gene)
        OPTIONAL MATCH (d)-[LOCALIZES]->(a:Anatomy)
        RETURN d.name, collect(DISTINCT c.name) AS drug_names, collect(DISTINCT g.name) AS gene_names, collect(DISTINCT a.name) AS disease_locations
    """

    return session.run(query)

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'bigdatatechnology'))
session = driver.session()

#createDatabaseNodes(session)

diseaseId = input("Enter Disease ID: ")
diseaseDetails = [record['d.name'] for record in getDiseaseQuery(session, diseaseId)]
for i in diseaseDetails:
    print(i)

'''
Find all compounds that can treat a NEW DISEASE
A compound can treat a new disease if:
    * The compound up-regulates/down-regulates a gene
    * The location of the disease down-regulates/up-regulates the gene respectively
'''

def findCompoundForDisease(session, diseaseId):
    query = """
        MATCH (d:Disease {id: '""" + diseaseId + """'})
        MATCH (c:Compound)-[r1:UPREGULATES|DOWNREGULATES]->(g:Gene)
        MATCH (d)-[:LOCALIZES]->(a:Anatomy)
        MATCH (a)-[r2:DOWNREGULATES|UPREGULATES]->(g)
        WHERE
            ((type(r1) = 'UPREGULATES' AND type(r2) = 'DOWNREGULATES') OR
            (type(r1) = 'DOWNREGULATES' AND type(r2) = 'UPREGULATES')) AND
            NOT EXISTS { (c)-[:TREATS]->(d) }
        RETURN DISTINCT c.name;
    """

    return session.run(query)

diseaseId = input("Enter Disease Id: ")
potentialCompounds = [record["c.name"] for record in findCompoundForDisease(session, diseaseId)]
for i in potentialCompounds:
    print(i)
session.close()
driver.close()
