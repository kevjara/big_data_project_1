from neo4j import GraphDatabase
import tkinter as tk
from tkinter import simpledialog, messagebox, scrolledtext, Toplevel

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
    for i in labels:
        session.run(createIndexQuery(i))
    session.run(nodesQuery)
    session.run(relationshipsQuery)

def getDiseaseQuery(session):
    diseaseId = simpledialog.askstring("Input", "Enter Disease ID:")
    if diseaseId is None:
        return

    query = """
        MATCH (d:Disease {id: $diseaseId})
        OPTIONAL MATCH (c:Compound)-[:TREATS|PALLIATES]->(d)
        OPTIONAL MATCH (d)-[:ASSOCIATES]->(g:Gene)
        OPTIONAL MATCH (d)-[:LOCALIZES]->(a:Anatomy)
        RETURN d.name AS disease_name, collect(DISTINCT c.name) AS drug_names, collect(DISTINCT g.name) AS gene_names, collect(DISTINCT a.name) AS disease_locations
    """

    result = session.run(query, diseaseId=diseaseId)

    output = ""
    foundResult = False
    for record in result:
        foundResult = True
        diseaseName = record["disease_name"]
        drugNames = record["drug_names"]
        geneNames = record["gene_names"]
        diseaseLocations = record["disease_locations"]

        output += f"Disease Name: {diseaseName}\n"
        output += f"Compounds (Treat/Palliate): {', '.join(drugNames) if drugNames else 'None'}\n"
        output += f"Associated Genes: {', '.join(geneNames) if geneNames else 'None'}\n"
        output += f"Localized Anatomy: {', '.join(diseaseLocations) if diseaseLocations else 'None'}\n"

    if foundResult:
        messagebox.showinfo("Query Result", output)
    else:
        messagebox.showinfo("Query Result", f"No information found for Disease ID: {diseaseId}")

def findCompoundForDisease(session):
    diseaseId = simpledialog.askstring("Input", "Enter Disease ID:")
    if diseaseId is None:
        return

    query = """
        MATCH (d:Disease {id: $diseaseId})
        MATCH (c:Compound)-[r1:UPREGULATES|DOWNREGULATES]->(g:Gene)
        MATCH (d)-[:LOCALIZES]->(a:Anatomy)
        MATCH (a)-[r2:DOWNREGULATES|UPREGULATES]->(g)
        WHERE
            ((type(r1) = 'UPREGULATES' AND type(r2) = 'DOWNREGULATES') OR
            (type(r1) = 'DOWNREGULATES' AND type(r2) = 'UPREGULATES')) AND
            NOT EXISTS { (c)-[:TREATS]->(d) }
        RETURN DISTINCT c.name AS compound_name;
    """

    result = session.run(query, diseaseId=diseaseId)

    resultWindow = Toplevel(root)
    resultWindow.title("Query Results")
    resultWindow.geometry("600x400")

    textArea = scrolledtext.ScrolledText(resultWindow, wrap=tk.WORD, width=70, height=20, font=("Arial", 10))
    textArea.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    output = "Compounds with New Potential Treatments:\n\n"
    compounds_found = False
    for record in result:
        compounds_found = True
        compound_name = record["compound_name"]
        output += f"- {compound_name}\n"

    if compounds_found:
        textArea.insert(tk.END, output)
    else:
        textArea.insert(tk.END, f"No potential treatments found for Disease ID: {diseaseId}")

    textArea.config(state=tk.DISABLED)

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'bigdatatechnology'))
session = driver.session()

createDatabaseNodes(session)

root = tk.Tk()
root.title("Database GUI")
root.geometry("800x800")

queryOneButton = tk.Button(root, text="Neo4j Query One", command=getDiseaseQuery(session), font=("Arial", 12), padx=10, pady=5)
queryOneButton.pack(pady=50)

queryTwoButton = tk.Button(root, text="Neo4j Query Two", command=findCompoundForDisease(session), font=("Arial", 12), padx=10, pady=5)
queryTwoButton.pack(pady=10)

root.mainloop()

session.close()
driver.close()
