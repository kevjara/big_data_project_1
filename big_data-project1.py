import tkinter as tk
from tkinter import simpledialog, messagebox, scrolledtext
import pandas as pd
from pymongo import MongoClient

# Relevant edge types
VALID_EDGES = {
    "CtD": "treats",
    "CpD": "palliates",
    "CuG": "upregulates",
    "CdG": "downregulates",
    "DaG": "associates",
    "DlA": "localizes",
    "AuG": "upregulates",
    "AdG": "downregulates"
}

# Load nodes into a dictionary for quick lookup
# Assumes nodes.tsv is in the same directory as python file
nodes_df = pd.read_csv("nodes.tsv", sep="\t")
nodes_dict = nodes_df.set_index("id").to_dict(orient="index")

# Load edges and filter only relevant ones
# Assumes edges.tsv is in the same directory as python file
edges_df = pd.read_csv("edges.tsv", sep="\t")
edges_df = edges_df[edges_df["metaedge"].isin(VALID_EDGES.keys())]

# Create a dictionary to store the connections for each node
graph = {}

for node_id, node_info in nodes_dict.items():
    graph[node_id] = {
        "name": node_info["name"],
        "kind": node_info["kind"],
        "connections": []
    }

# Build the graph with connections (edges)
# index of edges_df not needed only row so _ placeholder used
for _, row in edges_df.iterrows():
    source = row["source"]
    metaedge = row["metaedge"]
    target = row["target"]

    # Sets edge_type to the name of the edge
    edge_type = VALID_EDGES[metaedge]

    # Adds the connections to the source node
    if source in graph:
        graph[source]["connections"].append({
            "target_id": target,
            "edge_type": edge_type
        })

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["graph_db"]
node_collection = db.node

# Drop the database to clear all previous data
client.drop_database("graph_db")

# Insert data into database in batches of 5000
batch_size = 5000
batch = []
for node_id, node_info in graph.items():
    doc = {
        "_id": node_id,
        "name": node_info["name"],
        "kind": node_info["kind"],
        "connections": node_info["connections"]
    }
    batch.append(doc)
    
    if len(batch) >= batch_size:
        node_collection.insert_many(batch)
        batch = []

# Insert remaining batch
if batch:
    node_collection.insert_many(batch)


def mongo_query_one(disease_id):
    pipeline = [
        # Find the disease node
        {"$match": {"_id": disease_id}},

        # Lookup compounds that treat (CtD) or palliate (CpD) this disease
        {
            "$lookup": {
                "from": "node",
                "let": {"disease_id": "$_id"},
                "pipeline": [
                    {"$match": {"kind": "Compound"}},  # Start with compounds
                    {"$unwind": "$connections"},  # Expand connections array
                    {"$match": {  # Filter compounds that have a connection to this disease
                        "$expr": {
                            "$and": [
                                {"$eq": ["$connections.target_id", "$$disease_id"]},
                                {"$in": ["$connections.edge_type", ["treats", "palliates"]]}
                            ]
                        }
                    }},
                    {"$project": {"_id": 0, "name": 1}}  # Output only compound names
                ],
                "as": "compounds"
            }
        },

        # Lookup genes associated with this disease (DaG)
        {
            "$lookup": {
                "from": "node",
                "localField": "connections.target_id",
                "foreignField": "_id",
                "as": "associated_genes",
                "pipeline": [
                    {"$match": {"kind": "Gene"}},
                    {"$project": {"_id": 0, "name": 1}}
                ]
            }
        },

        # Lookup anatomy where this disease localizes (DlA)
        {
            "$lookup": {
                "from": "node",
                "localField": "connections.target_id",
                "foreignField": "_id",
                "as": "localized_anatomy",
                "pipeline": [
                    {"$match": {"kind": "Anatomy"}},
                    {"$project": {"_id": 0, "name": 1}}
                ]
            }
        },

        # Reshape the output
        {
            "$project": {
                "_id": 0,
                "disease_name": "$name",
                "treats_or_palliates_compounds": "$compounds.name",
                "associated_genes": "$associated_genes.name",
                "localized_anatomy": "$localized_anatomy.name"
            }
        }
    ]

    result = list(node_collection.aggregate(pipeline))
    return result[0] if result else {"error": "Disease node not found"}

def mongo_query_two():
    pipeline = [
        # Start with compounds
        {"$match": {"kind": "Compound"}},

        # Lookup genes regulated by the compound (CuG, CdG)
        {
            "$lookup": {
                "from": "node",
                "localField": "connections.target_id",
                "foreignField": "_id",
                "as": "regulated_genes",
                "pipeline": [
                    {"$match": {"kind": "Gene"}},
                    {"$project": {"_id": 1}}
                ]
            }
        },

        # Lookup anatomies that regulate those genes (AuG, AdG)
        {
            "$lookup": {
                "from": "node",
                "let": {"gene_ids": "$regulated_genes._id"},
                "pipeline": [
                    {"$match": {"kind": "Anatomy"}},
                    {"$unwind": "$connections"},
                    {"$match": {"$expr": {"$in": ["$connections.target_id", "$$gene_ids"]}}},
                    {"$project": {"_id": 1}}
                ],
                "as": "associated_anatomies"
            }
        },

        # Lookup diseases localized to those anatomies (DlA)
        {
            "$lookup": {
                "from": "node",
                "let": {"anatomy_ids": "$associated_anatomies._id"},
                "pipeline": [
                    {"$match": {"kind": "Disease"}},
                    {"$unwind": "$connections"},
                    {"$match": {"$expr": {"$in": ["$connections.target_id", "$$anatomy_ids"]}}},
                    {"$project": {"_id": 1}}
                ],
                "as": "potential_diseases"
            }
        },

        # Lookup diseases the compound already treats (CtD, CpD)

       {
            "$project": {
                "_id": 1,
                "name": 1,
                "regulated_genes": 1,
                "associated_anatomies": 1,
                "potential_diseases": 1,
                "existing_treatments": {
                    "$filter": {
                        "input": "$connections",
                        "as": "conn",
                        "cond": {"$eq": ["$$conn.edge_type", "treats"]}
                    }
                }
            }
        },

        # Data preperation
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "regulated_genes": 1,
                "associated_anatomies": 1,
                "potential_diseases": 1,
                "existing_treatments": 1
            }
        },

        # Only keep compounds that have new potential treatments
        {"$match": {"potential_diseases": {"$ne": []}, "existing_treatments": []}},

        # Output only the compound id and name
        { 
            "$project": { 
                "_id": 1, 
                "name": 1
            }
        }
    ]

    result = list(node_collection.aggregate(pipeline))
    return result

def run_mongo_query_one():
    disease_id = simpledialog.askstring("Input", "Enter Disease ID:")

    if disease_id:
        result = mongo_query_one(disease_id)

        if "error" in result:
            messagebox.showerror("Error", result["error"])
        else:
            output = f"Disease Name: {result['disease_name']}\n"
            output += f"Compounds (Treat/Palliate): {', '.join(result['treats_or_palliates_compounds']) if result['treats_or_palliates_compounds'] else 'None'}\n"
            output += f"Associated Genes: {', '.join(result['associated_genes']) if result['associated_genes'] else 'None'}\n"
            output += f"Localized Anatomy: {', '.join(result['localized_anatomy']) if result['localized_anatomy'] else 'None'}"
            messagebox.showinfo("Query Result", output)

# Takes approx. 20 mins to output and while calculating window does not respond
# This is normal it is just executing the query
def run_mongo_query_two():
    result = mongo_query_two()

    if not result:
        messagebox.showinfo("Query Result", "No new potential treatments found.")
        return

    # Create window to display results
    result_window = tk.Toplevel(root)
    result_window.title("Query Results")
    result_window.geometry("600x400")

    # Create a scrollable text widget
    text_area = scrolledtext.ScrolledText(result_window, wrap=tk.WORD, width=70, height=20, font=("Arial", 10))
    text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    # Format and insert results
    output = "Compounds with New Potential Treatments:\n\n"
    output += "\n".join([f"- {comp['_id']} ({comp['name']})" for comp in result])
    
    text_area.insert(tk.END, output)
    text_area.config(state=tk.DISABLED) # Read only text

#Create GUI
root = tk.Tk()
root.title("Database GUI")
root.geometry("800x800")

mongo_query_one_button = tk.Button(root, text="MongoDB Query One", command=run_mongo_query_one, font=("Arial", 12), padx=10, pady=5)
mongo_query_one_button.pack(pady=50)

mongo_query_two_button = tk.Button(root, text="MongoDB Query Two", command=run_mongo_query_two, font=("Arial", 12), padx=10, pady=5)
mongo_query_two_button.pack(pady=10)

root.mainloop()
