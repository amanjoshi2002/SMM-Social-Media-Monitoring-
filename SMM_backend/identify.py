import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

# Load the more realistic dataset
df = pd.read_csv("realistic_social_media_posts.csv")

# Create a directed graph for mentions and retweets
G = nx.DiGraph()

# Add nodes and edges to the graph based on interactions
for index, row in df.iterrows():
    user = row["Username"]
    if row["Is Harmful"]:  # Only consider harmful content for this network analysis
        # Add an edge for mentions
        if pd.notna(row["Mentions"]):
            mentioned_user = row["Mentions"]
            G.add_edge(user, mentioned_user, label="mention")

        # Add an edge for retweets (assuming each retweet creates a link)
        if row["Retweets"] > 0:
            G.add_edge(user, "retweet_" + str(row["Post ID"]), label="retweet")

# Analyze centrality to identify key players in the harmful network
centrality = nx.degree_centrality(G)
top_influencers = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]

print("Top Influencers in Harmful Content Network:")
for influencer, score in top_influencers:
    print(f"{influencer}: {score:.2f}")

# Draw the network of harmful content propagation
plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G)
nx.draw(
    G,
    pos,
    with_labels=True,
    node_size=700,
    font_size=10,
    node_color="lightcoral",
    edge_color="gray",
)
plt.title("Harmful Content Network", fontsize=16)
plt.show()


# Step 1: Find the earliest post with harmful content
harmful_posts = df[df["Is Harmful"]]
original_post = harmful_posts.sort_values(by="Timestamp").iloc[0]
print(
    f"Original harmful post by: {original_post['Username']} at {original_post['Timestamp']}"
)

# Step 2: Track the spread of this harmful post via retweets
print("\nPropagation of harmful content:")
for index, row in harmful_posts.iterrows():
    if row["Retweets"] > 0:
        print(
            f"Post by {row['Username']} with {row['Retweets']} retweets on {row['Timestamp']}"
        )

# Step 3: Generate a propagation chain for harmful content
G_propagation = nx.DiGraph()

# Create a network of propagation (user -> retweet)
for index, row in harmful_posts.iterrows():
    user = row["Username"]
    retweets = row["Retweets"]
    if retweets > 0:
        # Add edges representing the propagation of harmful content
        G_propagation.add_edge(user, f'retweet_{row["Post ID"]}', label="retweet")

# Draw propagation chain
plt.figure(figsize=(10, 6))
pos = nx.spring_layout(G_propagation)
nx.draw(G_propagation, pos, with_labels=True, node_color="lightblue", edge_color="gray")
plt.title("Propagation Chain of Harmful Content", fontsize=16)
plt.show()