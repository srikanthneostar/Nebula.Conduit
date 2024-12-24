import sys
from langchain_huggingface import HuggingFaceEmbeddings

# Path to your model
embeddings_model = "C:/models/sentence-transformers_all-MiniLM-L12-v2"

def generate_embeddings(input_text):
    embeddings = HuggingFaceEmbeddings(model_name=embeddings_model)
    embedding = embeddings.embed_query(input_text)
    return embedding

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python app.py '<text_to_embed>'", file=sys.stderr)
        sys.exit(1)
    
    input_text = sys.argv[1]
    try:
        embedding = generate_embeddings(input_text)
        # print(json.dumps(embedding))
    except Exception as e:
        print(f"Error generating embeddings: {e}", file=sys.stderr)
        sys.exit(1)
