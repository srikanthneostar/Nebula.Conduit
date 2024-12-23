import sys
import os
from langchain_huggingface import HuggingFaceEmbeddings

embeddings_model = "C:/models/sentence-transformers_all-MiniLM-L12-v2"


def generate_embeddings(input_text):
    try:
        embeddings = HuggingFaceEmbeddings(model_name=embeddings_model)
        embedding = embeddings.embed_query(input_text)
        return embedding
    except Exception as e:
        print(f"Error generating embeddings: {str(e)}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    input_text = sys.argv[1]
    embedding = generate_embeddings(input_text)
