import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from llama_index.core import (
    StorageContext,
    VectorStoreIndex,
    SimpleDirectoryReader,
)
import openai
from airflow.exceptions import AirflowFailException
from llama_index.vector_stores.postgres import PGVectorStore
from llama_index.core.node_parser import SentenceSplitter

from .extract import Config

try:
    config = Config()
except:
    print("Error loadng config file in embed.py")


try:
    openai.api_key = os.environ["OPENAI_API_KEY"].strip()
except:
    print("Error loading OpenAI API key")


postgres_connection_id = config["postgres"]["postgres_connection_id"]
model_vector_len = config["postgres"]["model_vector_len"]
gcs_connection_id = config["gcs"]["gcs_connection_id"]


def check_pgvector_extension():
    hook = PostgresHook(postgres_conn_id=postgres_connection_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector';")
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result is not None


def create_tables_if_not_exist():
    pghook = PostgresHook(postgres_conn_id=postgres_connection_id)
    conn = pghook.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files(
            id SERIAL PRIMARY KEY,
            file_name VARCHAR(255) UNIQUE NOT NULL
        );
    """
    )
    conn.commit()
    cur.close()
    conn.close()


def list_new_files(**kwargs):
    processed_files_bucket_name = kwargs.get("processed_files_bucket_name")
    # Behavior: You can provide a default value to return if the key is missing.
    # Example: bucket_name = kwargs.get("bucket_name", "default_value") will return "default_value" if "bucket_name" is not found.

    # List files in GCS bucket
    gcshook = GCSHook(gcp_conn_id=gcs_connection_id)
    file_list = gcshook.list(bucket_name=processed_files_bucket_name)

    # PG connection to get processed file names
    pghook = PostgresHook(postgres_conn_id=postgres_connection_id)
    conn = pghook.get_conn()
    cur = conn.cursor()
    processed_file_names = []
    cur.execute(
        """
        SELECT file_name FROM processed_files
        """
    )
    results = cur.fetchall()
    for row in results:
        processed_file_names.append(row[0])

    new_files_to_process = []
    for file in file_list:
        if file not in processed_file_names:
            new_files_to_process.append(file)

    cur.close()
    conn.close()

    return new_files_to_process


def get_meta(file_path):
    file_name = os.path.basename(file_path)
    return {"file_name": file_name, "file_path": file_path}


def download_files(**kwargs):
    processed_files_bucket_name = kwargs.get("processed_files_bucket_name")
    new_files = kwargs.get("new_files")
    temp_data_path = config["files"]["temp_data_path"]

    gcshook = GCSHook(gcp_conn_id=gcs_connection_id)
    downloaded_files = []

    try:
        for file_name in new_files:
            print(f"Attempting to download: {file_name}")
            # Construct the full path for the temporary file
            temp_file_name = os.path.join(temp_data_path, file_name)

            # Ensure the directory for the file exists; create directories if needed
            os.makedirs(os.path.dirname(temp_file_name), exist_ok=True)

            # Download the file from Google Cloud Storage (GCS)
            if gcshook.exists(
                bucket_name=processed_files_bucket_name, object_name=file_name
            ):
                # Download the file from Google Cloud Storage (GCS)
                gcshook.download(
                    bucket_name=processed_files_bucket_name,
                    object_name=file_name,
                    filename=temp_file_name,
                )
                print(f"File {file_name} downloaded successfully.")
                # Add the path of the downloaded file to the list of downloaded files
                downloaded_files.append(temp_file_name)

    except Exception as e:
        # Raise an AirflowFailException if an error occurs during file download
        # ADD a clasue here - only download if file not found in temp_data_path!
        raise AirflowFailException("Error downloading from GCS bucket") from e

    return downloaded_files


# In steps above, we make sure that we only download files that aren't already in the vector index. So subsequently, any files that reach this step are the new ones and need to be processed accordingly in vector index. so we will vectorize and load them.
def generate_embeddings_and_store(downloaded_files):
    try:
        documents = SimpleDirectoryReader(
            input_files=downloaded_files, file_metadata=get_meta
        ).load_data()
        print(f"SimpleDirectoryReader works. Loaded {len(documents)}")

        for doc in documents:
            doc.excluded_llm_metadata_keys = ["file_path"]
            doc.excluded_embed_metadata_keys = ["file_path"]
            print(f"Document Metadata: {doc.metadata}")

        parser = SentenceSplitter(
            chunk_size=256,
            chunk_overlap=20,
            include_metadata=True,
            include_prev_next_rel=True,
        )
        # parser = MarkdownNodeParser(include_metadata=True, include_prev_next_rel=True)
        print("Parsed using sentence splitter")
        # Could you sentence splitter here.

        nodes = parser.get_nodes_from_documents(documents)
        print(f"Got nodes. Length: {len(nodes)}")

    except Exception as e:
        print(f"Encountered issue with directory reader: {e}")
        raise

    try:
        # Create vector store
        vector_store = PGVectorStore.from_params(
            database=config["postgres"]["database"],
            host=config["postgres"]["host"],
            password=config["postgres"]["password"],
            port=config["postgres"]["port"],
            user=config["postgres"]["user"],
            table_name="embeddings",  # ideally in a config
            embed_dim=model_vector_len,
        )
        # Create storage context
        storage_context = StorageContext.from_defaults(vector_store=vector_store)

        # Create and setup the vector store index
        VectorStoreIndex(
            nodes,
            storage_context=storage_context,
            show_progress=True,
        )
        print("Vector index generated and stored in embeddings table!")
        return True
    except Exception as e:
        print(f"Error generating embeddings and storing: {e}")
        return False


def update_processed_files_table():
    try:
        pghook = PostgresHook(postgres_conn_id=postgres_connection_id)
        conn = pghook.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO processed_files (file_name)
            SELECT 
                (metadata_ ->> 'file_name')::text AS file_name
            FROM 
                data_embeddings
            ON CONFLICT (file_name) DO NOTHING;
            """
        )
        conn.commit()
        print("Successfully updated processed_files table in processed_files table")
        cur.close()
        conn.close()
    except Exception as e:
        raise AirflowFailException(f"Failed to update processed_files table: {e}")
