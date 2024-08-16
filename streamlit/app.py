import streamlit as st
import os
import openai
from llama_index.vector_stores.postgres import PGVectorStore
from llama_index.llms.openai import OpenAI
from llama_index.core import VectorStoreIndex
from utils.extract import Config

config = Config()

openai.api_key = os.environ["OPENAI_API_KEY"]

llm = OpenAI(model="gpt-3.5-turbo")
model_vector_len = config["postgres"]["model_vector_len"]
system_prompt = "You are a chatbot. Your primary objective is to answer technical questions and generate new ideas for data science and machine learning projects inspired by GitHub README data. Ensure that you provide relevant sources. Be polite and professional in your interactions."


def get_index():
    vector_store = PGVectorStore.from_params(
        database=config["postgres"]["database"],
        host=config["postgres"]["host"],
        password=config["postgres"]["password"],
        port=config["postgres"]["port"],
        user=config["postgres"]["user"],
        table_name="embeddings",
        embed_dim=model_vector_len,
    )
    return VectorStoreIndex.from_vector_store(vector_store=vector_store)


# Function to get a response (placeholder)
def get_response(user_input):
    index = get_index()

    chat_engine = index.as_chat_engine(
        llm=llm, chat_mode="context", system_prompt=system_prompt
    )

    response = chat_engine.stream_chat(user_input)

    for token in response.response_gen:
        yield token


def reset_chat_engine():
    index = get_index()
    chat_engine = index.as_chat_engine(
        llm=llm, chat_mode="context", system_prompt=system_prompt
    )
    chat_engine.reset()


st.set_page_config(
    page_title="GitHub Project Ideas",
    page_icon=":robot_face:",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None,
)


st.title(":red[GitHub Project Ideas] :robot_face:")
st.header("Find your next great idea!")

user_input = st.text_input(
    "How can I help you?",
    "Can you help me find interesting projects on data science or machine learning?",
    max_chars=100,
)

if st.button("GIVE"):
    st.header("Response")
    with st.spinner(text="Searching the top secret database...(jk)"):
        response_gen = get_response(user_input)
    st.success("Here you go! :smile:")

    st.write_stream(response_gen)

if st.button("CLEAR"):
    with st.spinner(text="Clearing your responses now"):
        reset_chat_engine()
        st.success("Chat engine reset! :clipboard:")

st.caption(
    """
    **Disclaimer:**
    The content and responses provided by this application are for educational purposes only. While efforts are made to ensure the accuracy and relevance of the information, it may not always be up-to-date or correct. Users are encouraged to refer to official documentation, resources, or consult with professionals for verification and accurate information.
    The creators of this application do not accept any responsibility for errors or omissions, or for any actions taken based on the content provided. Use this application at your own risk.
"""
)
