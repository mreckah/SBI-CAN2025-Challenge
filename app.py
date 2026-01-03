import streamlit as st
import os
import glob
import chromadb
import io
import base64
import re
from gtts import gTTS
from dotenv import load_dotenv
from langchain_community.document_loaders import Docx2txtLoader, TextLoader, PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA

load_dotenv()
API_KEY = os.getenv("OPENROUTER_API_KEY")
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = os.getenv("CHROMA_PORT", "8000")
DB_DIR = "chroma_db"
DOCS_DIR = "docs"

st.set_page_config(page_title="DeepSeek Chat", layout="wide")
st.title("DeepSeek Document Chat")

# Ensure directories exist
if not os.path.exists(DOCS_DIR):
    os.makedirs(DOCS_DIR)

@st.cache_resource
def get_vectorstore():
    # Load all documents from the docs folder
    documents = []
    
    # Process files in docs directory
    for file_path in glob.glob(os.path.join(DOCS_DIR, "*")):
        try:
            if file_path.endswith(".docx"):
                loader = Docx2txtLoader(file_path)
            elif file_path.endswith(".txt"):
                loader = TextLoader(file_path)
            elif file_path.endswith(".pdf"):
                loader = PyPDFLoader(file_path)
            else:
                continue
            documents.extend(loader.load())
        except Exception as e:
            st.error(f"Error loading {file_path}: {e}")

    if not documents:
        return None

    # Split
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    chunks = text_splitter.split_documents(documents)

    # Vectorize (Free local embeddings)
    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    
    try:
        # Try connecting to Chroma server if configured
        if CHROMA_HOST:
            vectorstore = Chroma.from_documents(
                chunks,
                embeddings,
                collection_name="docs_collection",
                client=chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
            )
        else:
            vectorstore = Chroma.from_documents(
                chunks, 
                embeddings, 
                persist_directory=DB_DIR
            )
        return vectorstore
    except Exception as e:
        st.warning(f"Could not connect to Chroma server at {CHROMA_HOST}:{CHROMA_PORT}. Falling back to local DB. Error: {e}")
        return Chroma.from_documents(
            chunks, 
            embeddings, 
            persist_directory=DB_DIR
        )

def add_to_vectorstore(file_content, file_name):
    temp_path = os.path.join(DOCS_DIR, file_name)
    with open(temp_path, "wb") as f:
        f.write(file_content)
    
    # Clear cache to force reload of all docs including the new one
    st.cache_resource.clear()
    st.rerun()

# --- 1. Sidebar: Document Management ---
with st.sidebar:
    st.header("Document Setup")
    uploaded_file = st.file_uploader("Upload new document", type=["docx", "txt", "pdf"])
    
    if uploaded_file:
        if st.button("Add to Collection"):
            add_to_vectorstore(uploaded_file.getvalue(), uploaded_file.name)
            st.success(f"Added {uploaded_file.name} to /docs")

    st.divider()
    files_in_docs = os.listdir(DOCS_DIR)
    if files_in_docs:
        st.write("### Managed Documents:")
        for f in files_in_docs:
            st.text(f"📄 {f}")
    else:
        st.info("No documents found in /docs")

    st.divider()
    enable_tts = st.checkbox("Speak Answers", value=True)

def clean_text_for_speech(text):
    text = re.sub(r'\*+', '', text)
    text = re.sub(r'#+', '', text)
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
    text = re.sub(r'(\d+)\.', r'\1', text)
    return text.strip()

def speak_text_hidden(text):
    try:
        clean_text = clean_text_for_speech(text)
        tts = gTTS(text=clean_text, lang='en')
        fp = io.BytesIO()
        tts.write_to_fp(fp)
        fp.seek(0)
        audio_base64 = base64.b64encode(fp.read()).decode()
        audio_html = f"""
            <audio autoplay="true" style="display:none;">
                <source src="data:audio/mp3;base64,{audio_base64}" type="audio/mp3">
            </audio>
        """
        st.components.v1.html(audio_html, height=0)
    except Exception as e:
        st.error(f"TTS Error: {e}")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Initialize Vectorstore
vectorstore = get_vectorstore()

if vectorstore:
    retriever = vectorstore.as_retriever()
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # User Input
    if prompt := st.chat_input("Ask me anything about the documents..."):
        # Add user message to history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate Response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    llm = ChatOpenAI(
                        model="deepseek/deepseek-r1",
                        openai_api_key=API_KEY,
                        openai_api_base="https://openrouter.ai/api/v1",
                        max_tokens=2000,
                        temperature=0.7,
                        default_headers={"HTTP-Referer": "http://localhost:8501"}
                    )
                    qa_chain = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever)
                    
                    response = qa_chain.invoke(prompt)
                    full_response = response["result"]
                    st.markdown(full_response)
                    
                    if enable_tts:
                        speak_text_hidden(full_response)
                    
                    st.session_state.messages.append({"role": "assistant", "content": full_response})
                except Exception as e:
                    st.error(f"Error calling AI: {e}")
else:
    st.warning("Please add some documents to the /docs folder to start chatting.")
