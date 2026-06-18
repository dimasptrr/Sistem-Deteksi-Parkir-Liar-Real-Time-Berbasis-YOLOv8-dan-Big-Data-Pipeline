from __future__ import annotations

import socket
import sys
import threading
import time
from pathlib import Path

import streamlit as st

# Make the project root importable when running: streamlit run frontend/app.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Port on which the FastAPI backend will run
FASTAPI_PORT = 8080

def is_port_in_use(port: int) -> bool:
    """Check if the local port is already occupied."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Use localhost loopback address
        return s.connect_ex(('127.0.0.1', port)) == 0

def run_fastapi_server():
    """Runs the Uvicorn FastAPI server programmatically."""
    import uvicorn
    from frontend.server import app
    # Run uvicorn on all network interfaces
    uvicorn.run(app, host="0.0.0.0", port=FASTAPI_PORT, log_level="warning")

@st.cache_resource
def start_backend_server() -> bool:
    """Starts the FastAPI backend server exactly once during the Streamlit session."""
    if not is_port_in_use(FASTAPI_PORT):
        server_thread = threading.Thread(target=run_fastapi_server, daemon=True)
        server_thread.start()
        # Poll the port until it is open (maximum 8 seconds)
        for _ in range(80):
            if is_port_in_use(FASTAPI_PORT):
                break
            time.sleep(0.1)
    return True

# Boot up the FastAPI server in a background thread if not already running
start_backend_server()

# Configure Streamlit page layout
st.set_page_config(
    page_title="Sistem Deteksi Parkir Liar Real-time",
    layout="wide",
    page_icon="🚫"
)

# Apply custom styling to make the iframe completely fullscreen and hide Streamlit headers/footers
st.markdown(
    """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .block-container {
            padding-top: 0px !important;
            padding-bottom: 0px !important;
            padding-left: 0px !important;
            padding-right: 0px !important;
            margin: 0px !important;
            max-width: 100% !important;
        }
        iframe {
            border: none;
            width: 100vw;
            height: calc(100vh - 4px);
            margin: 0px;
            padding: 0px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Resolve the visiting hostname dynamically from request headers
hostname = "127.0.0.1"
try:
    host_header = st.context.headers.get("host", "")
    if host_header:
        hostname = host_header.split(":")[0]
        # Force loopback to IPv4 127.0.0.1 to avoid Windows IPv6 localhost (::1) binding issues
        if hostname == "localhost":
            hostname = "127.0.0.1"
except Exception:
    pass

# Render the FastAPI dashboard in a fullscreen iframe natively
st.iframe(f"http://{hostname}:{FASTAPI_PORT}", height=850)
