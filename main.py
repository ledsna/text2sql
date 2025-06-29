import streamlit as st
from chat import run_react_agent, graph
import io
import sys
import subprocess
import time
import re
from contextlib import redirect_stdout

st.set_page_config(
    page_title="ReAct SQL Agent",
    page_icon="🤖",
    layout="wide"
)

hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.stDeployButton {display: none;}
#stToolbar {display: none;}
#stDecoration {display: none;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

def start_ollama():
    try:
        result = subprocess.run(["ollama", "list"], capture_output=True, text=True, timeout=10)
        if "qwen3" not in result.stdout:
            st.info("Starting Ollama with qwen3 model...")
            subprocess.Popen(["ollama", "run", "qwen3"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(5)
        return True
    except Exception as e:
        st.error(f"Could not start Ollama: {e}")
        return False

def remove_think_tags(text):
    if not text:
        return ""
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    cleaned = re.sub(r'</think>+', '', cleaned)
    cleaned = re.sub(r'<think>+', '', cleaned)
    return cleaned.strip()

st.title("ReAct Text-to-SQL Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

if "ollama_started" not in st.session_state:
    st.session_state.ollama_started = False

if "is_thinking" not in st.session_state:
    st.session_state.is_thinking = False

if "current_question" not in st.session_state:
    st.session_state.current_question = None

if "question_being_processed" not in st.session_state:
    st.session_state.question_being_processed = False

if not st.session_state.ollama_started:
    if start_ollama():
        st.session_state.ollama_started = True
        st.success("Ollama started successfully!")
    else:
        st.error("Failed to start Ollama. Please make sure it's installed and accessible.")

def capture_agent_output(question):
    f = io.StringIO()
    with redirect_stdout(f):
        run_react_agent(question)
    return f.getvalue()

def parse_agent_output(output):
    sections = {}
    current_section = None
    current_content = []
    
    for line in output.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        if line.startswith('REASONING STEP:'):
            if current_section and current_content:
                sections[current_section] = '\n'.join(current_content)
            current_section = 'REASONING STEP:'
            current_content = []
        elif line.startswith('VALIDATION STEP:'):
            if current_section and current_content:
                sections[current_section] = '\n'.join(current_content)
            current_section = 'VALIDATION STEP:'
            current_content = []
        elif line.startswith('EXECUTION STEP:'):
            if current_section and current_content:
                sections[current_section] = '\n'.join(current_content)
            current_section = 'EXECUTION STEP:'
            current_content = []
        elif line.startswith('FINAL ANSWER:'):
            if current_section and current_content:
                sections[current_section] = '\n'.join(current_content)
            current_section = 'FINAL ANSWER:'
            current_content = []
        elif current_section:
            current_content.append(line)
    
    if current_section and current_content:
        sections[current_section] = '\n'.join(current_content)
    
    return sections

def render_message_details(sections, message_index):
    if not sections:
        return
    
    st.write("---")
    st.write("**Model's Reasoning Process:**")
    
    for section_name, content in sections.items():
        if section_name == "FINAL ANSWER:":
            continue
            
        if content and content.strip():
            with st.expander(f"📋 {section_name}", expanded=False):
                st.markdown(content.strip())

with st.form("chat_form", clear_on_submit=True):
    question = st.text_input(
        "Ask a question about your database...",
        key="question_input",
        disabled=False
    )
    submitted = st.form_submit_button(
        "Send",
        disabled=st.session_state.question_being_processed
    )

for i, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.write(message["content"])
        
        if message["role"] == "assistant" and "sections" in message:
            render_message_details(message["sections"], i)

if submitted and question and not st.session_state.question_being_processed:
    st.session_state.current_question = question
    st.session_state.is_thinking = True
    st.session_state.question_being_processed = True
    st.rerun()

if st.session_state.is_thinking and st.session_state.current_question:
    if not any(msg.get("content") == st.session_state.current_question for msg in st.session_state.messages):
        st.session_state.messages.append({"role": "user", "content": st.session_state.current_question})
    
    with st.chat_message("user"):
        st.write(st.session_state.current_question)
    
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                output = capture_agent_output(st.session_state.current_question)
                sections = parse_agent_output(output)
                
                final_answer = sections.get("FINAL ANSWER:", "No answer generated")
                clean_final_answer = remove_think_tags(final_answer)
                
                st.write(clean_final_answer)
                render_message_details(sections, len(st.session_state.messages))
                
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": clean_final_answer,
                    "sections": sections
                })
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"Error occurred: {str(e)}"
                })
    
    st.session_state.is_thinking = False
    st.session_state.current_question = None
    st.session_state.question_being_processed = False
    st.rerun()

if st.button("Clear History"):
    st.session_state.messages = []
    st.session_state.ollama_started = False
    st.rerun() 