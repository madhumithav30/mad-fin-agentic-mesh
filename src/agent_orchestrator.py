import pandas as pd
import hashlib
import os
import warnings
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

import google.generativeai as genai
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, ToolMessage

warnings.filterwarnings("ignore")


load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=api_key)

def generate_source_data():
    print("--- STEP 1: GENERATING SOURCE DATA ---")
    rows = []
    start_date = datetime.now() - timedelta(days=30)
    for i in range(1, 1001):
        tx_id = f"TX-{1000 + i}"
        email = f"customer_{random.randint(1, 50)}@example.com"
        amount = random.choice([0.00, round(random.uniform(5.0, 5000.0), 2)])
        timestamp = start_date + timedelta(days=random.randint(0, 30))
        rows.append({
            'tx_id': tx_id,
            'customer_email': email,
            'amount': amount,
            'transaction_timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(rows).to_csv("source_raw_unprocessed_transactions.csv", index=False)
    print("✅ Created: source_raw_unprocessed_transactions.csv")

# 3. DEFINE THE TOOLS
@tool
def remove_zero_value_transactions(query: str):
    """Removes 0.00 amount records to cleanse the dataset. Call this first."""
    print("\n🚀 [AI EXECUTING TOOL]: Pruning zero-value records...")
    df = pd.read_csv("source_raw_unprocessed_transactions.csv")
    clean_df = df[df['amount'] > 0]
    clean_df.to_csv("staging_cleansed_transactions.csv", index=False)
    return f"SUCCESS: Cleansed {len(clean_df)} records."

@tool
def secure_pii_and_save_parquet(query: str):
    """Hashes emails for privacy and saves to Parquet. Call this last."""
    print("\n🚀 [AI EXECUTING TOOL]: Masking PII and exporting to Parquet...")
    input_file = "staging_cleansed_transactions.csv"
    if not os.path.exists(input_file):
        df_tmp = pd.read_csv("source_raw_unprocessed_transactions.csv")
        df_tmp = df_tmp[df_tmp['amount'] > 0]
        df_tmp.to_csv(input_file, index=False)
        
    df = pd.read_csv(input_file)
    df['customer_email'] = df['customer_email'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
    df.to_parquet("secure_final_optimized_transactions.parquet")
    return "SUCCESS: Secure Parquet file generated."


def run_agentic_pipeline():
    print("--- STEP 2: RUNNING AGENTIC ORCHESTRATOR ---")
    
    print("🔍 Scanning for available models...")
    available_models = []
    try:
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                clean_name = m.name.replace('models/', '')
                available_models.append(clean_name)
        print(f"✅ Found models: {available_models}")
    except Exception as e:
        print(f"Error listing models: {e}")
        return

    # Pick the best one available (Prefer Flash 1.5, then Pro, then others)
    target_model = None
    for m in ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-pro"]:
        if m in available_models:
            target_model = m
            break
    
    if not target_model:
        target_model = available_models[0] # Just pick the first one if preferred ones aren't there

    print(f"🤖 Initializing Agent with model: {target_model}")
    
    policies = "1. Remove 0.00 transactions. 2. Hash emails (SHA-256). 3. Save as Parquet."
    
    llm = ChatGoogleGenerativeAI(
        model=target_model, 
        google_api_key=api_key, 
        temperature=0
    )
    
    tools = [remove_zero_value_transactions, secure_pii_and_save_parquet]
    llm_with_tools = llm.bind_tools(tools)

    messages = [
        HumanMessage(content=f"Policies: {policies}. Use your tools to process 'source_raw_unprocessed_transactions.csv' correctly.")
    ]

    print("🤖 Agent is reasoning and executing tools...")

    for i in range(5):
        try:
            response = llm_with_tools.invoke(messages)
            messages.append(response)

            if not response.tool_calls:
                print(f"\n🏁 AI Final Response: {response.content}")
                break

            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                selected_tool = {
                    "remove_zero_value_transactions": remove_zero_value_transactions, 
                    "secure_pii_and_save_parquet": secure_pii_and_save_parquet
                }.get(tool_name)
                
                if selected_tool:
                    output = selected_tool.invoke(tool_call["args"])
                    messages.append(ToolMessage(content=output, tool_call_id=tool_call["id"]))
        except Exception as e:
            print(f"❌ Execution error: {e}")
            break

    print("\n--- ✅ SUCCESS: AGENTIC PIPELINE COMPLETE ---")

if __name__ == "__main__":
    generate_source_data()
    run_agentic_pipeline()
    print(f"\nFINAL FILE READY: secure_final_optimized_transactions.parquet")