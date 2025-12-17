import streamlit as st
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from typing import Optional, Dict, List, Any
import ast
import json
import re
import traceback
from datetime import datetime

load_dotenv()

if 'dataset' not in st.session_state:
    st.session_state.dataset = None
if 'workflow_step' not in st.session_state:
    st.session_state.workflow_step = 1
if 'query_executed' not in st.session_state:
    st.session_state.query_executed = False
if 'agent_fq_name' not in st.session_state:
    st.session_state.agent_fq_name = None
if 'agent_db_name' not in st.session_state:
    st.session_state.agent_db_name = None
if 'agent_schema_name' not in st.session_state:
    st.session_state.agent_schema_name = None
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 0

@st.cache_resource
def get_snowflake_connection():
    """Get or create Snowflake connection (cached)"""
    try:
        session = Session.get_active_session()
        # Verify session is actually valid by checking if it has a connection
        if session is None:
            raise ValueError("Session is None")
        # Test the session by running a simple query
        session.sql("SELECT 1").collect()
        return session
    except Exception:
        try:
            connection_parameters = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "LOCAL"),
                "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
            }
            return Session.builder.configs(connection_parameters).create()
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")
            return None

@st.cache_data(ttl=300)
def get_agent_list(_session) -> List[str]:
    """Get list of agents (cached for 5 minutes)"""
    try:
        # Use RESULT_SCAN to get reliable column names from SHOW command
        _session.sql('SHOW AGENTS IN ACCOUNT').collect()
        
        agents_df = _session.sql('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').to_pandas()

        # Now we can reliably access columns - they should be properly named
        # Column names are typically quoted: "name", "database_name", "schema_name"
        agent_df = agents_df[["database_name", "schema_name", "name"]].copy()
        agent_df['"fully_qualified_agent_name"'] = (
            agent_df["database_name"] + "." + 
            agent_df["schema_name"] + "." + 
            agent_df["name"]
        )
        return agent_df
        
    except Exception as e:
        st.error(f"Failed to load agents: {e}")
        st.error(f"Error details: {traceback.format_exc()}")
        return []

def build_query(agent_name: str, agent_db_name: str, agent_schema_name: str, record_id: Optional[str] = None, user_feedback: Optional[str] = None) -> str:
    """Build the query with optional filters for AGENT_NAME and THREAD_ID"""
    
    base_query = f"""
WITH RESULTS AS (SELECT 
    TIMESTAMP AS TS,
    RECORD_ATTRIBUTES:"snow.ai.observability.object.name" AS AGENT_NAME,
    RECORD_ATTRIBUTES:"ai.observability.record_id" AS RECORD_ID, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id" AS THREAD_ID,
    RECORD_ATTRIBUTES:"ai.observability.record_root.input" AS INPUT_QUERY,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.thinking_response" AS AGENT_PLANNING,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.sql_query" AS GENERATED_SQL,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.result" AS SQL_RESULT,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_search.results" AS CORTEX_SEARCH_RESULT,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.results" AS CUSTOM_TOOL_RESULT,
    RECORD_ATTRIBUTES:"ai.observability.record_root.output" AS AGENT_RESPONSE, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.model" AS REASONING_MODEL, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool.name" AS AVAILABLE_TOOLS, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool_selection.name" AS TOOL_SELECTION,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_search.name" AS CSS_NAME,

    RECORD:"name" as TOOL_CALL,

    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool_selection.type" AS TOOL_TYPE,
    CASE 
        WHEN RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.id" IS NOT NULL     
        AND RECORD:"name" NOT IN ('SqlExecution', 'SqlExecution_CortexAnalyst','CortexChartToolImpl-data_to_chart')
 
        THEN OBJECT_CONSTRUCT (
            'tool_name',
            TOOL_CALL,
            'tool_type',
            TOOL_TYPE,
            'tool_output',
            OBJECT_CONSTRUCT(
            'SQL',
            GENERATED_SQL,
            'search results',
            CORTEX_SEARCH_RESULT,
            'CUSTOM_TOOL_RESULT',
            CUSTOM_TOOL_RESULT
            ))
        ELSE NULL
        END AS TOOL_ARRAY,

    CASE
        WHEN VALUE:"positive"='true' THEN 1
        WHEN VALUE:"positive"='false'THEN 0
        ELSE NULL
        END AS USER_FEEDBACK,
    VALUE:"feedback_message" AS USER_FEEDBACK_MESSAGE,
    RECORD:"name" as OPERATION
    
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    '{agent_db_name}', 
    '{agent_schema_name}', 
    '{agent_name}', 
    'CORTEX AGENT'))"""
    
    filters = []
    if record_id:
        filters.append(f"AND RECORD_ID = '{record_id}'")
    
    query = base_query + " " + " ".join(filters)
    query += """
    ORDER BY THREAD_ID, TS, START_TIMESTAMP ASC)

    SELECT 
        RECORD_ID,
        MIN(TS) AS START_TS,
        MAX(TS) AS END_TS,
        DATEDIFF(SECOND, START_TS, END_TS)::FLOAT AS LATENCY, 
        MIN(AGENT_NAME) AS AGENT_NAME,
        MIN(INPUT_QUERY) AS INPUT_QUERY,
        MIN(AGENT_RESPONSE) AS AGENT_RESPONSE,
        MIN(AGENT_PLANNING) AS AGENT_PLANNING,
        ARRAY_AGG(TOOL_ARRAY) WITHIN GROUP (ORDER BY TS ASC) AS TOOL_ARRAY,
        MIN(USER_FEEDBACK) AS USER_FEEDBACKS,
        MIN(USER_FEEDBACK_MESSAGE) AS USER_FEEDBACK_MESSAGES
    
        FROM RESULTS    
        GROUP BY RECORD_ID"""
    
    if user_feedback == 'Positive Feedback Only':
        query += " HAVING USER_FEEDBACKS = 1"
    elif user_feedback == 'Negative Feedback Only':
        query += " HAVING USER_FEEDBACKS = 0"
    elif user_feedback == 'Any Feedback':
        query += " HAVING USER_FEEDBACKS IS NOT NULL"
    
    query += " ORDER BY START_TS DESC;"
    return query

def add_tool_sequence(tool_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    new_order = ['tool_sequence', 'tool_name', 'tool_output']
    drop_list = ['SqlExecution', 'SqlExecution_CortexAnalyst', 'CortexChartToolImpl-data_to_chart']

    # 1. Remove unwanted tools
    filtered_tools = [
        tool
        for tool in tool_list
        if tool.get('tool_name') not in drop_list
    ]

    # 2. Add sequence and reorder keys
    updated_tools = []
    for idx, tool in enumerate(filtered_tools):
        tool_name = tool['tool_name']
        if tool_name.startswith('CortexAnalystTool_'):
            tool_name = tool_name.replace('CortexAnalystTool_', '', 1)

        elif tool_name.startswith('CortexSearchService_'):
            tool_name = 'cortex_search'

        elif tool_name.startswith('ToolCall-'):
            tool_name = tool_name.replace('ToolCall-', '', 1)

        updated_tool = {
            **tool,
            "tool_sequence": idx + 1,
            "tool_name": tool_name
        }


        # Reorder output keys
        reordered_tool = {k: updated_tool[k] for k in new_order if k in updated_tool}

        updated_tools.append(reordered_tool)

    # 3. Define return var and return it
    final_tool_list = updated_tools
    return final_tool_list

@st.cache_data(ttl=600)
def execute_query_and_postprocess(_session, query: str) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame (cached for 10 minutes)
        Perform some operations in pandas to clean up data."""
    try:
        data = _session.sql(query)
        df = data.to_pandas()

        def clean_text(s):
            """
            Normalize a text cell:
            - If it looks like a Python/JSON quoted literal, try ast.literal_eval to unescape safely.
            - Otherwise try a unicode_escape decode as a fallback.
            - Remove matching surrounding quotes (single or double), repeating a few times to handle nested quoting.
            - Remove any leftover backslashes, collapse whitespace, strip.
            """
            if pd.isna(s):
                return s
        
            s = str(s).strip()
        
            # Try to safely unescape if it looks like a quoted literal.
            # Using ast.literal_eval is safest when strings are like: '"abc"', "'abc'", r'\"abc\"'
            try:
                # Only try literal_eval for strings that start with a quote or an escape-quote (cheap heuristic)
                if s.startswith('"') or s.startswith("'") or s.startswith(r'\"') or s.startswith(r"\'"):
                    s_eval = ast.literal_eval(s)
                    # If literal_eval returns a non-str (rare), convert to str
                    s = s_eval if isinstance(s_eval, str) else str(s_eval)
                else:
                    # fallback: unescape typical escape sequences like \n, \t, \" etc.
                    # This will turn r'\"abc\"' -> '"abc"'
                    try:
                        s = bytes(s, "utf-8").decode("unicode_escape")
                    except Exception:
                        pass
            except Exception:
                # If literal_eval fails, try unicode escaping fallback, but don't raise.
                try:
                    s = bytes(s, "utf-8").decode("unicode_escape")
                except Exception:
                    pass
        
            # Remove matching surrounding quotes repeatedly (handles nested quoting)
            for _ in range(3):  # loop a few times in case of multiple nested levels
                if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
                    s = s[1:-1]
                else:
                    break
        
            # Remove leftover backslashes that are likely artifacts
            s = s.replace('\\', '')
        
            # Collapse whitespace and trim
            s = re.sub(r'\s+', ' ', s).strip()
        
            return s

        #Clean up text
        df['INPUT_QUERY'] = df['INPUT_QUERY'].apply(clean_text)
        df['AGENT_RESPONSE'] = df['AGENT_RESPONSE'].apply(clean_text)

        #Drop Duplicates
        df.drop_duplicates(subset=['AGENT_NAME', 'INPUT_QUERY'], inplace=True)
        
        #Drop any NA records
        df = df[df['INPUT_QUERY'].notna()]
        
        #Create tool selection sequence
        df['TOOL_CALLING'] = df['TOOL_ARRAY'].apply(lambda x: add_tool_sequence(ast.literal_eval(x)))
        df['EXPECTED_TOOLS'] = df.apply(lambda x: {
            'ground_truth_invocations': x['TOOL_CALLING'], 
            'ground_truth_output': x['AGENT_RESPONSE']
        }, axis=1)
        final_df = df[['RECORD_ID', 'START_TS', 'AGENT_NAME',
              'INPUT_QUERY', 'AGENT_RESPONSE', 'TOOL_CALLING','EXPECTED_TOOLS', 
               'LATENCY','USER_FEEDBACKS', 'USER_FEEDBACK_MESSAGES']]

        return final_df
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        raise

def validate_table_name(table_name: str) -> bool:
    """Validate table name format"""
    parts = table_name.strip().split('.')
    return len(parts) == 3 and all(part.strip() for part in parts)

def write_to_table(session, df: pd.DataFrame, table_name: str, overwrite: bool = False) -> bool:
    """Write DataFrame to Snowflake table"""
    try:
        target_table = table_name.upper()
        
        # Convert EXPECTED_TOOLS to JSON strings for proper VARIANT handling
        df_copy = df.copy()
        if 'EXPECTED_TOOLS' in df_copy.columns:
            df_copy['EXPECTED_TOOLS'] = df_copy['EXPECTED_TOOLS'].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
        
        # Use write_pandas without auto_create since we're managing schema ourselves
        success, nchunks, nrows, _ = session.write_pandas(
            df_copy, 
            target_table, 
            auto_create_table=False,
            overwrite=overwrite,
            quote_identifiers=False
        )
        
        return success and nrows > 0
    except Exception as e:
        st.error(f"Failed to write to table: {e}")
        return False

def validate_table_schema(session, table_name: str) -> tuple[bool, str]:
    """Validate that table has required schema (INPUT_QUERY, EXPECTED_TOOLS)"""
    try:
        target_table = table_name.upper()
        # Get table description
        # desc_result = session.sql(f"DESCRIBE TABLE {target_table}").to_pandas()
        existing_table = session.sql(f"SELECT * FROM {target_table}").to_pandas()
        st.write(existing_table.dtypes)
        
        # Check for required columns
        column_names = [col.strip('"').upper() for col in existing_table.columns.tolist()]
        st.write(column_names)
        
        if 'INPUT_QUERY' not in column_names:
            return False, "Missing required column: INPUT_QUERY"
        if 'EXPECTED_TOOLS' not in column_names:
            return False, "Missing required column: EXPECTED_TOOLS"
        
        return True, "Schema valid"
    except Exception as e:
        return False, f"Error validating schema: {str(e)}"

def load_from_table(session, table_name: str) -> pd.DataFrame:
    """Load data from Snowflake table with schema validation"""
    try:
        # Validate schema first
        is_valid, message = validate_table_schema(session, table_name)
        if not is_valid:
            st.error(f"❌ Invalid table schema: {message}")
            return pd.DataFrame()
        
        target_table = table_name.upper()
        query = f"SELECT INPUT_QUERY, EXPECTED_TOOLS FROM {target_table}"
        df = session.sql(query).to_pandas()
        
        # Verify data loaded
        if df.empty:
            st.warning("⚠️ Table exists but contains no records")
            return df
        
        st.success(f"✅ Loaded {len(df)} records from {target_table}")
        return df
        
    except Exception as e:
        st.error(f"Failed to load from table: {e}")
        return pd.DataFrame()

def create_manual_record(input_query: str, agent_response: str, tools: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a manual evaluation record in the expected format"""
    return {
        'INPUT_QUERY': input_query.strip(),
        'EXPECTED_TOOLS': {
            'ground_truth_invocations': tools,
            'ground_truth_output': agent_response.strip()
        }
    }

st.title("🔍 AI evaluation dataset builder")
st.caption("Build evaluation datasets from agent logs and manual entries")

# Add custom CSS to maximize width - more aggressive overrides
st.markdown("""
    <style>
    /* Force full width for main content */
    .main .block-container {
        max-width: 95% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    
    /* Remove tab panel padding */
    .stTabs [data-baseweb="tab-panel"] {
        padding: 0 !important;
    }
    
    /* Force dataframe to use full width */
    div[data-testid="stDataFrame"] {
        width: 100% !important;
    }
    
    div[data-testid="stDataFrame"] > div {
        width: 100% !important;
    }
    
    /* Remove padding from elements containing dataframes */
    div[data-testid="column"] {
        padding: 0 !important;
    }
    </style>
""", unsafe_allow_html=True)

session = get_snowflake_connection()

with st.sidebar:
    st.header("Configuration")
    
    if session:
        st.success("✅ Connected")
        try:
            user_info = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()").collect()[0]
            st.caption(f"User: {user_info[0]} | Role: {user_info[1]}")
        except:
            pass
    else:
        st.error("❌ Not connected")
    
    st.divider()
    
    if st.session_state.dataset is not None:
        st.metric("Records in dataset", len(st.session_state.dataset))
    else:
        st.caption("No dataset loaded yet")
    
    st.divider()
    
    if st.button("🔄 Reset dataset", help="Clear dataset and start over"):
        st.session_state.dataset = None
        st.session_state.query_executed = False
        st.rerun()
if session:
    # Create tab selection with navigation buttons at the top
    tab_names = ["📥 1. Load Data", "➕ 2. Add records", "✏️ 3. Review & edit", "📤 4. Export"]
    
    # Navigation bar with buttons and tab selector
    col_prev, col_tabs, col_next = st.columns([1, 8, 1])
    
    with col_prev:
        if st.session_state.active_tab > 0:
            if st.button("← Back", key="nav_prev", use_container_width=True):
                st.session_state.active_tab -= 1
                st.rerun()
    
    with col_tabs:
        selected_tab_name = st.radio(
            "Navigation",
            tab_names,
            index=st.session_state.active_tab,
            horizontal=True,
            label_visibility="collapsed",
            key="tab_selector"
        )
        # Update active tab based on radio selection
        st.session_state.active_tab = tab_names.index(selected_tab_name)
    
    with col_next:
        if st.session_state.active_tab < len(tab_names) - 1:
            if st.button("Next →", key="nav_next", type="primary", use_container_width=True):
                st.session_state.active_tab += 1
                st.rerun()
    
    st.divider()
    
    # Render content based on active tab
    if st.session_state.active_tab == 0:
        st.header("Load Data")
        st.caption("Load data from agent logs or existing tables")
        
        # Put data source and load mode side by side with stacked radio buttons
        col1, col2 = st.columns(2)
        
        with col1:
            data_source = st.radio(
                "Data source",
                ["From Agent Logs", "From Existing Table"],
                key="data_source_selector"
            )
        
        with col2:
            load_mode = st.radio(
                "Load mode",
                ["Replace", "Append"],
                help="Replace: Clear current dataset and load new data\nAppend: Add new records to current dataset",
                key="load_mode_global"
            )
        
        st.divider()
        
        if data_source == "From Agent Logs":
            st.subheader("📥 Load from agent observability logs")
            
            col1, col2 = st.columns([2, 1])
            with col1:
                agent_df = get_agent_list(session)
                agent_list = sorted(agent_df["name"].dropna().astype(str).tolist())
                if agent_list:
                    agent_name = st.selectbox("Select your agent name", agent_list, key="agent_select")
                    agent_db_name = agent_df["database_name"][agent_df["name"]==agent_name].values[0]
                    agent_schema_name = agent_df["schema_name"][agent_df["name"]==agent_name].values[0]
                    agent_fq_name = agent_df['"fully_qualified_agent_name"'][agent_df["name"]==agent_name].values[0]
                    
                    # Store in session state for use in other tabs
                    st.session_state.agent_fq_name = agent_fq_name
                    st.session_state.agent_db_name = agent_db_name
                    st.session_state.agent_schema_name = agent_schema_name
                else:
                    agent_name = st.text_input("Agent name", key="agent_input")
                    agent_db_name = None
                    agent_schema_name = None
                    agent_fq_name = None
            
            with col2:
                record_id = st.text_input("Record ID (optional)", value="", key="record_id_input")
            
            user_feedback = st.selectbox(
                "Filter by user feedback",
                [None, "Positive Feedback Only", "Negative Feedback Only", "Any Feedback"],
                index=0,
                key="feedback_filter"
            )
            
            if st.button("📥 Load from agent logs", type="primary", disabled=not agent_name):
                with st.spinner("Querying agent logs..."):
                    try:
                        query = build_query(
                            agent_name=agent_name,
                            agent_db_name = agent_db_name,
                            agent_schema_name = agent_schema_name,
                            record_id=record_id.strip() if record_id else None,
                            user_feedback=user_feedback
                        )
                        df = execute_query_and_postprocess(session, query)
                        
                        if load_mode == "Replace" or st.session_state.dataset is None or len(st.session_state.dataset) == 0:
                            st.session_state.dataset = df[['INPUT_QUERY', 'EXPECTED_TOOLS']].copy()
                            st.toast(f"✅ Loaded {len(df)} records (replaced existing)", icon="✅")
                        else:  # Append mode
                            new_records = df[['INPUT_QUERY', 'EXPECTED_TOOLS']].copy()
                            st.session_state.dataset = pd.concat([st.session_state.dataset, new_records], ignore_index=True)
                            st.toast(f"✅ Added {len(df)} records to dataset", icon="✅")
                        
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error loading logs: {e}")
        
        else:  # From Existing Table
            st.subheader("📊 Load from existing Snowflake table")
            
            # Clear agent info when loading from table
            st.session_state.agent_fq_name = None
            st.session_state.agent_db_name = None
            st.session_state.agent_schema_name = None
            
            st.markdown("""
            **Requirements:**
            - Table must exist in your Snowflake account
            - Required columns: `INPUT_QUERY` (VARCHAR), `EXPECTED_TOOLS` (VARIANT)
            - Use format: `DATABASE.SCHEMA.TABLE` or just `TABLE` (uses current context)
            """)
            
            table_input = st.text_input(
                "Table name",
                placeholder="e.g., MY_DATABASE.MY_SCHEMA.EVAL_DATASET or EVAL_DATASET",
                key="load_table_input",
                help="Enter fully qualified table name or just table name to use current database/schema"
            )
            
            if st.button("📊 Load from table", type="primary", disabled=not table_input):
                with st.spinner(f"Loading from {table_input}..."):
                    try:
                        loaded_df = load_from_table(session, table_input.strip())
                        
                        if not loaded_df.empty:
                            if load_mode == "Replace" or st.session_state.dataset is None:
                                st.session_state.dataset = loaded_df
                                st.toast(f"✅ Loaded {len(loaded_df)} records (replaced existing)", icon="✅")
                            else:  # Append mode
                                if st.session_state.dataset is None:
                                    st.session_state.dataset = loaded_df
                                else:
                                    st.session_state.dataset = pd.concat([st.session_state.dataset, loaded_df], ignore_index=True)
                                st.toast(f"✅ Added {len(loaded_df)} records to dataset", icon="✅")
                            st.rerun()
                        else:
                            st.warning("No records loaded")
                            
                    except Exception as e:
                        st.error(f"Error loading table: {e}")
                        st.error(f"Details: {traceback.format_exc()}")
        
        st.divider()
        
        # Data preview section - outside any columns for full width
        if st.session_state.dataset is not None and len(st.session_state.dataset) > 0:
            st.subheader("📊 Current Dataset Preview")
            st.success(f"✅ {len(st.session_state.dataset)} records loaded")
            
            # Format EXPECTED_TOOLS as readable JSON strings for display
            display_df = st.session_state.dataset.copy()
            display_df['EXPECTED_TOOLS'] = display_df['EXPECTED_TOOLS'].apply(
                lambda x: json.dumps(x, indent=2) if pd.notna(x) else ''
            )
            
            # Use container to ensure full width
            with st.container():
                st.dataframe(
                    display_df, 
                    use_container_width=True, 
                    hide_index=True, 
                    height=500,
                    column_config={
                        "INPUT_QUERY": st.column_config.TextColumn(
                            "Input Query",
                            width="medium",
                        ),
                        "EXPECTED_TOOLS": st.column_config.TextColumn(
                            "Expected Tools (JSON)",
                            width="large",
                        )
                    }
                )
        else:
            st.info("💡 No records loaded yet. Choose a data source above and load data to get started.")
    
    elif st.session_state.active_tab == 1:
        st.header("Add evaluation records")
        st.caption("Manually create evaluation records using the form below")
        
        if st.session_state.dataset is not None and len(st.session_state.dataset) > 0:
            st.success(f"✅ Current dataset: {len(st.session_state.dataset)} records")
        
        with st.form("add_record_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                input_query = st.text_area(
                    "Input query",
                    placeholder="Enter the test query for your agent...",
                    height=150,
                    key="add_input_query"
                )
            
            with col2:
                agent_response = st.text_area(
                    "Expected agent response",
                    placeholder="Enter the expected response from the agent...",
                    height=150,
                    key="add_agent_response"
                )
            
            st.divider()
            st.markdown("**Tool invocations**")
            st.caption("Define the expected sequence of tool calls")
            
            num_tools = st.number_input("Number of tools", min_value=0, max_value=10, value=1, step=1, key="add_num_tools")
            
            tools = []
            for i in range(int(num_tools)):
                st.markdown(f"**Tool {i+1}**")
                col1, col2 = st.columns([2, 3])
                
                with col1:
                    # Check if agent info is available to show dropdown, otherwise text input
                    if st.session_state.agent_fq_name:
                        try:
                            session.sql(f'DESCRIBE AGENT {st.session_state.agent_fq_name}').collect()
                            agent_desc_df = session.sql('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').to_pandas()
                            agent_tool_list = [i['tool_spec']['name'] for i in json.loads(agent_desc_df['agent_spec'][0])['tools']]

                            tool_name = st.selectbox(
                                "Tool name",
                                agent_tool_list,
                                key=f"add_tool_name_{i}",
                            )
                        except Exception:
                            # Fallback to text input if agent description fails
                            tool_name = st.text_input(
                                "Tool name",
                                key=f"add_tool_name_{i}",
                                placeholder="e.g., cortex_search, SALES_SEMANTIC_MODEL"
                            )
                    else:
                        # No agent info available, use text input
                        tool_name = st.text_input(
                            "Tool name",
                            key=f"add_tool_name_{i}",
                            placeholder="e.g., cortex_search, SALES_SEMANTIC_MODEL"
                        )
                
                with col2:
                    tool_output_type = st.selectbox(
                        "Tool output type",
                        ["SQL", "Search results", "Custom"],
                        key=f"add_tool_output_type_{i}"
                    )
                
                tool_output_value = st.text_area(
                    "Tool output",
                    key=f"add_tool_output_{i}",
                    height=80,
                    placeholder="Enter the expected tool output..."
                )
                
                if tool_name:
                    tool_output_dict = {}
                    if tool_output_type == "SQL":
                        tool_output_dict["SQL"] = tool_output_value
                    elif tool_output_type == "Search results":
                        tool_output_dict["search results"] = tool_output_value
                    else:
                        tool_output_dict["CUSTOM_TOOL_RESULT"] = tool_output_value
                    
                    tools.append({
                        "tool_sequence": i + 1,
                        "tool_name": tool_name,
                        "tool_output": tool_output_dict
                    })
                
                if i < int(num_tools) - 1:
                    st.divider()
            
            submit_button = st.form_submit_button("➕ Add record to dataset", type="primary")
        
        if submit_button:
            if not input_query or not agent_response:
                st.error("❌ Please fill in both input query and expected agent response")
            else:
                record = create_manual_record(input_query, agent_response, tools)
                new_row = pd.DataFrame([record])
                
                if st.session_state.dataset is None or len(st.session_state.dataset) == 0:
                    st.session_state.dataset = new_row
                else:
                    st.session_state.dataset = pd.concat([st.session_state.dataset, new_row], ignore_index=True)
                
                st.toast(f"✅ Record added! Total: {len(st.session_state.dataset)}", icon="✅")
                st.rerun()
        
        st.divider()
        
        if st.session_state.dataset is not None and len(st.session_state.dataset) > 0:
            st.subheader("Current dataset preview")
            # Format EXPECTED_TOOLS as readable JSON strings for display
            display_df = st.session_state.dataset.copy()
            display_df['EXPECTED_TOOLS'] = display_df['EXPECTED_TOOLS'].apply(
                lambda x: json.dumps(x, indent=2) if pd.notna(x) else ''
            )
            st.dataframe(
                display_df, 
                use_container_width=True, 
                hide_index=True, 
                height=300,
                column_config={
                    "INPUT_QUERY": st.column_config.TextColumn("Input Query", width="medium"),
                    "EXPECTED_TOOLS": st.column_config.TextColumn("Expected Tools (JSON)", width="large")
                }
            )
        else:
            st.info("No records yet. Add your first record using the form above.")
    
    elif st.session_state.active_tab == 2:
        st.header("Review & edit dataset")
        st.caption("Review your records and make final edits")
        
        if st.session_state.dataset is not None and len(st.session_state.dataset) > 0:
            st.metric("Total records", len(st.session_state.dataset))
            
            st.divider()
            
            st.subheader("Edit individual records")
            st.caption("Select a record to edit using the form below")
            
            record_options = [f"Record {i+1}: {row['INPUT_QUERY'][:60]}..." if row['INPUT_QUERY'] is not None else 'None' for i, row in st.session_state.dataset.iterrows()]

            
            record_index = st.selectbox(
                "Select record to edit",
                range(len(st.session_state.dataset)),
                format_func=lambda x: record_options[x],
                key="edit_record_selector"
            )
            
            current_record = st.session_state.dataset.iloc[record_index]
            
            # Safe extraction with null handling
            current_tools = current_record['EXPECTED_TOOLS'].get('ground_truth_invocations', []) if isinstance(current_record['EXPECTED_TOOLS'], dict) else []
            current_response = current_record['EXPECTED_TOOLS'].get('ground_truth_output', '') if isinstance(current_record['EXPECTED_TOOLS'], dict) else ''
            current_query = str(current_record['INPUT_QUERY']) if pd.notna(current_record['INPUT_QUERY']) else ''
            
            with st.form(f"edit_form_{record_index}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    edited_query = st.text_area(
                        "Input query",
                        value=current_query,
                        height=150,
                        key=f"edit_query_{record_index}"
                    )
                
                with col2:
                    edited_response = st.text_area(
                        "Expected agent response",
                        value=str(current_response) if pd.notna(current_response) else '',
                        height=150,
                        key=f"edit_response_{record_index}"
                    )
                
                st.divider()
                st.markdown("**Tool invocations**")
                
                num_tools_edit = st.number_input(
                    "Number of tools",
                    min_value=0,
                    max_value=10,
                    value=len(current_tools) if current_tools else 0,
                    step=1,
                    key=f"num_tools_edit_{record_index}"
                )
                
                edited_tools = []
                for i in range(int(num_tools_edit)):
                    st.markdown(f"**Tool {i+1}**")
                    
                    if i < len(current_tools) and current_tools[i]:
                        tool_data = current_tools[i]
                        default_name = tool_data.get('tool_name', '') if isinstance(tool_data, dict) else ''
                        default_output = tool_data.get('tool_output', {}) if isinstance(tool_data, dict) else {}
                        
                        if isinstance(default_output, dict):
                            if 'SQL' in default_output:
                                default_type = "SQL"
                                default_value = str(default_output.get('SQL', ''))
                            elif 'search results' in default_output:
                                default_type = "Search results"
                                default_value = str(default_output.get('search results', ''))
                            else:
                                default_type = "Custom"
                                default_value = str(default_output.get('CUSTOM_TOOL_RESULT', ''))
                        else:
                            default_type = "SQL"
                            default_value = ''
                    else:
                        default_name = ''
                        default_type = "SQL"
                        default_value = ''
                    
                    col1, col2 = st.columns([2, 3])
                    
                    with col1:
                        tool_name_edit = st.text_input(
                            "Tool name",
                            value=default_name,
                            key=f"edit_tool_name_{record_index}_{i}"
                        )
                    
                    with col2:
                        tool_type_index = ["SQL", "Search results", "Custom"].index(default_type) if default_type in ["SQL", "Search results", "Custom"] else 0
                        tool_output_type_edit = st.selectbox(
                            "Tool output type",
                            ["SQL", "Search results", "Custom"],
                            index=tool_type_index,
                            key=f"edit_tool_type_{record_index}_{i}"
                        )
                    
                    tool_output_value_edit = st.text_area(
                        "Tool output",
                        value=default_value,
                        height=80,
                        key=f"edit_tool_output_{record_index}_{i}"
                    )
                    
                    if tool_name_edit:
                        tool_output_dict = {}
                        if tool_output_type_edit == "SQL":
                            tool_output_dict["SQL"] = tool_output_value_edit
                        elif tool_output_type_edit == "Search results":
                            tool_output_dict["search results"] = tool_output_value_edit
                        else:
                            tool_output_dict["CUSTOM_TOOL_RESULT"] = tool_output_value_edit
                        
                        edited_tools.append({
                            "tool_sequence": i + 1,
                            "tool_name": tool_name_edit,
                            "tool_output": tool_output_dict
                        })
                    
                    if i < int(num_tools_edit) - 1:
                        st.divider()
                
                col1, col2 = st.columns(2)
                with col1:
                    save_button = st.form_submit_button("💾 Save changes", type="primary")
                with col2:
                    delete_button = st.form_submit_button("🗑️ Delete record", type="secondary")
            
            if save_button:
                updated_record = create_manual_record(edited_query, edited_response, edited_tools)
                st.session_state.dataset.at[record_index, 'INPUT_QUERY'] = updated_record['INPUT_QUERY']
                st.session_state.dataset.at[record_index, 'EXPECTED_TOOLS'] = updated_record['EXPECTED_TOOLS']
                st.toast("✅ Record updated!", icon="✅")
                st.rerun()
            
            if delete_button:
                st.session_state.dataset = st.session_state.dataset.drop(record_index).reset_index(drop=True)
                st.toast("🗑️ Record deleted", icon="🗑️")
                st.rerun()
            
            st.divider()
            st.subheader("All records")
            # Format EXPECTED_TOOLS as readable JSON strings for display
            display_df = st.session_state.dataset.copy()
            display_df['EXPECTED_TOOLS'] = display_df['EXPECTED_TOOLS'].apply(
                lambda x: json.dumps(x, indent=2) if pd.notna(x) else ''
            )
            st.dataframe(
                display_df, 
                use_container_width=True, 
                hide_index=True, 
                height=300,
                column_config={
                    "INPUT_QUERY": st.column_config.TextColumn("Input Query", width="medium"),
                    "EXPECTED_TOOLS": st.column_config.TextColumn("Expected Tools (JSON)", width="large")
                }
            )
        else:
            st.warning("No records in dataset. Go to 'Load logs' or 'Add records' tab to get started.")
    
    elif st.session_state.active_tab == 3:
        st.header("Export dataset")
        st.caption("Export your evaluation dataset to Snowflake or download as CSV")
        
        if st.session_state.dataset is not None and len(st.session_state.dataset) > 0:
            st.success(f"✅ Dataset ready with {len(st.session_state.dataset)} records")
            
            st.subheader("Dataset preview")
            # Format EXPECTED_TOOLS as readable JSON strings for display
            display_df = st.session_state.dataset.copy()
            display_df['EXPECTED_TOOLS'] = display_df['EXPECTED_TOOLS'].apply(
                lambda x: json.dumps(x, indent=2) if pd.notna(x) else ''
            )
            st.dataframe(
                display_df, 
                use_container_width=True, 
                hide_index=True, 
                height=300,
                column_config={
                    "INPUT_QUERY": st.column_config.TextColumn("Input Query", width="medium"),
                    "EXPECTED_TOOLS": st.column_config.TextColumn("Expected Tools (JSON)", width="large")
                }
            )
            
            st.divider()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("💾 Save to Snowflake")
                table_name = st.text_input(
                    "Table name",
                    value="AGENT_EVAL_DB.PUBLIC.EVAL_DATASET",
                    placeholder="DATABASE.SCHEMA.TABLE_NAME",
                    key="export_table_name"
                )
                
                save_mode = st.radio("Save mode", ["Append", "Overwrite"], horizontal=True, key="export_save_mode")
                
                if st.button("📤 Save to Snowflake", type="primary"):
                    if not table_name.strip():
                        st.warning("⚠️ Please enter a table name")
                    elif not validate_table_name(table_name):
                        st.error("❌ Invalid table name format. Use DATABASE.SCHEMA.TABLE")
                    else:
                        with st.spinner("Saving to Snowflake..."):
                            try:
                                overwrite = (save_mode == "Overwrite")
                                table_upper = table_name.strip().upper()
                                
                                # Create or replace table with proper schema
                                if overwrite:
                                    session.sql(f"CREATE OR REPLACE TABLE {table_upper} (INPUT_QUERY VARCHAR, EXPECTED_TOOLS VARIANT);").collect()
                                else:
                                    session.sql(f"CREATE TABLE IF NOT EXISTS {table_upper} (INPUT_QUERY VARCHAR, EXPECTED_TOOLS VARIANT);").collect()
                                
                                # Prepare data - convert to proper format
                                records_to_insert = []
                                for _, row in st.session_state.dataset.iterrows():
                                    query_val = str(row['INPUT_QUERY']) if pd.notna(row['INPUT_QUERY']) else ''
                                    tools_dict = row['EXPECTED_TOOLS'] if pd.notna(row['EXPECTED_TOOLS']) else {}
                                    records_to_insert.append((query_val, json.dumps(tools_dict)))
                                
                                # Create temp dataframe with properly formatted data
                                temp_df = pd.DataFrame(records_to_insert, columns=['INPUT_QUERY', 'EXPECTED_TOOLS_JSON'])
                                
                                # Write to temp staging table using write_pandas
                                temp_table = f"{table_upper}_TEMP_STAGING"
                                session.sql(f"CREATE OR REPLACE TEMP TABLE {temp_table} (INPUT_QUERY VARCHAR, EXPECTED_TOOLS_JSON VARCHAR);").collect()
                                
                                # Use write_pandas for temp table (handles escaping properly)
                                # Snowpark Session.write_pandas returns (success: bool, nrows: int)
                                write_result = session.write_pandas(
                                    temp_df,
                                    temp_table,
                                    auto_create_table=False,
                                    quote_identifiers=False
                                )
                                
                                # Handle return value - could be tuple of 2 or 4 values depending on version
                                if isinstance(write_result, tuple):
                                    if len(write_result) == 2:
                                        success, nrows = write_result
                                    elif len(write_result) == 4:
                                        success, nchunks, nrows, output = write_result
                                    else:
                                        success = write_result[0]
                                        nrows = len(temp_df)
                                else:
                                    success = write_result
                                    nrows = len(temp_df)
                                
                                if success:
                                    # Copy from temp to final table with PARSE_JSON
                                    insert_sql = f"""
                                    INSERT INTO {table_upper} (INPUT_QUERY, EXPECTED_TOOLS)
                                    SELECT INPUT_QUERY, PARSE_JSON(EXPECTED_TOOLS_JSON)
                                    FROM {temp_table}
                                    """
                                    session.sql(insert_sql).collect()
                                    
                                    # Verify records were inserted
                                    count_result = session.sql(f"SELECT COUNT(*) as cnt FROM {table_upper}").collect()
                                    actual_count = count_result[0]['CNT']
                                    
                                    st.toast(f"✅ Saved {nrows} records to {table_name} (Total in table: {actual_count})", icon="✅")
                                else:
                                    st.error("❌ Failed to write records to staging table")
                                
                            except Exception as e:
                                st.error(f"Error: {e}")
                                st.error(traceback.format_exc())
            
            with col2:
                st.subheader("📥 Download CSV")
                st.caption("Download the dataset as a CSV file for local use")
                
                csv = st.session_state.dataset.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"eval_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    type="primary"
                )
        else:
            st.warning("No records in dataset. Go to 'Load logs' or 'Add records' tab to build your dataset.")

else:
    st.info("👈 Please connect to Snowflake using the sidebar")
    
    with st.expander("📝 Connection requirements"):
        st.markdown("""
        Required environment variables:
        - `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
        - `SNOWFLAKE_USER` - Your Snowflake username
        - `SNOWFLAKE_PASSWORD` - Your Snowflake password
        
        Optional environment variables (with defaults):
        - `SNOWFLAKE_WAREHOUSE` (default: COMPUTE_WH)
        - `SNOWFLAKE_DATABASE` (default: SNOWFLAKE)
        - `SNOWFLAKE_SCHEMA` (default: LOCAL)
        - `SNOWFLAKE_ROLE` (default: ACCOUNTADMIN)
        """)

st.divider()
st.caption("AI evaluation dataset builder | Powered by Snowflake")

