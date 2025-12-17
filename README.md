# Cortex Agent Evaluations Quickstart (PrPr)

A practical toolkit for building and testing evaluation datasets with Snowflake's Cortex Agent Evaluations (Private Preview).

## What's included

- **Evaluation Dataset Builder**: Streamlit app for creating agent evaluation datasets
- **Setup Scripts**: SQL to configure sample agents and tables

## Setup

### 1. Run the setup script to generate sample agents

In Snowsight or your SQL client run contents of **setup.sql** to create
  - MARKETING_CAMPAIGNS_DB
  - Several tables of marketing data
  - Semantic View for Cortex Analyst Service on campaign performance and feedback data
  - Cortex Search Service on campaign content
  - Stored Procedure for custom agent tool
  - Cortex Agents  with above services configured

### 2. Navigate to your newly created agent and click into Evaluations Tab

- Name your new evaluation run and optionally give a description [click next]
- Select Create New Dataset
  - Select MARKETING_CAMPAIGNS_DB.PUBLIC.EVALS_TABLE as your input table
  - Select MARKETING_CAMPAIGNS_DB.PUBLIC.QUICKSTART_EVALSET as your new dataset destination [click next] 
- Select INPUT_QUERY as your Query Text column
  - Check boxes for all metrics available
    - Tool Selection Accuracy, Tool Execution Accuracy, and Answer Correctness should reference the EXPECTED_TOOLS column
  - Click Create Evaluation

Now wait as your queries are executed and your evaluation metrics are computed! This should populate in roughly ~3-5 minutes. 


### 3. Repeat the process with the improved agent and compare results

- Follow the same steps as Step 2. (note this time you can just reuse the dataset you created in step 2)
- Investigate cases where added orchestration and response instructions led to stronger agent performance and higher evaluation metrics!

### 4. Optional - Try out with your Agents! 

- Launch the streamlit app <pointer> and select one of your existing agents
- Use the app to either load in a table of input queries and ground truth data - or generate from existing log data. Make appropriate edits and save to a table.
  - See more details on this in the **Using the Evalset Generator App** section below!
- Repeat the process described in step 2 to run an evaluation on your agent with your newly created 

### 4b. Run app in local streamlit

First ensure all packages are installed

```bash
pip install -r requirements.txt
```

Then create a .env file with the following data

SNOWFLAKE_ACCOUNT=<ACCOUNT_LOCATOR.ACCOUNT_REGION> ## ie. ABC1234.us-west-2
SNOWFLAKE_USER=<USERNAME>
SNOWFLAKE_USER_PASSWORD=<PASSWORD>

Launch your streamlit app!

```bash
streamlit run agent_observability_app.py
```

## **Using the Evalset Generator App**

The app helps you build evaluation datasets by:

1. **Loading data** from agent logs or existing tables
2. **Adding records** manually for edge cases
3. **Editing** to refine queries and expected outputs
4. **Exporting** to Snowflake tables

Datasets follow this schema:
```sql
INPUT_QUERY VARCHAR
EXPECTED_TOOLS VARIANT  -- {ground_truth_invocations: [...], ground_truth_output: "..."}
```

## Requirements

- Snowflake account with Cortex Agent Evaluations enabled (Private Preview)
- Python 3.8+
- Access to agent observability events

## Docs

Cortex Agent Evaluations: https://docs.snowflake.com/LIMITEDACCESS/cortex-agent-evaluations
