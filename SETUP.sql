-- ====================================================================
-- MARKETING CAMPAIGNS ANALYTICS SYSTEM - COMPLETE SETUP SCRIPT
-- ====================================================================
-- This script creates a complete marketing campaigns analytics system with:
-- - Database and tables with sample data
-- - Semantic view for Cortex Analyst
-- - Cortex Search service for content discovery
-- - Stored procedure for report generation
-- - Cortex Agent integrating all tools
--
-- Prerequisites:
-- - ACCOUNTADMIN role (or role with CREATE DATABASE privileges)
-- - Access to COMPUTE_WH warehouse (or modify warehouse name below)
-- - SNOWFLAKE.CORTEX_USER database role granted
--
-- Estimated runtime: 5-10 minutes
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 0: CREATE GIT INTEGRATION (for loading CSV files from repo)
-- ============================================================================


-- ====================================================================
-- SECTION 1: DATABASE AND TABLE CREATION
-- ====================================================================


-- Create database using variable
CREATE DATABASE IF NOT EXISTS MARKETING_CAMPAIGNS_DB;
CREATE SCHEMA IF NOT EXISTS MARKETING_CAMPAIGNS_DB.AGENTS;
USE SCHEMA MARKETING_CAMPAIGNS_DB.AGENTS;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WAREHOUSE_SIZE='SMALL';

-- ============================================================================
-- SECTION 2: CREATE GIT INTEGRATION (for loading CSV files from repo)
-- ============================================================================

-- Create API integration for GitHub (public repo, no secrets needed)
CREATE API INTEGRATION IF NOT EXISTS GITHUB_API_INTEGRATION_AGENT_EVAL_QUICKSTART
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-ebotwick/')
    ENABLED = TRUE;

-- Clone the GitHub repository
CREATE OR REPLACE GIT REPOSITORY CORTEX_AGENT_QUICKSTART_REPO
    API_INTEGRATION = GITHUB_API_INTEGRATION_AGENT_EVAL_QUICKSTART
    ORIGIN = 'https://github.com/sfc-gh-ebotwick/CORTEX_AGENT_EVALS_QUICKSTART.git';

-- Fetch latest from GitHub
ALTER GIT REPOSITORY CORTEX_AGENT_QUICKSTART_REPO FETCH;

-- Verify repository
SHOW GIT BRANCHES IN CORTEX_AGENT_QUICKSTART_REPO;
LS @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data;


-- ============================================================================
-- SECTION 3: CREATE AND POPULATE TABLES
-- ============================================================================

-- First create a file format to use when reading data from github
CREATE OR REPLACE FILE FORMAT AGENT_EVAL_QUICKSTART_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'AUTO';


-- ============================================================================
-- CAMPAIGNS
-- ============================================================================
-- Create CAMPAIGNS table
CREATE OR REPLACE TABLE CAMPAIGNS (
    campaign_id INT,
    campaign_name VARCHAR(200) NOT NULL,
    campaign_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget_allocated DECIMAL(12,2),
    target_audience VARCHAR(200),
    channel VARCHAR(50),
    status VARCHAR(50),
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Populate CAMPAIGNS table
INSERT INTO CAMPAIGNS (campaign_id, campaign_name, campaign_type, start_date, end_date, budget_allocated, target_audience, channel, status, created_by)
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
FROM @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data/CAMPAIGNS.csv (FILE_FORMAT=>AGENT_EVAL_QUICKSTART_CSV_FORMAT);

-- ============================================================================
-- CAMPAIGN_PERFORMANCE
-- ============================================================================
-- Create CAMPAIGN_PERFORMANCE table
CREATE OR REPLACE TABLE CAMPAIGN_PERFORMANCE (
    performance_id INT,
    campaign_id INT,
    date DATE,
    impressions INT,
    clicks INT,
    conversions INT,
    cost_per_click DECIMAL(10,4),
    cost_per_acquisition DECIMAL(10,2),
    revenue_generated DECIMAL(12,2),
    roi_percentage DECIMAL(8,2),
    engagement_rate DECIMAL(8,4)
    -- FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- Populate CAMPAIGN_PERFORMANCE table
INSERT INTO CAMPAIGN_PERFORMANCE (performance_id, campaign_id, date, impressions, clicks, conversions, cost_per_click, cost_per_acquisition, revenue_generated, roi_percentage, engagement_rate) 
SELECT $1, $2,$3,$4,$5, $6, $7, $8, $9, $10, $11
FROM @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data/CAMPAIGN_PERFORMANCE.csv (FILE_FORMAT=>AGENT_EVAL_QUICKSTART_CSV_FORMAT);

-- ============================================================================
--CAMPAIGN_CONTENT
-- ============================================================================
-- Create CAMPAIGN_CONTENT table
CREATE OR REPLACE TABLE CAMPAIGN_CONTENT (
    campaign_id INT,
    content_type VARCHAR(100),
    campaign_description TEXT,
    marketing_copy TEXT,
    a_b_test_notes TEXT
    -- FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- Populate CAMPAIGN_CONTENT table
INSERT INTO CAMPAIGN_CONTENT (campaign_id, content_type, campaign_description, marketing_copy, a_b_test_notes)
SELECT $1, $2,$3,$4,$5 
FROM @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data/CAMPAIGN_CONTENT.csv (FILE_FORMAT=>AGENT_EVAL_QUICKSTART_CSV_FORMAT);

-- ============================================================================
--CAMPAIGN_FEEDBACK
-- ============================================================================
-- Create CAMPAIGN_FEEDBACK table
CREATE OR REPLACE TABLE CAMPAIGN_FEEDBACK (
    feedback_id INT,
    campaign_id INT,
    feedback_date DATE,
    customer_segment VARCHAR(100),
    satisfaction_score DECIMAL(3,2),
    detailed_comments TEXT,
    survey_responses TEXT,
    recommended_improvements TEXT
    -- FOREIGN KEY (campaign_id) REFERENCES CAMPAIGNS(campaign_id)
);

-- Populate CAMPAIGN_FEEDBACK table
INSERT INTO CAMPAIGN_FEEDBACK (feedback_id, campaign_id, feedback_date, customer_segment, satisfaction_score, detailed_comments, survey_responses, recommended_improvements)
SELECT $1, $2,$3,$4,$5, $6, $7, $8
FROM @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data/CAMPAIGN_FEEDBACK.csv (FILE_FORMAT=>AGENT_EVAL_QUICKSTART_CSV_FORMAT);

-- ============================================================================
--EVALS_TABLE
-- ============================================================================
-- Create EVALS_TABLE table
CREATE OR REPLACE TABLE EVALS_TABLE (
    INPUT_QUERY TEXT,
    EXPECTED_TOOLS VARCHAR);

-- Populate EVALS_TABLE table
INSERT INTO EVALS_TABLE (input_query, expected_tools)
SELECT $1, $2
FROM @CORTEX_AGENT_QUICKSTART_REPO/branches/main/data/EVALS_TABLE.csv (FILE_FORMAT=>AGENT_EVAL_QUICKSTART_CSV_FORMAT);

CREATE OR REPLACE TABLE EVALS_TABLE
AS SELECT INPUT_QUERY, PARSE_JSON(EXPECTED_TOOLS) AS GROUND_TRUTH_DATA
FROM EVALS_TABLE;


-- ====================================================================
-- SECTION 3: VALIDATE DATA;
-- ====================================================================

SELECT * FROM CAMPAIGNS;

SELECT * FROM CAMPAIGN_CONTENT;

SELECT * FROM CAMPAIGN_FEEDBACK;

SELECT * FROM CAMPAIGN_PERFORMANCE;

SELECT * FROM EVALS_TABLE;

-- ====================================================================
-- SECTION 4: CREATE SEMANTIC VIEW
-- ====================================================================

CREATE OR REPLACE SEMANTIC VIEW MARKETING_PERFORMANCE_ANALYST
  TABLES (
    campaigns AS CAMPAIGNS PRIMARY KEY (campaign_id),
    performance AS CAMPAIGN_PERFORMANCE PRIMARY KEY (performance_id)
  )
  RELATIONSHIPS (
    performance(campaign_id) REFERENCES campaigns(campaign_id)
  )
  DIMENSIONS (
    PUBLIC campaigns.campaign_id AS campaign_id,
    PUBLIC campaigns.campaign_name AS campaign_name,
    PUBLIC campaigns.campaign_type AS campaign_type,
    PUBLIC campaigns.channel AS channel,
    PUBLIC campaigns.target_audience AS target_audience,
    PUBLIC campaigns.status AS status,
    PUBLIC campaigns.start_date AS start_date,
    PUBLIC campaigns.end_date AS end_date,
    PUBLIC campaigns.created_by AS created_by,
    PUBLIC performance.date AS date
  )
  METRICS (
    PUBLIC performance.total_revenue AS SUM(revenue_generated),
    PUBLIC performance.total_impressions AS SUM(impressions),
    PUBLIC performance.total_clicks AS SUM(clicks),
    PUBLIC performance.total_conversions AS SUM(conversions),
    PUBLIC performance.avg_cost_per_click AS AVG(cost_per_click),
    PUBLIC performance.avg_cost_per_acquisition AS AVG(cost_per_acquisition),
    PUBLIC performance.avg_roi AS AVG(roi_percentage),
    PUBLIC performance.avg_engagement_rate AS AVG(engagement_rate),
    PUBLIC campaigns.total_budget AS SUM(budget_allocated),
    PUBLIC campaigns.campaign_count AS COUNT(campaign_id)
  )
  COMMENT = 'Semantic view for analyzing marketing campaign performance and ROI';

-- Verify semantic view was created
SHOW SEMANTIC VIEWS LIKE 'MARKETING_PERFORMANCE_ANALYST';

-- ====================================================================
-- SECTION 7: CREATE CORTEX SEARCH SERVICE
-- ====================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE MARKETING_CAMPAIGNS_SEARCH
  ON combined_text
  ATTRIBUTES campaign_name, campaign_type, channel, content_type
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT 
      c.campaign_id,
      c.campaign_name,
      c.campaign_type,
      c.channel,
      cnt.content_type,
      CONCAT(
        'Campaign: ', c.campaign_name, '. ',
        'Type: ', c.campaign_type, '. ',
        'Channel: ', c.channel, '. ',
        'Description: ', cnt.campaign_description, '. ',
        'Marketing Copy: ', cnt.marketing_copy, '. ',
        'A/B Test Notes: ', cnt.a_b_test_notes
      ) as combined_text
    FROM CAMPAIGNS c
    JOIN CAMPAIGN_CONTENT cnt ON c.campaign_id = cnt.campaign_id
    
    UNION ALL
    
    SELECT 
      c.campaign_id,
      c.campaign_name,
      c.campaign_type,
      c.channel,
      'feedback' as content_type,
      CONCAT(
        'Campaign: ', c.campaign_name, '. ',
        'Customer Segment: ', fb.customer_segment, '. ',
        'Satisfaction Score: ', fb.satisfaction_score, '. ',
        'Comments: ', fb.detailed_comments, '. ',
        'Improvements: ', fb.recommended_improvements
      ) as combined_text
    FROM CAMPAIGNS c
    JOIN CAMPAIGN_FEEDBACK fb ON c.campaign_id = fb.campaign_id
  );

-- Verify search service was created
SHOW CORTEX SEARCH SERVICES LIKE 'MARKETING_CAMPAIGNS_SEARCH';

-- ====================================================================
-- SECTION 8: CREATE REPORT GENERATION STORED PROCEDURE
-- ====================================================================

-- Create an internal stage with directory table enabled
CREATE OR REPLACE STAGE CAMPAIGN_REPORTS
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage to host generated campaign reports';

CREATE OR REPLACE PROCEDURE GENERATE_CAMPAIGN_REPORT_HTML(campaign_id NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER                                                                                                                         
AS
$$
DECLARE
  report_html VARCHAR;
  campaign_info VARCHAR;
  performance_metrics VARCHAR;
  feedback_summary VARCHAR;
  file_name VARCHAR;
  upload_result VARCHAR;
  org_name VARCHAR;
  account_name VARCHAR;
BEGIN
  -- Get campaign basic information
  SELECT 
    '<h1>Campaign Report</h1>' ||
    '<h2>Campaign: ' || campaign_name || '</h2>' ||
    '<p><strong>Type:</strong> ' || campaign_type || '</p>' ||
    '<p><strong>Channel:</strong> ' || channel || '</p>' ||
    '<p><strong>Duration:</strong> ' || start_date || ' to ' || end_date || '</p>' ||
    '<p><strong>Budget:</strong> $' || budget_allocated || '</p>' ||
    '<p><strong>Target Audience:</strong> ' || target_audience || '</p>' ||
    '<p><strong>Status:</strong> ' || status || '</p>'
  INTO campaign_info
  FROM CAMPAIGNS
  WHERE campaign_id = :campaign_id;
  
  -- Get performance metrics summary
  SELECT 
    '<h3>Performance Metrics</h3>' ||
    '<table border="1" style="border-collapse:collapse; width:100%">' ||
    '<tr><th>Metric</th><th>Value</th></tr>' ||
    '<tr><td>Total Impressions</td><td>' || TO_CHAR(SUM(impressions), '999,999,999') || '</td></tr>' ||
    '<tr><td>Total Clicks</td><td>' || TO_CHAR(SUM(clicks), '999,999,999') || '</td></tr>' ||
    '<tr><td>Total Conversions</td><td>' || TO_CHAR(SUM(conversions), '999,999') || '</td></tr>' ||
    '<tr><td>Click-Through Rate</td><td>' || ROUND((SUM(clicks)::FLOAT / SUM(impressions)::FLOAT) * 100, 2) || '%</td></tr>' ||
    '<tr><td>Conversion Rate</td><td>' || ROUND((SUM(conversions)::FLOAT / SUM(clicks)::FLOAT) * 100, 2) || '%</td></tr>' ||
    '<tr><td>Average Cost Per Click</td><td>$' || ROUND(AVG(cost_per_click), 2) || '</td></tr>' ||
    '<tr><td>Average Cost Per Acquisition</td><td>$' || ROUND(AVG(cost_per_acquisition), 2) || '</td></tr>' ||
    '<tr><td>Total Revenue Generated</td><td>$' || TO_CHAR(SUM(revenue_generated), '999,999,999.99') || '</td></tr>' ||
    '<tr><td>Average ROI</td><td>' || ROUND(AVG(roi_percentage), 2) || '%</td></tr>' ||
    '<tr><td>Average Engagement Rate</td><td>' || ROUND(AVG(engagement_rate) * 100, 2) || '%</td></tr>' ||
    '</table>'
  INTO performance_metrics
  FROM CAMPAIGN_PERFORMANCE
  WHERE campaign_id = :campaign_id;
  
  -- Get feedback summary
  SELECT 
    '<h3>Customer Feedback Summary</h3>' ||
    '<p><strong>Average Satisfaction Score:</strong> ' || ROUND(AVG(satisfaction_score), 2) || ' / 5.0</p>' ||
    '<p><strong>Number of Feedback Entries:</strong> ' || COUNT(*) || '</p>' ||
    '<h4>Recent Feedback:</h4>' ||
    LISTAGG(
      '<div style="border:1px solid #ccc; padding:10px; margin:10px 0;">' ||
      '<p><strong>Segment:</strong> ' || customer_segment || '</p>' ||
      '<p><strong>Score:</strong> ' || satisfaction_score || ' / 5.0</p>' ||
      '<p><strong>Comments:</strong> ' || detailed_comments || '</p>' ||
      '<p><strong>Recommendations:</strong> ' || recommended_improvements || '</p>' ||
      '</div>',
      ''
    ) WITHIN GROUP (ORDER BY feedback_date DESC)
  INTO feedback_summary
  FROM CAMPAIGN_FEEDBACK
  WHERE campaign_id = :campaign_id;
  
  -- Combine all sections
  report_html := '<!DOCTYPE html><html><head><style>' ||
    'body { font-family: Arial, sans-serif; margin: 20px; }' ||
    'table { margin: 20px 0; }' ||
    'th { background-color: #4CAF50; color: white; padding: 10px; text-align: left; }' ||
    'td { padding: 10px; }' ||
    'tr:nth-child(even) { background-color: #f2f2f2; }' ||
    '</style></head><body>' ||
    campaign_info ||
    performance_metrics ||
    COALESCE(feedback_summary, '<p>No feedback available</p>') ||
    '<hr><p style="text-align:center; color:#666;">Report Generated: ' || CURRENT_TIMESTAMP() || '</p>' ||
    '</body></html>';
  
  -- Generate filename with timestamp
  file_name := 'CAMPAIGN_' || campaign_id || '_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD_HH_MI') || '.html';
  
  -- Create a file format for HTML content
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE FILE FORMAT html_format
    TYPE = ''CSV''
    FIELD_DELIMITER = NONE
    RECORD_DELIMITER = NONE
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    ESCAPE_UNENCLOSED_FIELD = NONE
    COMPRESSION = NONE
    ENCODING = ''UTF8''
  ';
  
  -- Create temporary table to hold the HTML content
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_report_' || campaign_id || ' (html_content VARCHAR(16777216))';
  
  -- Insert the HTML content
  EXECUTE IMMEDIATE 'INSERT INTO temp_report_' || campaign_id || ' VALUES (?)' USING (report_html);
  
  -- Copy the file to the stage using the HTML file format
  EXECUTE IMMEDIATE 
    'COPY INTO @CAMPAIGN_REPORTS/' || file_name || 
    ' FROM (SELECT html_content FROM temp_report_' || campaign_id || ') ' ||
    'FILE_FORMAT = html_format ' ||
    'SINGLE = TRUE OVERWRITE = TRUE HEADER = FALSE';
  
  -- Clean up temporary table
  EXECUTE IMMEDIATE 'DROP TABLE temp_report_' || campaign_id;

SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME() 
  INTO ORG_NAME, ACCOUNT_NAME;
  
  upload_result := 'Report '|| file_name || ' generated and uploaded to stage. View here - https://app.snowflake.com/'|| ORG_NAME ||'/' || ACCOUNT_NAME ||'/#/data/databases/MARKETING_CAMPAIGNS_DB/schemas/TEST/stage/CAMPAIGN_REPORTS';

  
  RETURN upload_result;
END;
$$;

-- Verify procedure was created

SHOW PROCEDURES like 'GENERATE_CAMPAIGN_REPORT_HTML';
-- ====================================================================
-- SECTION 9: TEST NEWLY CREATED SERVICES
-- ====================================================================

-- Simple campaign performance summary
-- Campaign performance by type


-- Test semantic view
SELECT 
    campaign_type,
    campaign_count,
    total_budget,
    total_revenue,
    avg_roi
FROM SEMANTIC_VIEW(
    MARKETING_PERFORMANCE_ANALYST
    DIMENSIONS campaign_type
    METRICS campaign_count, total_budget, total_revenue, avg_roi
);

-- Test search service
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MARKETING_CAMPAIGNS_SEARCH',
        '{"query": "email campaigns", "columns": ["campaign_name", "campaign_type", "combined_text"], "limit": 3}'
    )
) as search_results;

-- Test stored procedure
CALL GENERATE_CAMPAIGN_REPORT_HTML(1);

LS @CAMPAIGN_REPORTS;

-- ====================================================================
-- SECTION 10: CREATE CORTEX AGENTS
-- ====================================================================


CREATE OR REPLACE AGENT MARKETING_CAMPAIGN_AGENT
WITH PROFILE='{ "display_name": "MARKETING_CAMPAIGN_AGENT" }'
    COMMENT=$$ Agent specializing in analyzing marketing campaigns for performance, ROI, feedbakc, etc. $$
FROM SPECIFICATION $$
{
    "models": {"orchestration": "auto"},
    "instructions": {
        "orchestration": "",
        "response": "",
        "sample_questions": [
      {
        "question": "What campaigns have the highest ROI?"
      }
    ]
    },
    "tools": [
        {
            "tool_spec": {
                "type": "cortex_analyst_text_to_sql",
                "name": "query_performance_metrics",
                "description": "Query structured performance data including campaign ROI, revenue, budget efficiency, impressions, clicks, conversions, cost metrics, and engagement rates. Use for quantitative analysis of campaign performance across channels and time periods."
            }
        },
        {
            "tool_spec": {
                "type": "cortex_search",
                "name": "search_campaign_content",
                "description": "Search unstructured campaign content including campaign descriptions, marketing copy, A/B test results, customer feedback, and recommended improvements. Use for qualitative insights, content discovery, and learning from past campaigns."
            }
        },
        {
            "tool_spec": {
                "type": "generic",
                "name": "generate_campaign_report",
                "description": "Generate a comprehensive HTML report for a specific campaign including all performance metrics, customer feedback, and key insights. Returns formatted report ready for PDF conversion.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "campaign_id": {
                            "type": "integer",
                            "description": "The unique identifier of the campaign to generate a report for"
                        }
                    },
                    "required": ["campaign_id"]
                }
            }
        }
    ],
    "tool_resources": {
        "query_performance_metrics": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "semantic_view": "MARKETING_CAMPAIGNS_DB.AGENTS.MARKETING_PERFORMANCE_ANALYST"
        },
        "search_campaign_content": {
            "execution_environment": {
                "query_timeout": 299,
                "type": "warehouse",
                "warehouse": "COMPUTE_WH"
            },
            "search_service": "MARKETING_CAMPAIGNS_DB.AGENTS.MARKETING_CAMPAIGNS_SEARCH"
        },
        "generate_campaign_report": {
            "type": "procedure",
            "identifier": "MARKETING_CAMPAIGNS_DB.AGENTS.GENERATE_CAMPAIGN_REPORT_HTML",
            "execution_environment": {
                "type": "warehouse",
                "warehouse": "COMPUTE_WH",
                "query_timeout": 300
            }
        }
    }
}

$$;

DESCRIBE AGENT MARKETING_CAMPAIGN_AGENT;


-- CREATE OR REPLACE AGENT MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_CAMPAIGN_AGENT_OPTIMIZED
-- WITH PROFILE='{ "display_name": "MARKETING_CAMPAIGN_AGENT_OPTIMIZED" }'
--     COMMENT=$$ Agent specializing in analyzing marketing campaigns for performance, ROI, feedback, etc. $$;
-- FROM SPECIFICATION $$
-- {
--     "models": {"orchestration": "auto"},
--     "instructions": {
-- "orchestration": "You are a marketing campaigns analytics agent with three specialized tools. Follow these STRICT routing rules to ensure consistent tool selection:

-- ## TOOL ROUTING RULES (Apply in order)


-- ### Rule 1: Quantitative Analysis (Use query_performance_metrics)
-- Use query_performance_metrics when the query involves:
-- - NUMERICAL METRICS: revenue, ROI, conversions, clicks, impressions, costs, budget, engagement rates
-- - CALCULATIONS: totals, averages, percentages, ratios, growth rates, trends over time
-- - COMPARISONS: top/bottom campaigns, ranking, channel comparison, time period analysis
-- - AGGREGATIONS: sum, count, average, min, max by dimensions like channel, type, audience
-- - PERFORMANCE QUESTIONS: 'how much', 'how many', 'what is the rate', 'calculate'
-- - Keywords: 'revenue', 'ROI', 'cost', 'conversions', 'clicks', 'performance', 'metrics', 'total', 'average', 'rate', 'top', 'bottom', 'best', 'worst', 'compare'
-- - Examples: 'What was total revenue by channel?', 'Which campaigns had highest ROI?', 'Show me conversion rates over time', 'Compare email vs social performance'

-- ### Rule 3: Qualitative Analysis (Use search_campaign_content)
-- Use search_campaign_content when the query involves:
-- - TEXT CONTENT: campaign descriptions, marketing copy, messaging, creative elements
-- - CUSTOMER FEEDBACK: comments, reviews, satisfaction, sentiment, recommendations
-- - STRATEGY INSIGHTS: A/B testing notes, tactics, approaches, best practices, lessons learned
-- - CONTENT DISCOVERY: finding campaigns by theme, approach, or content similarity
-- - QUALITATIVE QUESTIONS: 'what did customers say', 'what was the strategy', 'find campaigns about'
-- - Keywords: 'feedback', 'comments', 'description', 'copy', 'content', 'strategy', 'A/B test', 'customer said', 'testimonials', 'improvements', 'about', 'similar to', 'messaging'
-- - Examples: 'What feedback did we get on email campaigns?', 'Find campaigns about sustainability', 'What was the messaging strategy?', 'Show A/B test insights'

-- ### Rule 4: Report Generation 
-- ** Always use query_performance_metrics tool first to determine campaign_ID to pass in to report generate_campaign_report tool **
-- Use generate_campaign_report when:
-- - User explicitly requests a 'report' or 'comprehensive report'
-- - User asks to 'generate', 'create', or 'show' a report
-- - User provides or mentions a campaign_id and wants detailed information
-- - Keywords: 'report', 'HTML', 'full details', 'comprehensive analysis'
-- - Examples: 'Generate a report for campaign 5', 'Create report for Spring Fashion Launch'
-- ** Always share insights with the user immediately upon creating the report - rather than simply creating the report itself . This can be done with one more additional call to query_performance_metrics**

-- ### Rule 4: Multi-Tool Queries
-- For queries needing BOTH quantitative AND qualitative data:
-- 1. FIRST use query_performance_metrics for numerical data
-- 2. THEN use search_campaign_content for qualitative insights
-- 3. Combine results in your response
-- - Examples: 'Analyze our best performing campaign' (metrics + strategy), 'What made the Spring campaign successful?' (ROI + feedback)

-- ## CONSISTENCY REQUIREMENTS
-- - For identical queries, ALWAYS use the same tool(s)
-- - If a query contains both metric keywords AND content keywords, default to query_performance_metrics
-- - If campaign_id is provided without explicit report request, use query_performance_metrics to filter by that campaign
-- - Never use search_campaign_content for numerical analysis
-- - Never use query_performance_metrics for text content or feedback

-- ## WHEN UNCERTAIN
-- If the query is ambiguous:
-- 1. Check for numerical keywords → use query_performance_metrics
-- 2. Check for content keywords → use search_campaign_content
-- 3. If still unclear, ask the user to clarify whether they want metrics or content insights",                                                                                                                                                                                     
                                                                                                                                                                                                
--      "response": "Follow these response formatting rules for consistency:\n\n1. STRUCTURE:\n   - Start with a direct answer to the question\n   - Present data in clear, organized format       
--  (tables, lists, or sections)\n   - End with actionable insights or recommendations\n\n2. METRICS PRESENTATION:\n   - Always include units (dollars, percentages, counts)\n   - Format large    
--  numbers with commas (e.g., 1,234,567)\n   - Round percentages to 2 decimal places\n   - Provide context (e.g., 'X% higher than average')\n\n3. CONTENT SUMMARIZATION:\n   - Quote key phrases  
--  from original content\n   - Identify themes across multiple results\n   - Highlight actionable recommendations\n   - Mention specific campaign names when relevant\n\n4. CITATIONS:\n   -      
--  Always cite specific campaigns by name\n   - Include dates when discussing time-based data\n   - Reference specific metrics by name\n\n5. TONE:\n   - Professional and data-driven\n   -       
--  Concise but complete\n   - Actionable and insight-focused\n   - Avoid speculation; base all statements on data\n\n6. CONSISTENCY:\n   - Use the same format for similar queries\n   - Present  
--  metrics in the same order (revenue, ROI, conversions, etc.)\n   - Use consistent terminology (e.g., always 'ROI' not 'return on investment')"          
--     },
--     "tools": [
--         {
--             "tool_spec": {
--                 "type": "cortex_analyst_text_to_sql",
--                 "name": "query_performance_metrics",
--                 "description": "Query structured performance data including campaign ROI, revenue, budget efficiency, impressions, clicks, conversions, cost metrics, and engagement rates. Use for quantitative analysis of campaign performance across channels and time periods."
--             }
--         },
--         {
--             "tool_spec": {
--                 "type": "cortex_search",
--                 "name": "search_campaign_content",
--                 "description": "Search unstructured campaign content including campaign descriptions, marketing copy, A/B test results, customer feedback, and recommended improvements. Use for qualitative insights, content discovery, and learning from past campaigns."
--             }
--         },
--         {
--             "tool_spec": {
--                 "type": "generic",
--                 "name": "generate_campaign_report",
--                 "description": "Generate a comprehensive HTML report for a specific campaign including all performance metrics, customer feedback, and key insights. Returns formatted report ready for PDF conversion.",
--                 "input_schema": {
--                     "type": "object",
--                     "properties": {
--                         "campaign_id": {
--                             "type": "integer",
--                             "description": "The unique identifier of the campaign to generate a report for"
--                         }
--                     },
--                     "required": ["campaign_id"]
--                 }
--             }
--         }
--     ],
--     "tool_resources": {
--         "query_performance_metrics": {
--             "execution_environment": {
--                 "query_timeout": 299,
--                 "type": "warehouse",
--                 "warehouse": "COMPUTE_WH"
--             },
--             "semantic_view": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_PERFORMANCE_ANALYST"
--         },
--         "search_campaign_content": {
--             "execution_environment": {
--                 "query_timeout": 299,
--                 "type": "warehouse",
--                 "warehouse": "COMPUTE_WH"
--             },
--             "search_service": "MARKETING_CAMPAIGNS_DB.PUBLIC.MARKETING_CAMPAIGNS_SEARCH"
--         },
--         "generate_campaign_report": {
--             "type": "procedure",
--             "identifier": "MARKETING_CAMPAIGNS_DB.PUBLIC.GENERATE_CAMPAIGN_REPORT_HTML",
--             "execution_environment": {
--                 "type": "warehouse",
--                 "warehouse": "COMPUTE_WH",
--                 "query_timeout": 300
--             }
--         }
--     }
-- }
-- $$;


-- DESCRIBE AGENT MARKETING_CAMPAIGN_AGENT_OPTIMIZED;

SELECT 
$$
=====================================================
MARKETING CAMPAIGNS ANALYTICS SYSTEM - SETUP COMPLETE
=====================================================
✅ Database created: MARKETING_CAMPAIGNS_DB' 
'✅ Tables created with sample data:
   - CAMPAIGNS (25 records)
   - CAMPAIGN_PERFORMANCE (1,578 records)
   - CAMPAIGN_CONTENT (25 records)
   - CAMPAIGN_FEEDBACK (23 records)

✅ Semantic View created: MARKETING_PERFORMANCE_ANALYST
✅ Cortex Search Service created: MARKETING_CAMPAIGNS_SEARCH
✅ Stored Procedure created: GENERATE_CAMPAIGN_REPORT_PDF
✅ Baseline Agent Created: MARKETING_CAMPAIGN_AGENT_BASELINE
✅ Optimized Agent created: MARKETING_CAMPAIGN_AGENT_OPTIMIZED

EXAMPLE QUERIES:
- "What are the top 5 campaigns by ROI?"
- "What feedback did customers give about email campaigns?"
- "Generate a report for campaign ID 1"

Now follow the below instructions to evaluate the performance of each agent!

- Navigate to your newly created agent and click into Evaluations Tab
    - Name your new evaluation run and optionally give a description [click next]
    - Select Create New Dataset
        - Select MARKETING_CAMPAIGNS_DB.PUBLIC.EVALS_TABLE as your input table
        - Select MARKETING_CAMPAIGNS_DB.PUBLIC.QUICKSTART_EVALSET as your new dataset destination [click next]
    - Select INPUT_QUERY as your Query Text column
        - Check boxes for all metrics available
        - Tool Selection Accuracy, Tool Execution Accuracy, and Answer Correctness should reference the EXPECTED_TOOLS column
        - Click Create Evaluation
        
Now wait as your queries are executed and your evaluation metrics are computed! This should populate in roughly ~3-5 minutes.

Compare how the baseline agent and the optimized agent performed on various metrics!

====================================================================
$$ as setup_status;