# Snowflake SQL to DBT Model Using LLM Converter

## Overview
The **Snowflake SQL to dbt Model Converter** simplifies the migration from Snowflake SQL to dbt (data build tool) by leveraging the power of LLMs. Instead of manually rewriting SQL scripts into dbt models, the tool automates this process, transforming Snowflake SQL statements into dbt-compatible SQL quickly and efficiently.

Whether you have a single SQL query or a set of related SQL statements, the converter generates a **clean, structured dbt model** that adheres to dbt best practices. Additionally, the tool gives developers the ability to provide **design hints** (e.g., materialization type, partitioning logic, primary keys) to guide the model creation process and ensure optimal performance.

---

## Problem Statement
Migrating Snowflake SQL workloads to dbt is a time-consuming and error-prone task. Manual conversion requires significant development effort to:
- Refactor SQL into dbt-compatible templates (e.g., Jinja templating, macros).
- Ensure the resulting models adhere to dbt best practices.
- Handle connected queries, such as **Common Table Expressions (CTEs)** or temporary tables, that must be incorporated into unified dbt models.
- Maintain consistency, scalability, and accurate dependency mappings across models.

This creates a bottleneck in migrating to dbt, increasing the time and resources required for onboarding teams and organizations.

---

## Solution
Introducing a **LLM-based Code Converter** for Snowflake SQL to dbt models. The tool uses advanced **AI models** to transform SQL queries into dbt-compatible scripts in seconds, significantly reducing manual effort and enabling faster migration. 

Developers can utilize **design hints** to guide the behavior and structure of the output models, ensuring they align with project requirements.

### How It Works
1. **Input SQL**: Developers input one or more Snowflake SQL statements into the tool's interface.
2. **Add Design Hints (Optional)**: Specify key design elements, such as:
   - **Materialization type**: `table`, `incremental`, or `view`.
   - **Primary key(s)** for deduplication in incremental models.
   - **Partitioning or clustering** details.
   - **References to other dbt models or sources**.
3. **LLM Conversion**: The tool uses structured prompts to analyze the SQL, extract logic, detect dependencies, and apply Jinja templating where needed.
4. **Output dbt Model**: The final result is a dbt model script compliant with dbt best practices and ready for integration into your dbt project.

---

## Key Features and Capabilities
- **Batch Conversion**: Automatically converts multiple related SQL statements (e.g., CTEs) into a single dbt model. This is particularly useful for SQL logic spanning multiple steps or temporary tables.
- **Customizable Models**: Accepts design hints to guide the resulting model, including:
  - Materialization type (`table`, `incremental`, `view`).
  - Primary/key columns.
  - Partitioning or deduplication logic.
- **Accelerated Migration**: Minimizes manual code conversion efforts, speeding up dbt onboarding and adoption across teams.
- **Consistency and Scalability**: Produces models that adhere to dbt best practices, ensuring consistent patterns and maintainable codebases.

---

## Limitations
While the converter reduces manual effort, it has some limitations:
1. **Token Limit**: Inputs exceeding 10,000 tokens or approximately more than 15,000 characters—may result in incomplete or failed outputs. Developers are encouraged to break large SQL scripts into concise SQLs (or) smaller pieces before using the tool.
2. **Input Relevance**: The tool works best when SQL statements are logically connected. Including loosely related or unrelated SQL in a batch can generate inaccurate results.
3. **Developer Judgement Needed**: The tool requires developers to make key design decisions, such as selecting the materialization type or identifying primary keys for deduplication. Providing these as hints ensures higher-quality outputs.

---

## Best Practices
To optimize results and ensure smooth model generation:
1. **Group Logically Connected SQL**: Include related SQL statements that are intended to form part of the same dbt model. Avoid mixing unrelated queries in one batch.
2. **Provide Concise Inputs**: Simplify and clean the SQL to reduce complexity and stay within limits for accurate results.
3. **Use Design Hints**: Explicitly specify key elements such as materialization type, primary keys, and dependencies to guide the LLM.
4. **Modularize Complex Logic**: For complex workflows, break down transformations into smaller, manageable models. These can later be joined or referenced within dbt.
5. **Review and Test**: Always review the generated models for correctness and performance before integration into production environments.

---

Please find the examples: [a link](https://github.com/myfsuhail/stock_market_automation/blob/md_test/snowflake_to_dbt_examples.md)
