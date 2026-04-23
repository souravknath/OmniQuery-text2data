from groq import Groq
import os
import json
from dotenv import load_dotenv

load_dotenv()

class SQLGenerator:
    def __init__(self, api_key: str, model: str = "openai/gpt-oss-120b"):
        self.client = Groq(api_key=api_key)
        self.model = model

    def generate_sql(self, system_prompt: str, user_prompt: str) -> str:
        """
        Generates SQL query using Groq LLM
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ],
                temperature=0.1
            )

            sql_query = response.choices[0].message.content.strip()

            if sql_query.startswith("```"):
                sql_query = sql_query.replace("```json", "").replace("```sql", "").replace("```", "").strip()

            return sql_query

        except Exception as e:
            raise Exception(f"Error generating SQL: {str(e)}")


def load_schemas():
    schemas = {}
    schema_dir = "DBSchemas"
    if os.path.exists(schema_dir):
        for filename in os.listdir(schema_dir):
            if filename.endswith(".json"):
                db_name = filename.replace("_Schema.json", "")
                with open(os.path.join(schema_dir, filename), "r") as f:
                    schemas[db_name] = json.load(f)
    return json.dumps(schemas, indent=2)

if __name__ == "__main__":
    API_KEY = os.getenv("GROQ_API_KEY")
    if not API_KEY:
        print("Please set GROQ_API_KEY in your environment or .env file.")
        exit(1)
    sql_generator = SQLGenerator(api_key=API_KEY)
    schemas_json = load_schemas()

    system_prompt = f"""
        You are an expert multi-database query generator.
        Your task is to generate queries for different databases and explain how to combine the data.
        A "meaningful" result is expected, so always include descriptive fields like Customer Names, Product Names, and Locations in the queries and final selection, not just IDs.

        Rules:
        - Output ONLY a JSON object. No explanation, no conversational text.
        - Use valid SQL syntax for SQL databases (Postgres_Sales_DB, SQL_Inventory_DB).
        - For MongoDB (Mongo_Customer_DB), output a stringified JSON object exactly in this format: '{{"collection": "collection_name", "pipeline": [...]}}'
        - If you query the same database multiple times (e.g. for different collections or tables), give each entry a UNIQUE name in the "databases" list and "execution_order" (e.g. "Mongo_Customer_Address", "Mongo_Customer_Profile").
        - Only query the databases necessary to answer the user's prompt.
        - If a query depends on the results of another query, use a placeholder like {{DatabaseName.FieldName}} or {{{{DatabaseName.FieldName}}}}.
        - IMPORTANT: Ensure all opened parentheses `(` are correctly closed `)`. Check subqueries and `IN (...)` clauses carefully.
        - IMPORTANT: If a table name is a reserved word (e.g., "Order"), wrap it in double quotes (e.g., `"Order"`).
        - Determine the correct "execution_order" array, specifying the sequence of databases to query so dependencies are resolved.
        - Ensure the join conditions correctly map the fields between the different database results.
        - Do not hallucinate columns, tables, or collections. Only use what is provided in the schema.

        Database Schemas:
        {schemas_json}

        OUT JSON Structure:
        {{
            "execution_order": [
                "DB_NAME_1",
                "DB_NAME_2"
            ],
            "databases": [
                {{
                "name": "<DatabaseName>",
                "query": "<SELECT ... or MongoDB JSON. Use {{{{OtherDB.Field}}}} for dependencies>"
                }}
            ],
            "join": {{
                "type": "<inner|left|right|full>",
                "conditions": [
                    "<DB1>.<field> = <DB2>.<field>"
                ]
            }},
            "final_select": [
                "<field_name1>",
                "<field_name2>"
            ]
        }}
        """

    import sys
    user_prompt = sys.argv[1] if len(sys.argv) > 1 else "Get total order amount per customer for customers in Phoenix who bought the product Webcam HD"

    sql_query = sql_generator.generate_sql(system_prompt, user_prompt)

    print("Generated SQL:\n")
    print(sql_query)

    with open("llm_output.json", "w") as f:
        f.write(sql_query)
    print("\nSaved generated SQL to llm_output.json")