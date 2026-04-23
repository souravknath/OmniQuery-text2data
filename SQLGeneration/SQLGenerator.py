from groq import Groq
import os
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
                temperature=0.1  # low temperature for deterministic SQL
            )

            sql_query = response.choices[0].message.content.strip()

            # Optional cleanup (remove markdown if LLM adds it)
            if sql_query.startswith("```"):
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

            return sql_query

        except Exception as e:
            raise Exception(f"Error generating SQL: {str(e)}")


# -------------------------
# Example Usage
# -------------------------

if __name__ == "__main__":
    API_KEY = os.getenv("GROQ_API_KEY")

    if not API_KEY:
        print("Please set GROQ_API_KEY in your environment or .env file.")
        exit(1)

    sql_generator = SQLGenerator(api_key=API_KEY)

    system_prompt = """
            You are an expert SQL generator.

            Rules:
            - Output ONLY SQL query. No explanation.
            - Use ANSI SQL compatible syntax.
            - Use proper JOINs based on relationships.
            - Do not hallucinate columns or tables.

            Database Schema:

            Table: Customers
            - CustomerID (PK)
            - Name
            - Country

            Table: Orders
            - OrderID (PK)
            - CustomerID (FK -> Customers.CustomerID)
            - OrderDate
            - Amount

            Relationships:
            - Customers.CustomerID = Orders.CustomerID
"""

    user_prompt = "Get total order amount per customer for customers in India"

    sql_query = sql_generator.generate_sql(system_prompt, user_prompt)

    print("Generated SQL:\n")
    print(sql_query)