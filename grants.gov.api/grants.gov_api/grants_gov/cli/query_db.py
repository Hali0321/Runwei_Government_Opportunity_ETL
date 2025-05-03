import pandas as pd
import argparse
from sqlalchemy import create_engine, text
from database.db_config import get_connection_string

def run_query(query, limit=None):
    """
    Run a query against the database and return results as a DataFrame.
    
    Args:
        query: SQL query to run
        limit: Optional limit on number of results
    
    Returns:
        pandas DataFrame with query results
    """
    connection_string = get_connection_string()
    engine = create_engine(connection_string)
    
    if limit and 'LIMIT' not in query.upper():
        query = f"{query} LIMIT {limit}"
    
    return pd.read_sql(query, engine)

def show_summary():
    """Show a summary of opportunities in the database"""
    print("\n=== DATABASE SUMMARY ===")
    
    # Total count
    total = run_query("SELECT COUNT(*) as count FROM opportunities")
    print(f"Total opportunities: {total['count'].iloc[0]}")
    
    # By agency
    agencies = run_query("SELECT agency, COUNT(*) as count FROM opportunities GROUP BY agency ORDER BY count DESC")
    print("\nOpportunities by agency:")
    for _, row in agencies.iterrows():
        print(f"  {row['agency']}: {row['count']}")
    
    # By category
    categories = run_query("SELECT category, COUNT(*) as count FROM opportunities GROUP BY category ORDER BY count DESC")
    print("\nOpportunities by category:")
    for _, row in categories.iterrows():
        print(f"  {row['category']}: {row['count']}")
    
    # By award amount ranges
    awards = run_query("""
        SELECT 
            CASE 
                WHEN award_value <= 100000 THEN 'Up to $100k'
                WHEN award_value <= 500000 THEN '$100k - $500k'
                WHEN award_value <= 1000000 THEN '$500k - $1M'
                WHEN award_value <= 5000000 THEN '$1M - $5M'
                ELSE 'Over $5M'
            END as range,
            COUNT(*) as count
        FROM opportunities
        WHERE award_value IS NOT NULL
        GROUP BY range
        ORDER BY min(award_value)
    """)
    print("\nOpportunities by award amount:")
    for _, row in awards.iterrows():
        print(f"  {row['range']}: {row['count']}")

def show_recent(limit=10):
    """Show most recently posted opportunities"""
    print(f"\n=== {limit} MOST RECENT OPPORTUNITIES ===")
    
    recent = run_query(f"""
        SELECT id, title, agency, award_value, category, open_date, close_date
        FROM opportunities
        ORDER BY open_date DESC
        LIMIT {limit}
    """)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 40)
    print(recent)

def run_custom_query(query, limit=None):
    """Run a custom query and display results"""
    print("\n=== CUSTOM QUERY RESULTS ===")
    results = run_query(query, limit)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 40)
    print(results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query the grants database")
    parser.add_argument("--summary", action="store_true", help="Show database summary")
    parser.add_argument("--recent", type=int, metavar="N", help="Show N most recent opportunities")
    parser.add_argument("--query", help="Run a custom SQL query")
    parser.add_argument("--limit", type=int, help="Limit the number of results")
    
    args = parser.parse_args()
    
    if args.summary:
        show_summary()
    
    if args.recent:
        show_recent(args.recent)
    
    if args.query:
        run_custom_query(args.query, args.limit)
    
    # If no arguments provided, show help
    if not (args.summary or args.recent or args.query):
        parser.print_help()