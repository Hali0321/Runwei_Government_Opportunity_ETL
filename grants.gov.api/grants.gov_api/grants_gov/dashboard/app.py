import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import sys
import os
import numpy as np

# Add the project root to the path if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grants_gov.utils.config import get_connection_string

st.set_page_config(page_title="Grants.gov Dashboard", layout="wide")
st.title("Grants.gov Opportunities Dashboard")

# Connect to database
@st.cache_resource
def get_connection():
    connection_string = get_connection_string()
    return create_engine(connection_string)

# Load sample data if database connection fails or returns empty data
@st.cache_data
def load_sample_data():
    """Load sample data if database connection fails"""
    import datetime
    
    # Create sample data
    data = {
        'id': ['123456', '123457', '123458', '123459', '123460'],
        'title': [
            'Research in Artificial Intelligence for Climate Science', 
            'Quantum Computing Development Program',
            'STEM Education and Training Initiative',
            'Sustainable Development Research Grant',
            'Healthcare Technology Innovation'
        ],
        'agency': [
            'U.S. National Science Foundation', 
            'Department of Energy',
            'Department of Education', 
            'National Aeronautics and Space Administration',
            'Department of Health and Human Services'
        ],
        'award_value': [500000, 1000000, 250000, 750000, 1500000],
        'category': [
            'Science and Technology', 
            'Science and Technology',
            'Education', 
            'Environment',
            'Health'
        ],
        'opportunity_status': ['posted', 'posted', 'posted', 'posted', 'posted'],
        'open_date': [
            datetime.date(2025, 1, 1),
            datetime.date(2025, 2, 1),
            datetime.date(2025, 3, 1),
            datetime.date(2025, 4, 1),
            datetime.date(2025, 5, 1)
        ],
        'close_date': [
            datetime.date(2025, 12, 31),
            datetime.date(2025, 11, 30),
            datetime.date(2025, 10, 31),
            datetime.date(2025, 9, 30),
            datetime.date(2025, 8, 31)
        ],
        'cost_sharing_required': [False, True, False, True, False]
    }
    
    return pd.DataFrame(data)

# Load data with proper connection handling
@st.cache_data(ttl=600)
def load_data():
    try:
        engine = get_connection()
        
        # Try a different query that includes NULL award values
        query = """
        SELECT id, title, agency, award_value, category, opportunity_status,
               open_date, close_date, cost_sharing_required
        FROM opportunities
        """
        
        # Use connection properly with pandas
        with engine.connect() as connection:
            df = pd.read_sql_query(text(query), connection)
            
        # Check if we got any results
        if df.empty:
            st.warning("No opportunities found in the database.")
            return load_sample_data()
            
        # If award_value contains only nulls, use sample data
        if df['award_value'].isna().all():
            st.warning("No opportunities with award values found in the database.")
            return load_sample_data()
            
        return df
    
    except Exception as e:
        st.error(f"Database error: {e}")
        st.info("Loading sample data instead.")
        return load_sample_data()

# Show a spinner while loading data
with st.spinner("Loading grant data..."):
    df = load_data()

# Dashboard layout
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Filters")
    
    # Agency filter
    agencies = ['All'] + sorted(df['agency'].unique().tolist())
    selected_agency = st.selectbox('Select Agency', agencies)
    
    # Category filter
    categories = ['All'] + sorted([c for c in df['category'].unique() if pd.notna(c)])
    selected_category = st.selectbox('Select Category', categories)
    
    # Award range filter - handle NaN values safely
    awards = df['award_value'].dropna()
    if len(awards) > 0:
        min_award = int(awards.min())
        max_award = int(awards.max())
    else:
        # Default values if no award data
        min_award = 0
        max_award = 1000000
    
    award_range = st.slider(
        'Award Value Range ($)',
        min_award, max_award, (min_award, max_award)
    )
    
    # Apply filters
    filtered_df = df.copy()
    if selected_agency != 'All':
        filtered_df = filtered_df[filtered_df['agency'] == selected_agency]
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]
    
    # Filter by award range (handle NaN values)
    filtered_df = filtered_df[
        (filtered_df['award_value'].notna()) & 
        (filtered_df['award_value'] >= award_range[0]) & 
        (filtered_df['award_value'] <= award_range[1])
    ]
    
    st.write(f"Showing {len(filtered_df)} opportunities")

with col2:
    st.subheader("Award Distribution by Agency")
    
    if not filtered_df.empty:
        # Make sure we have award values before creating chart
        awards_by_agency = filtered_df.dropna(subset=['award_value']).groupby('agency')['award_value'].mean().reset_index()
        
        if not awards_by_agency.empty:
            fig = px.bar(
                awards_by_agency.sort_values('award_value', ascending=False),
                x='agency',
                y='award_value',
                title='Average Award Value by Agency',
                labels={'award_value': 'Average Award Value ($)', 'agency': 'Agency'}
            )
            st.plotly_chart(fig)
        else:
            st.info("No award values available for the selected filters.")
    else:
        st.write("No data to display with current filters")

# Show opportunities table
st.subheader("Opportunities")
if not filtered_df.empty:
    # Ensure we handle potential missing columns
    display_cols = ['title', 'agency', 'award_value']
    for col in ['category', 'open_date', 'close_date']:
        if col in filtered_df.columns:
            display_cols.append(col)
            
    st.dataframe(
        filtered_df[display_cols].sort_values('award_value', ascending=False),
        hide_index=True,
        column_config={
            'award_value': st.column_config.NumberColumn(
                'Award Value',
                format="$%d"
            ),
            'open_date': st.column_config.DateColumn('Open Date'),
            'close_date': st.column_config.DateColumn('Close Date')
        }
    )
else:
    st.info("No opportunities match your current filters.")

# Add debug section at the bottom (remove in production)
if st.checkbox("Show Database Debug Info"):
    st.subheader("Database Debug Information")
    
    # Show connection information
    try:
        connection_string = get_connection_string()
        # Mask password for security
        masked_connection = connection_string.replace(":", ":****@", 1) if "@" in connection_string else connection_string
        st.text(f"Connection string: {masked_connection}")
        
        # Show raw database content
        engine = get_connection()
        with engine.connect() as conn:
            table_query = text("SELECT COUNT(*) FROM opportunities")
            count = conn.execute(table_query).scalar()
            st.text(f"Total records in opportunities table: {count}")
            
            # Show columns with non-null counts
            column_query = text("""
            SELECT column_name, COUNT(column_name) 
            FROM opportunities 
            WHERE award_value IS NOT NULL 
            GROUP BY column_name
            """)
            try:
                columns_with_award = conn.execute(column_query).fetchall()
                st.text(f"Records with non-null award values: {columns_with_award}")
            except:
                st.text("Could not query column statistics")
    except Exception as e:
        st.text(f"Debug error: {e}")