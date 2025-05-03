import sys
import importlib

# Force Python to reload the module
if 'grants_gov.utils.data_generator' in sys.modules:
    del sys.modules['grants_gov.utils.data_generator']

# Add project root to path
sys.path.insert(0, '/Users/dinghali/Desktop/25 Spring/grants.gov_api')

# Try importing
try:
    from grants_gov.utils.data_generator import generate_sample_opportunities
    print("✅ Import successful!")
    
    # Test the function
    sample_data = generate_sample_opportunities(5)
    print(f"Generated {len(sample_data)} samples")
except ImportError as e:
    print(f"❌ Import failed: {e}")

    # Print file contents for debugging
    print("\nFile contents:")
    try:
        with open('/Users/dinghali/Desktop/25 Spring/grants.gov_api/grants_gov/utils/data_generator.py', 'r') as f:
            print(f.read())
    except Exception as read_error:
        print(f"Error reading file: {read_error}")