import os
import glob

def update_url_format_in_files():
    # Define the directory to search in
    base_dir = "/Users/dinghali/Desktop/Runwei/grants_gov_api_azure/clean_project/src/functions/GrantsGovFunctions"
    
    # Find all Python files
    python_files = glob.glob(f"{base_dir}/**/*.py", recursive=True)
    
    # The incorrect URL format to replace
    old_url = "https://www.grants.gov/search-grant-opportunities.html?oppId="
    # The correct URL format
    new_url = "https://www.grants.gov/search-results-detail/"
    
    files_updated = 0
    
    # Process each file
    for file_path in python_files:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check if the file contains the incorrect URL format
        if old_url in content:
            # Replace the incorrect URL format with the correct one
            updated_content = content.replace(old_url, new_url)
            
            # Write the updated content back to the file
            with open(file_path, 'w') as f:
                f.write(updated_content)
                
            print(f"Updated URL format in: {file_path}")
            files_updated += 1
    
    print(f"Total files updated: {files_updated}")

if __name__ == "__main__":
    update_url_format_in_files()
