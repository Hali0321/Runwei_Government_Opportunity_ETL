# 🔒 Security Notice

## Sensitive Files - Not for Public Repository

The following files contain sensitive information and should never be committed to public repositories:

### Configuration Files
- `.env` - Contains database passwords and connection strings
- `config/SQL_Server_Connection_Details.txt` - Contains actual database credentials

### What's Included Instead
- `.env.example` - Template file with placeholder values
- `config/SQL_Server_Connection_Details_EXAMPLE.txt` - Template with example credentials

## Setup Instructions

1. **Copy the example files:**
   ```bash
   cp .env.example .env
   cp config/SQL_Server_Connection_Details_EXAMPLE.txt config/SQL_Server_Connection_Details.txt
   ```

2. **Edit with your actual credentials:**
   ```bash
   nano .env
   nano config/SQL_Server_Connection_Details.txt
   ```

3. **Never commit the real files:**
   - The `.gitignore` file already excludes these sensitive files
   - Always use the `.example` versions as templates

## Azure VM Security

For production deployment on Azure VM:
- Credentials are stored locally in `.env` file on the VM
- Repository changes don't affect existing VM credentials
- Use environment variables or Azure Key Vault for enhanced security

## Best Practices

✅ **Do:**
- Use example/template files in public repositories
- Store real credentials in private repositories or secure vaults
- Use different credentials for development vs production
- Regularly rotate passwords

❌ **Don't:**
- Commit actual passwords to any repository
- Share database credentials in public documentation
- Use the same credentials across multiple environments
