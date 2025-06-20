# Deployment & Infrastructure

## Purpose
Azure infrastructure setup, deployment scripts, and project maintenance.

## Scripts
- `project_maintenance.py` - File organization and cleanup

## Bash Scripts
- `deploy_full_pipeline.sh` - Deploy complete 3-layer pipeline
- `deploy_three_layer_architecture.sh` - Deploy database architecture

## SQL Scripts
- `infrastructure_cleanup.sql` - Database cleanup utilities
- `data_migration.sql` - Data migration between environments

## Usage
```bash
# Deploy full pipeline
bash deployment_infrastructure/bash/deploy_full_pipeline.sh

# Maintain project
python deployment_infrastructure/scripts/project_maintenance.py
```
