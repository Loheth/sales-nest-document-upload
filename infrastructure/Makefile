# Document Analysis Microservice Infrastructure Makefile
# Common Terraform commands for managing infrastructure

.PHONY: plan-dev apply-dev init-dev migrate-dev plan-prod apply-prod init init-prod migrate-prod format lint help

# Default target: show help
.DEFAULT_GOAL := help

help:
	@echo "Available commands:"
	@echo "  make init           - Initialize terraform for all environments"
	@echo "  make init-dev       - Initialize terraform for dev environment"
	@echo "  make plan-dev       - Run terraform plan for dev environment"
	@echo "  make apply-dev      - Apply terraform changes for dev environment"
	@echo "  make migrate-dev    - Migrate state for dev environment (use when backend changes)"
	@echo "  make init-prod      - Initialize terraform for prod environment"
	@echo "  make plan-prod      - Run terraform plan for prod environment"
	@echo "  make apply-prod     - Apply terraform changes for prod environment"
	@echo "  make migrate-prod   - Migrate state for prod environment (use when backend changes)"
	@echo "  make format         - Format all Terraform files"
	@echo "  make lint           - Format Terraform files and run Checkov security scan"
	@echo ""
	@echo "Note: Run init commands only when needed (first setup, backend changes, etc.)"

# Initialize all environments
init: init-dev init-prod
	@echo "All environments initialized!"

init-dev:
	@echo "Initializing dev environment..."
	@cd environments/dev && \
		terraform init -backend-config=backend.hcl

# Migrate state for dev environment (use when backend configuration changes)
migrate-dev:
	@echo "Migrating dev environment state..."
	@cd environments/dev && \
		terraform init -migrate-state -backend-config=backend.hcl

# Plan dev environment
plan-dev:
	@echo "Planning dev environment..."
	@cd environments/dev && terraform plan

# Apply dev environment
apply-dev:
	@echo "Applying dev environment changes..."
	@cd environments/dev && terraform apply

# Initialize prod environment
init-prod:
	@echo "Initializing prod environment..."
	@cd environments/prod && \
		terraform init -backend-config=backend.hcl

# Migrate state for prod environment (use when backend configuration changes)
migrate-prod:
	@echo "Migrating prod environment state..."
	@cd environments/prod && \
		terraform init -migrate-state -backend-config=backend.hcl

# Plan prod environment
plan-prod:
	@echo "Planning prod environment..."
	@cd environments/prod && terraform plan

# Apply prod environment
apply-prod:
	@echo "Applying prod environment changes..."
	@cd environments/prod && terraform apply

# Format all Terraform files
format:
	@echo "Formatting Terraform files..."
	@terraform fmt -recursive
	@echo "Terraform files formatted"

# Run formatting and Checkov linting/security scan
lint: format
	@echo "Running Checkov security scan..."
	@checkov -d . --framework terraform 2>/dev/null || true
