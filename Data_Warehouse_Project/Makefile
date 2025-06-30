# Makefile
.PHONY: help init plan apply destroy clean

# Default target
help:
	@echo "Sparkify Data Warehouse - Terraform Commands"
	@echo "============================================="
	@echo "init     - Initialize Terraform"
	@echo "plan     - Plan Terraform deployment"
	@echo "apply    - Apply Terraform changes"
	@echo "destroy  - Destroy infrastructure"
	@echo "clean    - Clean temporary files"
	@echo "deploy   - Full deployment script"
	@echo "cleanup  - Full cleanup script"
	@echo ""
	@echo "Usage: make <command>"

init:
	cd terraform && terraform init

plan:
	cd terraform && terraform plan

apply:
	cd terraform && terraform apply

destroy:
	cd terraform && terraform destroy

clean:
	rm -f terraform/tfplan
	rm -f terraform/.terraform.lock.hcl
	rm -rf terraform/.terraform/
	rm -f outputs.json

deploy:
	chmod +x scripts/deploy.sh
	./scripts/deploy.sh

cleanup:
	chmod +x scripts/cleanup.sh
	./scripts/cleanup.sh