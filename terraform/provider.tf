terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-north-1"
}

provider "google" {
  project = var.gcp_project_id
  region  = "eu-north1"
}