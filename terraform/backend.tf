terraform {
  backend "s3" {
    bucket = "coretelecoms-tf-state-backend-bimie" 
    key    = "global/s3/terraform.tfstate"
    region = "eu-north-1"
    encrypt = true
  }
}