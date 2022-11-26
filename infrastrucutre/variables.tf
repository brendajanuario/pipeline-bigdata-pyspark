variable "git_provider" {
  default = "gitLab"
}

variable "git_repo" {
  default = "https://git.toptal.com/screening/Brenda-Alexsandra-Januario"
}

variable "branch" {
  default = "main"
}

variable "instance_profile_arn" {
  default = "arn:aws:iam::275149370644:instance-profile/instance-profile"
}

variable "account_number" {
  default = "275149370644"
}

terraform {
  backend "s3" {
    bucket = "toptal-exercise-data-raw"
    key    = "tfstate"
    region = "us-east-1"
  }
}

