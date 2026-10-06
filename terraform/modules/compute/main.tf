terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  # Bucket, key, and region come from -backend-config. use_lockfile is S3
  # native state locking (Terraform 1.10+); no DynamoDB table needed.
  backend "s3" {
    use_lockfile = true
  }
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      ApexEnv            = var.env_name
      ApexIdempotencyKey = var.idempotency_key
    }
  }
}

variable "env_name"          { type = string }
variable "subnet_id"         { type = string }
variable "security_group_id" { type = string }
variable "idempotency_key" {
  type    = string
  default = ""
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

variable "app_version" {
  type    = string
  default = "v1"
}

resource "aws_instance" "web" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = "t3.micro"
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  associate_public_ip_address = true

  user_data_replace_on_change = true   # forces replace when user_data changes

  user_data = <<-EOF
    #!/bin/bash
    dnf install -y nginx
    echo "<h1>Apex demo: ${var.env_name} ${var.app_version} healthy</h1>" > /usr/share/nginx/html/index.html
    systemctl enable --now nginx
  EOF

  tags = { Name = "apex-${var.env_name}-web", Env = var.env_name, AppVersion = var.app_version }
}

output "instance_id" { value = aws_instance.web.id }
output "public_ip"   { value = aws_instance.web.public_ip }