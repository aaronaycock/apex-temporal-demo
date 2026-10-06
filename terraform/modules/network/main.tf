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

variable "env_name"        { type = string }
variable "idempotency_key" {
  type    = string
  default = ""
}

resource "aws_vpc" "this" {
  cidr_block           = "10.50.0.0/16"
  enable_dns_hostnames = true
  tags = { Name = "apex-${var.env_name}-vpc", Env = var.env_name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "apex-${var.env_name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.50.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "us-east-1a"
  tags                    = { Name = "apex-${var.env_name}-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "apex-${var.env_name}-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "web" {
  name        = "apex-${var.env_name}-web"
  description = "Demo SG: allow HTTP from anywhere"
  vpc_id      = aws_vpc.this.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "vpc_id"            { value = aws_vpc.this.id }
output "subnet_id"         { value = aws_subnet.public.id }
output "security_group_id" { value = aws_security_group.web.id }