###############################################################################
# Hive Metastore backing DB: PostgreSQL in private subnets, reachable only from
# the EKS nodes. Master password is generated and stored ONLY in Secrets Manager
# (+ encrypted remote state) — never in a committed file.
###############################################################################

resource "aws_db_subnet_group" "metastore" {
  name       = "${var.name_prefix}-metastore"
  subnet_ids = module.vpc.private_subnets
  tags       = local.tags
}

resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds"
  description = "Hive Metastore Postgres - ingress from EKS nodes only"
  vpc_id      = module.vpc.vpc_id
  tags        = merge(local.tags, { Name = "${var.name_prefix}-rds" })
}

# Postgres 5432 from the EKS node security group only.
resource "aws_vpc_security_group_ingress_rule" "rds_from_nodes" {
  security_group_id            = aws_security_group.rds.id
  description                  = "Postgres from EKS worker nodes"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = module.eks.node_security_group_id
}

resource "aws_vpc_security_group_egress_rule" "rds_all_out" {
  security_group_id = aws_security_group.rds.id
  description       = "Allow all egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "random_password" "metastore" {
  length           = 32
  special          = true
  override_special = "!#$%*-_=+:?" # RDS-safe set (no / @ " or space)
}

resource "aws_db_instance" "metastore" {
  identifier     = "${var.name_prefix}-metastore"
  engine         = "postgres"
  engine_version = var.rds_engine_version
  instance_class = var.rds_instance_class

  db_name  = var.metastore_db_name
  username = var.metastore_db_username
  password = random_password.metastore.result
  port     = 5432

  allocated_storage     = var.rds_allocated_storage
  max_allocated_storage = var.rds_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true

  db_subnet_group_name   = aws_db_subnet_group.metastore.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  multi_az               = var.rds_multi_az
  publicly_accessible    = false

  backup_retention_period = var.rds_backup_retention_days
  deletion_protection     = var.rds_deletion_protection
  skip_final_snapshot     = var.rds_skip_final_snapshot
  final_snapshot_identifier = (
    var.rds_skip_final_snapshot ? null : "${var.name_prefix}-metastore-final"
  )

  auto_minor_version_upgrade = true
  apply_immediately          = false

  tags = merge(local.tags, { Name = "${var.name_prefix}-metastore" })
}

###############################################################################
# Secrets Manager: the connection bundle the Hive Metastore reads at runtime.
# HMS pods get secretsmanager:GetSecretValue on this ARN via their IRSA role.
###############################################################################

resource "aws_secretsmanager_secret" "metastore" {
  name        = "${var.name_prefix}/metastore/connection"
  description = "Hive Metastore Postgres connection (host/port/db/user/password + JDBC URL)."
  tags        = local.tags
}

resource "aws_secretsmanager_secret_version" "metastore" {
  secret_id = aws_secretsmanager_secret.metastore.id
  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.metastore.address
    port     = aws_db_instance.metastore.port
    dbname   = var.metastore_db_name
    username = var.metastore_db_username
    password = random_password.metastore.result
    jdbc_url = "jdbc:postgresql://${aws_db_instance.metastore.address}:${aws_db_instance.metastore.port}/${var.metastore_db_name}"
  })
}
