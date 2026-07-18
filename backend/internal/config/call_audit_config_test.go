package config

import "testing"

func TestLoadCallAuditDefaults(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallAudit.Enabled || cfg.CallAudit.RetentionDays != 180 || cfg.CallAudit.FailurePolicy != "nonblocking" ||
		cfg.CallAudit.SpoolDir != "/app/data/audit-spool" || cfg.CallAudit.ObjectKeyPrefix != "ai-call-audit" ||
		!cfg.CallAudit.Worker.Enabled || cfg.CallAudit.Worker.MaxAttempts != 5 || cfg.CallAudit.S3.Bucket != "ai-call-audit" ||
		cfg.CallAudit.S3.Region != "us-east-1" || !cfg.CallAudit.S3.ForcePathStyle {
		t.Fatalf("call audit defaults = %+v", cfg.CallAudit)
	}
}

func TestLoadCallAuditLegacyEnvironmentAliases(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("AUDIT_CAPTURE_ENABLED", "true")
	t.Setenv("AUDIT_RETENTION_DAYS", "90")
	t.Setenv("AUDIT_SPOOL_DIR", " /var/lib/audit ")
	t.Setenv("AUDIT_OBJECT_KEY_PREFIX", "/legacy-prefix/")
	t.Setenv("AUDIT_MAX_ATTEMPTS", "7")
	t.Setenv("AUDIT_MINIO_ENDPOINT", "http://minio:9000")
	t.Setenv("AUDIT_MINIO_BUCKET", "legacy-bucket")
	t.Setenv("AUDIT_MINIO_ACCESS_KEY", "access")
	t.Setenv("AUDIT_MINIO_SECRET_KEY", "secret")
	t.Setenv("AUDIT_MINIO_REGION", "cn-north-1")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.CallAudit.Enabled || cfg.CallAudit.RetentionDays != 90 || cfg.CallAudit.SpoolDir != "/var/lib/audit" ||
		cfg.CallAudit.ObjectKeyPrefix != "legacy-prefix" || cfg.CallAudit.Worker.MaxAttempts != 7 ||
		cfg.CallAudit.S3.Endpoint != "http://minio:9000" || cfg.CallAudit.S3.Bucket != "legacy-bucket" ||
		cfg.CallAudit.S3.AccessKey != "access" || cfg.CallAudit.S3.SecretKey != "secret" || cfg.CallAudit.S3.Region != "cn-north-1" {
		t.Fatalf("legacy call audit config = %+v", cfg.CallAudit)
	}
}

func TestLoadCallAuditNewEnvironmentTakesPrecedence(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("CALL_AUDIT_RETENTION_DAYS", "30")
	t.Setenv("AUDIT_RETENTION_DAYS", "90")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallAudit.RetentionDays != 30 {
		t.Fatalf("retention days = %d", cfg.CallAudit.RetentionDays)
	}
}

func TestLoadCallAuditRejectsUnimplementedRetentionDeletion(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("CALL_AUDIT_ENABLED", "true")
	t.Setenv("CALL_AUDIT_RETENTION_CLEANUP_ENABLED", "true")
	if _, err := Load(); err == nil {
		t.Fatal("retention deletion must remain gated until the dry-run rollout is approved")
	}
}
