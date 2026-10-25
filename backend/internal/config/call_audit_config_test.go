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

func TestLoadCallAuditDeadLetterRetryDefaults(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.CallAudit.Worker.DeadLetterRetry
	if !got.Enabled || got.MaxRetries != 12 || got.BatchSize != 2 || got.InitialDelaySeconds != 900 || got.MaxDelaySeconds != 21600 {
		t.Fatalf("dead-letter retry defaults = %+v", got)
	}
}

func TestLoadCallAuditDeadLetterRetryEnvironment(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("CALL_AUDIT_ENABLED", "true")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_RETRY_ENABLED", "true")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_MAX_RETRIES", "8")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_BATCH_SIZE", "3")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_INITIAL_DELAY_SECONDS", "1200")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_MAX_DELAY_SECONDS", "7200")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.CallAudit.Worker.DeadLetterRetry
	if !got.Enabled || got.MaxRetries != 8 || got.BatchSize != 3 || got.InitialDelaySeconds != 1200 || got.MaxDelaySeconds != 7200 {
		t.Fatalf("dead-letter retry environment = %+v", got)
	}
}

func TestLoadCallAuditRejectsUnboundedDeadLetterRetry(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"zero retries", map[string]string{"CALL_AUDIT_DEAD_LETTER_MAX_RETRIES": "0"}},
		{"negative retries", map[string]string{"CALL_AUDIT_DEAD_LETTER_MAX_RETRIES": "-1"}},
		{"excessive retries", map[string]string{"CALL_AUDIT_DEAD_LETTER_MAX_RETRIES": "101"}},
		{"empty batch", map[string]string{"CALL_AUDIT_DEAD_LETTER_BATCH_SIZE": "0"}},
		{"excessive batch", map[string]string{"CALL_AUDIT_DEAD_LETTER_BATCH_SIZE": "101"}},
		{"zero delay", map[string]string{"CALL_AUDIT_DEAD_LETTER_INITIAL_DELAY_SECONDS": "0"}},
		{"inverted delays", map[string]string{"CALL_AUDIT_DEAD_LETTER_INITIAL_DELAY_SECONDS": "3600", "CALL_AUDIT_DEAD_LETTER_MAX_DELAY_SECONDS": "1800"}},
		{"excessive delay", map[string]string{"CALL_AUDIT_DEAD_LETTER_MAX_DELAY_SECONDS": "604801"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			t.Setenv("CALL_AUDIT_ENABLED", "true")
			t.Setenv("CALL_AUDIT_DEAD_LETTER_RETRY_ENABLED", "true")
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			if _, err := Load(); err == nil {
				t.Fatal("invalid dead-letter retry settings were accepted")
			}
		})
	}
}

func TestLoadCallAuditCanDisableDeadLetterRetry(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("CALL_AUDIT_ENABLED", "true")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_RETRY_ENABLED", "false")
	t.Setenv("CALL_AUDIT_DEAD_LETTER_MAX_RETRIES", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallAudit.Worker.DeadLetterRetry.Enabled {
		t.Fatal("explicitly disabled dead-letter retry was enabled")
	}
}
