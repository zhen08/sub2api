package callaudit

import (
	"strings"
	"testing"
	"time"
)

func TestAuditSchemaAndUpsertPreserveArtifactAndIdentityIDs(t *testing.T) {
	t.Parallel()
	for _, fragment := range []string{
		"audit_artifacts_request_kind_sequence_uidx",
		"ON audit_artifacts (request_id,kind,sequence)",
		"newer.id>older.id",
	} {
		if !strings.Contains(auditBaseSchemaSQL, fragment) {
			t.Fatalf("audit schema missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"api_key_id=COALESCE(NULLIF(audit_calls.api_key_id,''),EXCLUDED.api_key_id)",
		"api_key_name=COALESCE(NULLIF(audit_calls.api_key_name,''),EXCLUDED.api_key_name)",
		"user_username=COALESCE(NULLIF(audit_calls.user_username,''),EXCLUDED.user_username)",
		"account_id=COALESCE(NULLIF(audit_calls.account_id,''),EXCLUDED.account_id)",
	} {
		if !strings.Contains(auditUpsertCallSQL, fragment) {
			t.Fatalf("audit call upsert missing %q", fragment)
		}
	}
}

func TestAuditReadinessRequirementsCoverCrossYearPartitionsAndIndexes(t *testing.T) {
	t.Parallel()
	reference := time.Date(2026, time.December, 31, 23, 59, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	requirements := auditReadinessRequirements(reference)

	wantPartitions := []string{"audit_calls_2026_12", "audit_calls_2027_01"}
	if len(requirements.partitions) != len(wantPartitions) {
		t.Fatalf("partitions = %#v", requirements.partitions)
	}
	for index, want := range wantPartitions {
		if requirements.partitions[index] != want {
			t.Fatalf("partition[%d] = %q, want %q", index, requirements.partitions[index], want)
		}
	}

	wantIndexes := map[string]bool{
		"audit_calls_pkey":                           true,
		"audit_artifacts_request_kind_sequence_uidx": true,
		"audit_calls_2026_12_query_order_idx":        true,
		"audit_calls_2026_12_api_key_query_idx":      true,
		"audit_calls_2027_01_query_order_idx":        true,
		"audit_calls_2027_01_api_key_query_idx":      true,
	}
	for _, index := range requirements.indexes {
		delete(wantIndexes, index.name)
	}
	if len(wantIndexes) != 0 {
		t.Fatalf("readiness requirements missing indexes: %#v", wantIndexes)
	}
}

func TestAuditReadinessQueriesAreReadOnly(t *testing.T) {
	t.Parallel()
	for name, query := range map[string]string{
		"relations":  auditReadinessRelationsSQL,
		"columns":    auditReadinessColumnsSQL,
		"partitions": auditReadinessPartitionsSQL,
		"indexes":    auditReadinessIndexesSQL,
	} {
		normalized := strings.ToUpper(query)
		for _, mutation := range []string{"INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER ", "DROP ", "TRUNCATE "} {
			if strings.Contains(normalized, mutation) {
				t.Fatalf("%s readiness query contains mutation %q", name, mutation)
			}
		}
	}
}

func TestAuditWriterSQLContainsNoDDL(t *testing.T) {
	t.Parallel()
	normalized := strings.ToUpper(auditUpsertCallSQL)
	for _, ddl := range []string{"CREATE ", "ALTER ", "DROP "} {
		if strings.Contains(normalized, ddl) {
			t.Fatalf("writer upsert contains DDL %q", ddl)
		}
	}
}
