package main

import (
	"sort"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fake "k8s.io/client-go/dynamic/fake"
)

func newUnstructuredBackup(name, namespace, creationTimestamp, actionName, schedule, status, backupLocation string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "cr.kanister.io/v1alpha1",
			"kind":       "ActionSet",
			"metadata": map[string]interface{}{
				"namespace":         namespace,
				"name":              name,
				"creationTimestamp": creationTimestamp,
			},
			"spec": map[string]interface{}{
				"actions": []interface{}{
					map[string]interface{}{
						"name": actionName,
						"options": map[string]interface{}{
							"backup-schedule": schedule,
						},
					},
				},
			},
			"status": map[string]interface{}{
				"state": status,
				"actions": []interface{}{
					map[string]interface{}{
						"artifacts": map[string]interface{}{
							"cloudObject": map[string]interface{}{
								"keyValue": map[string]interface{}{
									"backupLocation": backupLocation,
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestGetBackups(t *testing.T) {
	defaultTime, _ := time.Parse(time.RFC3339, "2022-01-01T02:03:04.52Z")
	expectedBackups := []backup{
		{name: "backup-foo", schedule: "weekly", status: "complete", time: defaultTime, backupLocation: "pg_backups/renku/renku-postgresql/2022-01-01T02:03:04.52Z/backup.sql.gz"},
		{name: "backup-bar", schedule: "daily", status: "complete", time: defaultTime, backupLocation: "pg_backups/renku/renku-postgresql/2022-01-01T02:03:04.52Z/backup.sql.gz"},
	}
	sort.Slice(expectedBackups, func(i, j int) bool { return expectedBackups[i].name < expectedBackups[j].name })
	gvr := schema.GroupVersionResource{
		Group:    "cr.kanister.io",
		Version:  "v1alpha1",
		Resource: "actionsets",
	}
	scheme := runtime.NewScheme()

	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			{Group: "cr.kanister.io", Version: "v1alpha1", Resource: "actionsets"}: "ActionSetsList",
		},
		newUnstructuredBackup("backup-foo", "kanister", "2022-01-01T02:03:04.52Z", "backup", "weekly", "complete", "pg_backups/renku/renku-postgresql/2022-01-01T02:03:04.52Z/backup.sql.gz"),
		newUnstructuredBackup("backup-bar", "kanister", "2022-01-01T02:03:04.52Z", "backup", "daily", "complete", "pg_backups/renku/renku-postgresql/2022-01-01T02:03:04.52Z/backup.sql.gz"),
		newUnstructuredBackup("backup-baz", "kanister", "2022-01-01T02:03:04.52Z", "not-a-backup", "daily", "complete", "pg_backups/renku/renku-postgresql/2022-01-01T02:03:04.52Z/backup.sql.gz"),
	)

	var backupConfig backupconfig
	backupConfig.KanisterNamespace = "kanister"
	backupConfig.Name = "daily"

	backups := getBackups(client, gvr, backupConfig)
	if len(backups) < 1 {
		t.Fatal("Empty backups")
	}
	sort.Slice(backups, func(i, j int) bool { return expectedBackups[i].name < expectedBackups[j].name })
	for i, backup := range backups {
		// Compared field by field rather than with `!=`: `backup` now carries the full
		// artifacts map, and a struct containing a map is not comparable in Go.
		expected := expectedBackups[i]
		if backup.name != expected.name ||
			backup.schedule != expected.schedule ||
			backup.status != expected.status ||
			backup.backupLocation != expected.backupLocation ||
			!backup.time.Equal(expected.time) {
			t.Fatalf("Returned backup different from the expected one.\n got: %+v\nwant: %+v", backup, expected)
		}
		// The whole artifacts map must be captured, not just backupLocation — that is
		// what lets the deletion ActionSet feed a blueprint's delete action whatever
		// artifact its backup action emitted.
		if backup.artifacts == nil {
			t.Fatalf("backup %v captured no artifacts map", backup.name)
		}
		if _, ok := backup.artifacts["cloudObject"]; !ok {
			t.Fatalf("backup %v artifacts missing cloudObject: %+v", backup.name, backup.artifacts)
		}
	}
}

// convertArtifacts is the fix for Taweret only ever understanding cloudObject: it must
// pass through any artifact name a blueprint chooses. csi-snapshot-with-options-bp emits
// `snapshotInfo`, and before this the deletion ActionSet got no artifacts at all, so every
// delete failed with `Failed to render template: "snapshotInfo" not found`.
func TestConvertArtifacts(t *testing.T) {
	t.Run("passes through a non-cloudObject artifact", func(t *testing.T) {
		got := convertArtifacts(map[string]interface{}{
			"snapshotInfo": map[string]interface{}{
				"keyValue": map[string]interface{}{
					"name":      "harbor-database",
					"namespace": "kube-system",
					"snapshots": "k8s-pvc--kube-system--harbor-database--a,k8s-pvc--kube-system--harbor-database--b",
				},
			},
		})
		artifact, ok := got["snapshotInfo"]
		if !ok {
			t.Fatalf("snapshotInfo was dropped: %+v", got)
		}
		if artifact.KeyValue["snapshots"] != "k8s-pvc--kube-system--harbor-database--a,k8s-pvc--kube-system--harbor-database--b" {
			t.Fatalf("snapshots not carried through verbatim: %+v", artifact.KeyValue)
		}
		if artifact.KeyValue["namespace"] != "kube-system" {
			t.Fatalf("namespace not carried through: %+v", artifact.KeyValue)
		}
	})

	t.Run("stringifies non-string values", func(t *testing.T) {
		got := convertArtifacts(map[string]interface{}{
			"snapshotInfo": map[string]interface{}{
				"keyValue": map[string]interface{}{"replicaCount": int64(3)},
			},
		})
		if got["snapshotInfo"].KeyValue["replicaCount"] != "3" {
			t.Fatalf("expected replicaCount stringified to \"3\", got %+v", got["snapshotInfo"].KeyValue)
		}
	})

	t.Run("keeps multiple artifacts", func(t *testing.T) {
		got := convertArtifacts(map[string]interface{}{
			"snapshotInfo": map[string]interface{}{"keyValue": map[string]interface{}{"a": "1"}},
			"cloudObject":  map[string]interface{}{"keyValue": map[string]interface{}{"backupLocation": "s3://x"}},
		})
		if len(got) != 2 {
			t.Fatalf("expected both artifacts preserved, got %+v", got)
		}
	})

	t.Run("returns nil rather than an empty map", func(t *testing.T) {
		// nil makes the caller fall back to the legacy cloudObject path instead of
		// setting an empty Artifacts map that Kanister would reject.
		if got := convertArtifacts(nil); got != nil {
			t.Fatalf("expected nil for no input, got %+v", got)
		}
		if got := convertArtifacts(map[string]interface{}{"junk": "not-a-map"}); got != nil {
			t.Fatalf("expected nil when nothing convertible, got %+v", got)
		}
		if got := convertArtifacts(map[string]interface{}{"empty": map[string]interface{}{}}); got != nil {
			t.Fatalf("expected nil for artifact with no content, got %+v", got)
		}
	})
}
