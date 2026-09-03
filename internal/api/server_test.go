package api

import "testing"

func TestValidateCreateDefaults(t *testing.T) {
	r := createClusterRequest{Name: "Search-Prod"}
	if err := validateCreate(&r); err != nil {
		t.Fatalf("validateCreate: %v", err)
	}
	if r.Name != "search-prod" || r.Engine != "opensearch" || r.Version != "3.8.0" || r.Nodes != 1 {
		t.Fatalf("unexpected defaults: %+v", r)
	}
}

func TestValidateCreateRejectsTooManyNodes(t *testing.T) {
	r := createClusterRequest{Name: "search-prod", Nodes: 4}
	if err := validateCreate(&r); err == nil {
		t.Fatal("expected validation error")
	}
}
