package model

import "testing"

func TestClusterLifecycleTransitions(t *testing.T) {
	cases := []struct {
		name    string
		from    ClusterStatus
		to      ClusterStatus
		allowed bool
	}{
		{name: "provision requested cluster", from: StatusRequested, to: StatusProvisioning, allowed: true},
		{name: "finish provisioning", from: StatusProvisioning, to: StatusRunning, allowed: true},
		{name: "fail provisioning", from: StatusProvisioning, to: StatusFailed, allowed: true},
		{name: "start scaling", from: StatusRunning, to: StatusScaling, allowed: true},
		{name: "finish scaling", from: StatusScaling, to: StatusRunning, allowed: true},
		{name: "start deletion", from: StatusRunning, to: StatusDeleting, allowed: true},
		{name: "clean up failed cluster", from: StatusFailed, to: StatusDeleting, allowed: true},
		{name: "finish deletion", from: StatusDeleting, to: StatusDeleted, allowed: true},
		{name: "skip provisioning", from: StatusRequested, to: StatusRunning, allowed: false},
		{name: "scale while provisioning", from: StatusProvisioning, to: StatusScaling, allowed: false},
		{name: "restore deleted cluster", from: StatusDeleted, to: StatusRunning, allowed: false},
		{name: "repeat state", from: StatusRunning, to: StatusRunning, allowed: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if actual := CanTransition(tc.from, tc.to); actual != tc.allowed {
				t.Fatalf("CanTransition(%s, %s) = %t, want %t", tc.from, tc.to, actual, tc.allowed)
			}
		})
	}
}
