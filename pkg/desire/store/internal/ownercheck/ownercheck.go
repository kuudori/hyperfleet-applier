// Package ownercheck holds the owner policies that store backends apply inside
// their critical section.
package ownercheck

import (
	"context"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// Func returns nil when the caller may act on a target whose owner is current.
type Func func(ctx context.Context, id desire.Identity, current string) error

// OwnedBy accepts only a target that owner owns. It reports a mismatch through
// desire.CheckOwner.
func OwnedBy(owner string) Func {
	return func(ctx context.Context, id desire.Identity, current string) error {
		return desire.CheckOwner(ctx, id, current, owner)
	}
}

// AnyOwner accepts a target whatever its owner. Only the sweeper uses it.
func AnyOwner(context.Context, desire.Identity, string) error {
	return nil
}
