// Package memory provides an in-memory desire store.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/internal/ownercheck"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/internal/speccompare"
)

// resourceRecord stores one desire keyed by full Identity.
// Apply uses Apply+Status, Delete uses Status, and Read uses ReadStatus.
type resourceRecord struct {
	Apply         *desire.ApplySpec
	Identity      desire.Identity
	OriginID      string
	Owner         string
	TargetVersion string
	ReadStatus    desire.ReadStatus
	Status        desire.Status
	Generation    int64
	Version       int64
}

// Store is a single-process in-memory implementation of SpecStore and StatusStore.
type Store struct {
	items map[desire.Identity]*resourceRecord
	mu    sync.RWMutex
}

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		items: make(map[desire.Identity]*resourceRecord),
	}
}

// checkTargetOwner enforces the single owner-per-target rule for a Create:
// every existing sibling record must pass check.
func (s *Store) checkTargetOwner(ctx context.Context, id desire.Identity, check ownercheck.Func) error {
	for _, t := range desire.AllTypes() {
		sib := id
		sib.Type = t
		if rec, ok := s.items[sib]; ok {
			if err := check(ctx, id, rec.Owner); err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateApplyDesire creates a new ApplyDesire or returns an error.
func (s *Store) CreateApplyDesire(ctx context.Context, d desire.ApplyDesire) (desire.ApplyDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, ownercheck.OwnedBy(d.Owner)); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: create apply desire %s: %w", id, err)
	}

	deleteID := id
	deleteID.Type = desire.TypeDelete
	if deleteRecord, ok := s.items[deleteID]; ok {
		if !desire.IsDeleted(deleteRecord.Status) {
			return desire.ApplyDesire{}, desire.ErrDeletePending
		}
		// Retire the completed delete with the new apply.
		delete(s.items, deleteID)
	}
	if _, ok := s.items[id]; ok {
		return desire.ApplyDesire{}, desire.ErrAlreadyExists
	}

	spec := desire.CloneApplySpec(d.Spec)
	rec := &resourceRecord{
		Identity:   id,
		Owner:      d.Owner,
		OriginID:   d.OriginID,
		Generation: desire.InitialGeneration,
		Version:    1,
		Apply:      &spec,
	}
	s.items[id] = rec
	return s.projectApplyDesire(rec), nil
}

// GetApplyDesire retrieves an ApplyDesire or returns ErrNotFound.
func (s *Store) GetApplyDesire(ctx context.Context, id desire.Identity) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	return s.projectApplyDesire(rec), nil
}

// UpdateApplyDesireSpec updates the spec of an ApplyDesire after checking its owner.
func (s *Store) UpdateApplyDesireSpec(
	ctx context.Context, id desire.Identity, spec desire.ApplySpec, owner string, version int64,
) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if err := spec.Validate(); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: validate apply spec: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return desire.ApplyDesire{}, fmt.Errorf("desire: update apply desire spec %s: %w", id, err)
	}

	cloned := desire.CloneApplySpec(spec)
	if !speccompare.Equal(*rec.Apply, cloned) {
		rec.Generation++
	}
	rec.Apply = &cloned
	// Keep the previous outcome; a changed spec makes its observed generation
	// stale until the applier reconciles, while an identical spec does not.
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// DeleteApplyDesire deletes an ApplyDesire.
func (s *Store) DeleteApplyDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	if id.Type != desire.TypeApply {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := desire.CheckOwner(ctx, id, rec.Owner, owner); err != nil {
		return fmt.Errorf("desire: delete apply desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// CreateDeleteDesire creates a new DeleteDesire.
func (s *Store) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	return s.createDeleteDesire(ctx, d, ownercheck.OwnedBy(d.Owner))
}

func (s *Store) createDeleteDesire(
	ctx context.Context, d desire.DeleteDesire, check ownercheck.Func,
) (desire.DeleteDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: validate delete desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, check); err != nil {
		return desire.DeleteDesire{}, fmt.Errorf("desire: create delete desire %s: %w", id, err)
	}
	if _, ok := s.items[id]; ok {
		return desire.DeleteDesire{}, desire.ErrAlreadyExists
	}

	// Delete supersedes an existing Apply for the same target.
	applyID := id
	applyID.Type = desire.TypeApply
	delete(s.items, applyID)

	rec := &resourceRecord{
		Identity:   id,
		Owner:      d.Owner,
		OriginID:   d.OriginID,
		Generation: desire.InitialGeneration,
		Version:    1,
	}
	s.items[id] = rec
	return s.projectDeleteDesire(rec), nil
}

// GetDeleteDesire retrieves a DeleteDesire.
func (s *Store) GetDeleteDesire(ctx context.Context, id desire.Identity) (desire.DeleteDesire, error) {
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	return s.projectDeleteDesire(rec), nil
}

// DeleteDeleteDesire deletes a DeleteDesire.
func (s *Store) DeleteDeleteDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	return s.deleteDeleteDesire(ctx, id, version, ownercheck.OwnedBy(owner))
}

func (s *Store) deleteDeleteDesire(
	ctx context.Context, id desire.Identity, version int64, check ownercheck.Func,
) error {
	if id.Type != desire.TypeDelete {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := check(ctx, id, rec.Owner); err != nil {
		return fmt.Errorf("desire: delete delete desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// CreateReadDesire creates a new ReadDesire.
func (s *Store) CreateReadDesire(ctx context.Context, d desire.ReadDesire) (desire.ReadDesire, error) {
	if err := d.Validate(); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: validate read desire: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.Identity
	if err := s.checkTargetOwner(ctx, id, ownercheck.OwnedBy(d.Owner)); err != nil {
		return desire.ReadDesire{}, fmt.Errorf("desire: create read desire %s: %w", id, err)
	}
	if _, ok := s.items[id]; ok {
		return desire.ReadDesire{}, desire.ErrAlreadyExists
	}

	rec := &resourceRecord{
		Identity:      id,
		Owner:         d.Owner,
		OriginID:      d.OriginID,
		TargetVersion: d.TargetVersion,
		Generation:    desire.InitialGeneration,
		Version:       1,
	}
	s.items[id] = rec
	return s.projectReadDesire(rec), nil
}

// GetReadDesire retrieves a ReadDesire.
func (s *Store) GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error) {
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ReadDesire{}, desire.ErrNotFound
	}
	return s.projectReadDesire(rec), nil
}

// DeleteReadDesire deletes a ReadDesire.
func (s *Store) DeleteReadDesire(ctx context.Context, id desire.Identity, owner string, version int64) error {
	return s.deleteReadDesire(ctx, id, version, ownercheck.OwnedBy(owner))
}

func (s *Store) deleteReadDesire(
	ctx context.Context, id desire.Identity, version int64, check ownercheck.Func,
) error {
	if id.Type != desire.TypeRead {
		return desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ErrVersionConflict
	}
	if err := check(ctx, id, rec.Owner); err != nil {
		return fmt.Errorf("desire: delete read desire %s: %w", id, err)
	}

	delete(s.items, id)
	return nil
}

// ListApplyDesires returns all ApplyDesires for a management cluster.
func (s *Store) ListApplyDesires(ctx context.Context, managementCluster string) ([]desire.ApplyDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.ApplyDesire, 0, len(s.items))
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeApply {
			result = append(result, s.projectApplyDesire(rec))
		}
	}
	return result, nil
}

// ListDeleteDesires returns all DeleteDesires for a management cluster.
func (s *Store) ListDeleteDesires(ctx context.Context, managementCluster string) ([]desire.DeleteDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.DeleteDesire, 0, len(s.items))
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeDelete {
			result = append(result, s.projectDeleteDesire(rec))
		}
	}
	return result, nil
}

// ListReadDesires returns all ReadDesires for a management cluster.
func (s *Store) ListReadDesires(ctx context.Context, managementCluster string) ([]desire.ReadDesire, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]desire.ReadDesire, 0, len(s.items))
	for id, rec := range s.items {
		if id.ManagementCluster == managementCluster && id.Type == desire.TypeRead {
			result = append(result, s.projectReadDesire(rec))
		}
	}
	return result, nil
}

// DeleteByPrefix deletes desires matching a selector within a management cluster.
func (s *Store) DeleteByPrefix(ctx context.Context, managementCluster string, sel desire.PrefixSelector) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id := range s.items {
		if id.ManagementCluster != managementCluster {
			continue
		}
		if sel.Matches(id) {
			delete(s.items, id)
		}
	}
	return nil
}

// UpdateApplyDesireStatus updates the status of an ApplyDesire.
func (s *Store) UpdateApplyDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.ApplyDesire, error) {
	if id.Type != desire.TypeApply {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists || rec.Apply == nil {
		return desire.ApplyDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.ApplyDesire{}, desire.ErrVersionConflict
	}

	rec.Status = desire.CloneStatus(status)
	rec.Version++
	return s.projectApplyDesire(rec), nil
}

// UpdateDeleteDesireStatus updates the status of a DeleteDesire.
func (s *Store) UpdateDeleteDesireStatus(
	ctx context.Context, id desire.Identity, status desire.Status, version int64,
) (desire.DeleteDesire, error) {
	if id.Type != desire.TypeDelete {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.DeleteDesire{}, desire.ErrNotFound
	}
	if rec.Version != version {
		return desire.DeleteDesire{}, desire.ErrVersionConflict
	}

	rec.Status = desire.CloneStatus(status)
	rec.Version++
	return s.projectDeleteDesire(rec), nil
}

// UpdateReadDesireStatus updates the status of a ReadDesire.
func (s *Store) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	if id.Type != desire.TypeRead {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.items[id]
	if !exists {
		return desire.ReadDesire{}, desire.ErrNotFound
	}

	rec.ReadStatus = desire.CloneReadStatus(status)
	return s.projectReadDesire(rec), nil
}

func (s *Store) projectApplyDesire(rec *resourceRecord) desire.ApplyDesire {
	if rec == nil || rec.Apply == nil {
		return desire.ApplyDesire{}
	}
	return desire.ApplyDesire{
		Identity:   rec.Identity,
		Owner:      rec.Owner,
		OriginID:   rec.OriginID,
		Generation: rec.Generation,
		Version:    rec.Version,
		Spec:       desire.CloneApplySpec(*rec.Apply),
		Status:     desire.CloneStatus(rec.Status),
	}
}

func (s *Store) projectDeleteDesire(rec *resourceRecord) desire.DeleteDesire {
	if rec == nil {
		return desire.DeleteDesire{}
	}
	return desire.DeleteDesire{
		Identity:   rec.Identity,
		Owner:      rec.Owner,
		OriginID:   rec.OriginID,
		Generation: rec.Generation,
		Version:    rec.Version,
		Status:     desire.CloneStatus(rec.Status),
	}
}

func (s *Store) projectReadDesire(rec *resourceRecord) desire.ReadDesire {
	if rec == nil {
		return desire.ReadDesire{}
	}
	return desire.ReadDesire{
		Identity:      rec.Identity,
		Owner:         rec.Owner,
		OriginID:      rec.OriginID,
		TargetVersion: rec.TargetVersion,
		Generation:    rec.Generation,
		Version:       rec.Version,
		Status:        desire.CloneReadStatus(rec.ReadStatus),
	}
}

// Sweeper returns a SweeperStore that shares this store's state. *Store does
// not implement SweeperStore, so a SpecStore holder cannot reach it by type
// assertion.
func (s *Store) Sweeper() desire.SweeperStore {
	return sweeper{s}
}

type sweeper struct{ s *Store }

// CreateDeleteDesire is Store.CreateDeleteDesire with ownercheck.AnyOwner.
func (w sweeper) CreateDeleteDesire(ctx context.Context, d desire.DeleteDesire) (desire.DeleteDesire, error) {
	return w.s.createDeleteDesire(ctx, d, ownercheck.AnyOwner)
}

// DeleteDeleteDesire is Store.DeleteDeleteDesire with ownercheck.AnyOwner.
func (w sweeper) DeleteDeleteDesire(ctx context.Context, id desire.Identity, version int64) error {
	return w.s.deleteDeleteDesire(ctx, id, version, ownercheck.AnyOwner)
}

// DeleteReadDesire is Store.DeleteReadDesire with ownercheck.AnyOwner.
func (w sweeper) DeleteReadDesire(ctx context.Context, id desire.Identity, version int64) error {
	return w.s.deleteReadDesire(ctx, id, version, ownercheck.AnyOwner)
}
