// Package data provides data types for readers. All data types for readers are
// packages inside an Entry. This allows for a single channel to be used for all
// data types.
package data

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var (
	// ErrInvalidType is returned when the type is invalid.
	ErrInvalidType = errors.New("invalid type")
)

// SourceData is a generic type for objects wrappers in this package.
type SourceData interface {
	// GetUID returns the UID of the object.
	GetUID() types.UID
	// Object returns the object as a runtime.Object. This is always for the latest change,
	// in the case that this is an update.
	Object() runtime.Object
}

//go:generate stringer -type=EntryType -linecomment

type EntryType uint8

const (
	//
	ETUnknown  EntryType = 0 // Unknown
	ETInformer EntryType = 1 // Informer
)

// Entry is a data entry.
// This is field aligned for better performance.
type Entry struct {
	// data holds the data.
	data SourceData

	// Type is the type of the entry.
	Type EntryType
}

// NewEntry creates a new Entry. ObjectMeta is implemented by Informer.
func NewEntry(data SourceData) (Entry, error) {
	if data == nil {
		return Entry{}, ErrInvalidType
	}

	switch data.(type) {
	case Informer:
		return Entry{data: data, Type: ETInformer}, nil
	}
	return Entry{}, ErrInvalidType
}

// UID returns the UID of the underlying object. This is always the latest change.
func (e Entry) UID() types.UID {
	if e.data == nil {
		return types.UID("")
	}
	return e.data.GetUID()
}

// Object returns the data as a runtime.Object. This is always for the latest change.
func (e Entry) Object() runtime.Object {
	return e.data.Object()
}

// Informer returns the entry data as an Informer. An error is returned if the type is not Informer.
func (e Entry) Informer() (Informer, error) {
	if e.Type != ETInformer {
		return Informer{}, ErrInvalidType
	}
	if e.data == nil {
		return Informer{}, ErrInvalidType
	}
	v, ok := e.data.(Informer)
	if !ok {
		return Informer{}, ErrInvalidType
	}
	return v, nil
}

//go:generate stringer -type=ObjectType -linecomment

// ObjectType is the type of the object held in a type.
type ObjectType uint8

const (
	// OTUnknown indicates a bug in the code.
	OTUnknown ObjectType = 0 // Unknown
	// OTNode indicates the data is a node.
	OTNode ObjectType = 1 // Node
	// OTPod indicates the data is a pod.
	OTPod ObjectType = 2 // Pod
	// OTNamespace indicates the data is a namespace.
	OTNamespace ObjectType = 3 // Namespace
)

// Informer is data from an APIServer informer. This implementes SourceData.
// Note: This data type is field aligned for better performance.
type Informer struct {
	data any
	uid  types.UID
	// Type is the type of the data.
	Type ObjectType
}

// NewInformer creates a new Informer. Data must be a Change type.
func NewInformer[T K8Object](change Change[T]) (Informer, error) {
	uid, err := change.UID()
	if err != nil {
		return Informer{}, err
	}

	return Informer{data: change, uid: uid, Type: change.ObjectType}, nil
}

// GetUID returns the UID of the underlying object.
func (i Informer) GetUID() types.UID {
	return i.uid
}

// Object returns the data as a runtime.Object. This is always for latest change, in the case that this
// is an update. This returns nil if the object is of a type we don't understand.
func (i Informer) Object() runtime.Object {
	switch v := i.data.(type) {
	case Change[*corev1.Node]:
		if v.ChangeType == CTDelete {
			return v.Old
		}
		return v.New
	case Change[*corev1.Pod]:
		if v.ChangeType == CTDelete {
			return v.Old
		}
		return v.New
	case Change[*corev1.Namespace]:
		if v.ChangeType == CTDelete {
			return v.Old
		}
		return v.New
	}
	return nil
}

// Node returns the data for a Node type change. An error is returned if the type is not Node.
func (i Informer) Node() (Change[*corev1.Node], error) {
	if i.data == nil {
		return Change[*corev1.Node]{}, ErrInvalidType
	}

	v, ok := i.data.(Change[*corev1.Node])
	if !ok {
		return Change[*corev1.Node]{}, ErrInvalidType
	}
	return v, nil
}

// Pod returns the data a pod type change. An error is returned if the type is not Pod.
func (i Informer) Pod() (Change[*corev1.Pod], error) {
	if i.data == nil {
		return Change[*corev1.Pod]{}, ErrInvalidType
	}

	v, ok := i.data.(Change[*corev1.Pod])
	if !ok {
		return Change[*corev1.Pod]{}, ErrInvalidType
	}

	return v, nil
}

// Namespace returns the data as a namespace type change. An error is returned if the type is not Namespace.
func (i Informer) Namespace() (Change[*corev1.Namespace], error) {
	if i.data == nil {
		return Change[*corev1.Namespace]{}, ErrInvalidType
	}

	v, ok := i.data.(Change[*corev1.Namespace])
	if !ok {
		return Change[*corev1.Namespace]{}, ErrInvalidType
	}

	return v, nil
}

// ChangeType is the type of change.
type ChangeType uint8

const (
	// CTUnknown indicates a bug in the code.
	CTUnknown ChangeType = 0
	// CTAdd indicates the data was added.
	CTAdd ChangeType = 1
	// CTUpdate indicates the data was updated.
	CTUpdate ChangeType = 2
	// CTDelete indicates the data was deleted.
	CTDelete ChangeType = 3
)

// K8Object is implemented by all Kubernetes objects.
type K8Object interface {
	runtime.Object

	// GetUID returns the UID of the object.
	GetUID() types.UID
}

// Change is a change made to a data set.
// Note: This data type is field aligned for better performance.
type Change[T K8Object] struct {
	// Old is the old data. This is only valid if Type is Update or Delete.
	Old T
	// New is the new data. This is only valid if Type is Add or Update.
	New T
	// ChangeType is the type of the change.
	ChangeType ChangeType
	// ObjectType is the type of the object.
	ObjectType ObjectType
}

// UID returns the UID of the underlying object being changed.
func (c Change[T]) UID() (types.UID, error) {
	switch c.ChangeType {
	case CTAdd:
		return c.New.GetUID(), nil
	case CTUpdate:
		return c.New.GetUID(), nil
	case CTDelete:
		return c.Old.GetUID(), nil
	}
	return types.UID(""), fmt.Errorf("unknown ChangeType: %v", c.ChangeType)
}
