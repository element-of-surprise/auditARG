package benchme

import (
	corev1 "k8s.io/api/core/v1"
)

// DataType is the type of the data.
type DataType uint8

const (
	// DTUnknown indicates a bug in the code.
	DTUnknown DataType = 0
	// DTNode indicates the data is a node.
	DTNode DataType = 1
	// DTPod indicates the data is a pod.
	DTPod DataType = 2
	// DTNamespace indicates the data is a namespace.
	DTNamespace DataType = 3
)

type Data0 struct {
	// Node is the node data. This is only valid if Type is Node.
	Node corev1.Node
	// Pod is the pod data. This is only valid if Type is Pod.
	Pod corev1.Pod
	// Namespace is the namespace data. This is only valid if Type is Namespace.
	Namespace corev1.Namespace

	// Type is the type of the data.
	Type DataType
}

type Data1 struct {
	Element any
	// Type is the type of the data.
	Type DataType
}
