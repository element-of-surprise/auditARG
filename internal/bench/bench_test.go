package benchme

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var nodeData corev1.Node
var podData corev1.Pod
var namespaceData corev1.Namespace

var data0 string

func BenchmarkData0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		d := Data0{
			Node: corev1.Node{
				TypeMeta: metav1.TypeMeta{
					Kind: "Node",
				},
			},
			Type: DTNode,
		}

		switch d.Type {
		case DTNode:
			nodeData = d.Node
			data0 = nodeData.TypeMeta.Kind
		case DTPod:
			podData = d.Pod
		case DTNamespace:
			namespaceData = d.Namespace
		}
	}
}

var data1 string

func BenchmarkData1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		d := Data1{
			Element: corev1.Node{
				TypeMeta: metav1.TypeMeta{
					Kind: "Node",
				},
			},
			Type: DTNode,
		}

		switch d.Type {
		case DTNode:
			nodeData = d.Element.(corev1.Node)
			data1 = nodeData.TypeMeta.Kind
		case DTPod:
			podData = d.Element.(corev1.Pod)
		case DTNamespace:
			namespaceData = d.Element.(corev1.Namespace)
		}
	}
}
