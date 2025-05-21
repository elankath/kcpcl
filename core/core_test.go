package core

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"testing"
)

func TestCountKinds(t *testing.T) {
	ns1 := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind: "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ns-a",
		},
	}
	ns2 := ns1
	ns2.Name = "ns-b"

	pod1 := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind: "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "po-a",
		},
	}
	pod2 := pod1
	pod2.Name = "po-b"

	pod3 := pod2
	pod3.Name = "po-c"

	no1 := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			Kind: "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "no-a",
		},
	}
	no2 := no1
	no2.Name = "no-b"

	runtimeObjs := []runtime.Object{
		runtime.Object(ns1),
		runtime.Object(ns2),
		runtime.Object(pod1),
		runtime.Object(pod2),
		runtime.Object(pod3),
		runtime.Object(no1),
		runtime.Object(no2),
	}

	var objs []*unstructured.Unstructured
	for _, ns := range runtimeObjs {
		objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&ns)
		if err != nil {
			t.Error(err)
			return
		}
		objs = append(objs, &unstructured.Unstructured{Object: objMap})
	}
	kindCount := getNumKinds(objs)
	t.Logf("kindCount: %d", kindCount)
	expectedKindCount := 3
	if kindCount != expectedKindCount {
		t.Errorf("expected %d kinds, got %d", expectedKindCount, kindCount)
	}
}
