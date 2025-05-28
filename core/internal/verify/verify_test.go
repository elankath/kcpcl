package main

import (
	"context"
	"github.com/elankath/kcpcl/core/clientutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log/slog"
	"os"
	"testing"
)

func TestCreateKubeClientKine(t *testing.T) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		t.Fatal("KUBECONFIG environment variable not set")
		return
	}
	slog.Info("USING KUBECONFIG", "kubeconfig", kubeconfig)
	fi, err := os.Stat(kubeconfig)
	if err != nil {
		t.Error(err)
		return
	}
	slog.Info("KUBECONFIG size", "size", fi.Size())
	client, err := clientutil.CreateKubeClient(kubeconfig, 20)
	if err != nil {
		t.Error(err)
		return
	}
	ctx := context.Background()
	nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("nodeList: %v", nodeList)
}
