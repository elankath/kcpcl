package clientutil

import (
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func CreateKubeClient(kubeConfigPath string, poolSize int) (*kubernetes.Clientset, error) {
	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}
	restCfg.QPS = float32(poolSize)
	restCfg.QPS = float32(poolSize) * 1.5
	restCfg.Burst = poolSize
	clientSet, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	return clientSet, nil
}

func CreateDynamicAndDiscoveryClients(kubeConfigPath string, poolSize int) (dynamic.Interface, *discovery.DiscoveryClient, error) {
	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, nil, err
	}
	restCfg.QPS = float32(poolSize) * 1.5
	restCfg.Burst = poolSize
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, nil, err
	}
	return dyn, disc, nil
}
