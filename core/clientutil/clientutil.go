package clientutil

import (
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func CreateKubeClient(kubeConfigPath string, poolSize int) (*kubernetes.Clientset, error) {
	clientConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, err
	}
	clientConfig.QPS = float32(poolSize)
	clientConfig.Burst = poolSize
	clientSet, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, err
	}
	return clientSet, nil
}

func CreateDynamicAndDiscoveryClients(kubeConfigPath string, poolSize int) (dynamic.Interface, *discovery.DiscoveryClient, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, nil, err
	}
	config.QPS = float32(poolSize)
	config.Burst = poolSize
	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, nil, err
	}
	return dyn, disc, nil
}
