package core

import (
	"cmp"
	"context"
	"fmt"
	"github.com/alitto/pond/v2"
	"github.com/elankath/kcpcl/api"
	clientutil "github.com/elankath/kcpcl/core/clientutil"
	"io/fs"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"maps"
	"math"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//"github.com/mitchellh/go-homedir"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"log/slog"
)

var (
	_            api.ShootCopier = (*GardenerShootCopier)(nil)
	schemeAdders                 = []func(scheme *runtime.Scheme) error{
		metav1.AddMetaToScheme,
		corev1.AddToScheme,
		appsv1.AddToScheme,
		coordinationv1.AddToScheme,
		eventsv1.AddToScheme,
		rbacv1.AddToScheme,
		schedulingv1.AddToScheme,
		policyv1.AddToScheme,
		storagev1.AddToScheme,
	}
	SupportedScheme      = CreateRegisterScheme()
	APIResourcesFilename = "api-resources.yaml"
)

type GardenerShootCopier struct {
	cfg             api.CopierConfig
	gardenClient    *kubernetes.Clientset
	dynamicClient   dynamic.Interface
	discoveryClient *discovery.DiscoveryClient
	targetClient    *kubernetes.Clientset
	pool            pond.Pool
}

func NewShootCopierFromConfig(copyCfg api.CopierConfig) (copier api.ShootCopier, err error) {
	var gsc GardenerShootCopier
	gsc.cfg = copyCfg
	gsc.dynamicClient, gsc.discoveryClient, err = clientutil.CreateDynamicAndDiscoveryClients(copyCfg.KubeConfigPath, copyCfg.PoolSize)
	if err != nil {
		err = fmt.Errorf("%w: cannot create kube clients from %q: %w", api.ErrCreateKubeClient, copyCfg.KubeConfigPath, err)
		return
	}
	gsc.pool = pond.NewPool(copyCfg.PoolSize)
	copier = &gsc
	return
}

func (g *GardenerShootCopier) DownloadObjects(ctx context.Context, baseObjDir string, gvrList []schema.GroupVersionResource) error {
	slog.Info("Downloading objects")
	apiGroupResources, err := restmapper.GetAPIGroupResources(g.discoveryClient)
	if err != nil {
		return fmt.Errorf("%w: failed to fetch API group resources: %w", api.ErrDiscovery, err)
	}

	//err = os.MkdirAll(baseObjDir, 0755)
	//if err != nil {
	//	return fmt.Errorf("%w: failed to create directory %q: %w", api.ErrDownloadFailed, baseObjDir, err)
	//}
	//err = writeAPIResources(baseObjDir, toAPIResources(apiGroupResources))
	//if err != nil {
	//	return fmt.Errorf("%w: %w", api.ErrDownloadFailed, err)
	//}

	err = ValidateGVRs(apiGroupResources, gvrList)

	if err != nil {
		return fmt.Errorf("%w: %w", api.ErrDownloadFailed, err)
	}

	allNamespaces, err := getAllNamespaces(ctx, g.dynamicClient)
	if err != nil {
		return err
	}

	taskGroup := g.pool.NewGroupContext(ctx)

	var isNamespaced bool
	for _, gvr := range gvrList {
		isNamespaced, err = isNamespacedResource(apiGroupResources, gvr)
		if err != nil {
			return fmt.Errorf("%w: %w", api.ErrDiscovery, err)
		}
		resourceDir := filepath.Join(baseObjDir, gvr.Group+"-"+gvr.Version+"-"+gvr.Resource)
		err = os.MkdirAll(resourceDir, 0755)
		if err != nil {
			return fmt.Errorf("%w: failed to create directory %q: %w", api.ErrDownloadFailed, resourceDir, err)
		}

		if isNamespaced {
			for _, ns := range allNamespaces {
				taskGroup.SubmitErr(func() error {
					objList, err := g.dynamicClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
					if err != nil {
						err = fmt.Errorf("%w: failed to list objects for gvr %q in namespace %q: %w", api.ErrDownloadFailed, gvr, ns, err)
						return err
					}
					err = writeObjectList(objList, resourceDir, ns)
					if err != nil {
						return err
					}
					return nil
				})
			}
		} else {
			taskGroup.SubmitErr(func() error {
				objList, err := g.dynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
				if err != nil {
					err = fmt.Errorf("%w: failed to list objects for gvr %q: %w", api.ErrDownloadFailed, gvr, err)
					return err
				}
				err = writeObjectList(objList, resourceDir, "")
				if err != nil {
					return err
				}
				return nil
			})
		}
	}
	return taskGroup.Wait()
}

func (g *GardenerShootCopier) UploadObjects(ctx context.Context, baseObjDir string) (err error) {
	begin := time.Now()

	allObjs, err := loadObjects(baseObjDir, g.pool.NewGroupContext(ctx))
	if err != nil {
		err = fmt.Errorf("%w: failed to load objects: %w", api.ErrUploadFailed, err)
		return
	}
	apiGroupResources, err := restmapper.GetAPIGroupResources(g.discoveryClient)
	if err != nil {
		return fmt.Errorf("%w: failed to fetch API group resources: %w", api.ErrDiscovery, err)
	}

	objChunks := chunkObjectsByPriority(allObjs, toAPIResources(apiGroupResources))
	slog.Info("Grouped upload objects into chunks by priority.", "numObjs", len(allObjs), "numObjChunks", len(objChunks))
	uploadCounter := &atomic.Uint32{}

	mapper := restmapper.NewDiscoveryRESTMapper(apiGroupResources)
	var kindUploaders = make(map[string]*KindUploader)
	for _, o := range allObjs {
		oKind := o.GetKind()
		uploader, ok := kindUploaders[oKind]
		if ok {
			continue
		}
		gvk := o.GroupVersionKind()
		var restMapping *meta.RESTMapping
		restMapping, err = mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			err = fmt.Errorf("%w: failed to fetch REST mapping for %q: %w", api.ErrDiscovery, gvk, err)
			return
		}
		gvr := restMapping.Resource
		resourceFacade := g.dynamicClient.Resource(gvr)
		uploader = &KindUploader{
			GVK:            gvk,
			GVR:            gvr,
			ResourceFacade: resourceFacade,
			Counter:        uploadCounter,
		}
		kindUploaders[oKind] = uploader
	}

	var pods []*unstructured.Unstructured
	for i, objs := range objChunks {
		chunkTask := g.pool.NewGroupContext(ctx)
		slices.SortFunc(objs, func(a, b *unstructured.Unstructured) int {
			return a.GetCreationTimestamp().Compare(b.GetCreationTimestamp().Time)
		})
		for _, o := range objs {
			u := kindUploaders[o.GetKind()]
			if o.GetKind() == "Pod" {
				pods = append(pods, o)
			} else {
				u.UploadAsync(ctx, chunkTask, o)
			}
		}
		err := chunkTask.Wait()
		if err != nil {
			return fmt.Errorf("%w: failed to upload chunk %d: %w", api.ErrUploadFailed, i, err)
		}
		slog.Info("completed upload chunk", "chunkIndex", i, "numObjs", len(objs), "uploadCounter", uploadCounter.Load())
	}

	slices.SortFunc(pods, func(a, b *unstructured.Unstructured) int {
		return a.GetCreationTimestamp().Compare(b.GetCreationTimestamp().Time)
	})
	for i, p := range pods {
		podKey := cache.NewObjectName(p.GetNamespace(), p.GetName()).String()
		slog.Info("Pod upload", "num", i, "podKey", podKey, "creationTimestamp", p.GetCreationTimestamp().Time)
		u := kindUploaders[p.GetKind()]
		err = u.Upload(ctx, p)
		if err != nil {
			return fmt.Errorf("%w: failed to upload object %q, index: %d: %w", api.ErrUploadFailed, podKey, err)
		}
	}

	end := time.Now()
	slog.Info("UploadObjects time taken", "duration", end.Sub(begin), "totalUploadCount", uploadCounter.Load())
	return
}

type KindUploader struct {
	GVK            schema.GroupVersionKind
	GVR            schema.GroupVersionResource
	ResourceFacade dynamic.NamespaceableResourceInterface
	Counter        *atomic.Uint32
}

func (u *KindUploader) UploadAsync(ctx context.Context, taskGroup pond.TaskGroup, obj *unstructured.Unstructured) {
	slog.Debug("Commencing upload for obj", "kind", u.GVK.Kind, "objName", obj.GetName(), "objNamespace", obj.GetNamespace())
	taskGroup.SubmitErr(func() error {
		return u.Upload(ctx, obj)
	})
}

func (u *KindUploader) Upload(ctx context.Context, obj *unstructured.Unstructured) error {
	slog.Debug("Commencing upload for obj", "kind", u.GVK.Kind, "objName", obj.GetName(), "objNamespace", obj.GetNamespace())
	var ri dynamic.ResourceInterface = u.ResourceFacade
	if obj.GetNamespace() != "" {
		ri = u.ResourceFacade.Namespace(obj.GetNamespace())
	}
	createdObj, err := ri.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			slog.Warn("object already exists, skipping upload.", "kind", obj.GetKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())
			return nil
		}
		if errors.IsForbidden(err) {
			slog.Warn("object creation forbidden.", "name", obj.GetName(), "namespace", obj.GetNamespace(), "error", err)
			return nil
		}
		err = fmt.Errorf("failed to create obj of kind %q, name %q and namespace %q: %w",
			obj.GetKind(), obj.GetName(), obj.GetNamespace(), err)
		return err
	}
	if u.Counter.Load()%3000 == 0 {
		slog.Info("object created", "uploadCount", u.Counter.Load(), "kind", createdObj.GetKind(), "name", createdObj.GetName(), "namespace", createdObj.GetNamespace())
	} else {
		slog.Debug("object created", "uploadCount", u.Counter.Load(), "kind", createdObj.GetKind(), "name", createdObj.GetName(), "namespace", createdObj.GetNamespace())
	}
	u.Counter.Add(1)
	return nil
}

func loadObjects(baseObjDir string, loadTaskGroup pond.TaskGroup) ([]*unstructured.Unstructured, error) {
	slog.Info("Loading objects.", "baseObjDir", baseObjDir)
	objCount := 0
	var loadMutex sync.Mutex
	var objs = make([]*unstructured.Unstructured, 0, 3000)
	err := filepath.WalkDir(baseObjDir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%w: path error for %q: %w", api.ErrLoadObj, path, err)
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			return nil
		}
		// Infer GVR from parent directory
		resourcesDirName := filepath.Base(filepath.Dir(path))
		parts := strings.SplitN(resourcesDirName, "-", 3)
		if len(parts) != 3 {
			err = fmt.Errorf("%w: invalid object resourcesDirName: %s", api.ErrLoadObj, resourcesDirName)
			return err
		}
		loadTaskGroup.SubmitErr(func() error {
			var obj *unstructured.Unstructured
			obj, err := LoadAndCleanObj(path)
			if err != nil {
				return err
			}
			loadMutex.Lock()
			defer loadMutex.Unlock()
			objs = append(objs, obj)
			objCount++
			if objCount%2000 == 0 {
				slog.Info("Loaded object", "objCount", objCount, "path", path)
			} else {
				slog.Debug("Loaded object", "objCount", objCount, "path", path)
			}
			return nil
		})
		return nil
	})
	err = loadTaskGroup.Wait()
	if err != nil {
		return nil, err
	}
	slog.Info("Loaded total objects", "objCount", objCount, "baseObjDir", baseObjDir)
	return objs, nil
}

func LoadAndCleanObj(objPath string) (obj *unstructured.Unstructured, err error) {
	data, err := os.ReadFile(objPath)
	if err != nil {
		err = fmt.Errorf("%w: failed to read %q: %w", api.ErrLoadObj, objPath, err)
		return
	}
	obj = &unstructured.Unstructured{}
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		err = fmt.Errorf("%w: failed to convert YAML to JSON for %q: %w", api.ErrLoadObj, objPath, err)
		return
	}
	err = obj.UnmarshalJSON(jsonData)
	if err != nil {
		err = fmt.Errorf("%w: failed to unmarshal object in %q: %w", api.ErrLoadObj, objPath, err)
		return
	}
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	//unstructured.RemoveNestedField(obj.Object, "metadata", "uid")
	//unstructured.RemoveNestedField(obj.Object, "metadata", "generation")
	obj.SetManagedFields(nil)
	//unstructured.RemoveNestedField(obj.Object, "status")
	obj.SetGeneration(0)

	if obj.GetKind() != "Pod" {
		return
	}
	//TODO: Make this configurable via flag

	err = unstructured.SetNestedField(obj.Object, "", "spec", "nodeName")
	if err != nil {
		err = fmt.Errorf("%w: cannot clear spec.nodeName for pod %q: %w", api.ErrLoadObj, obj.GetName(), err)
		return
	}
	// TODO: make an option to set the scheduler name
	//err = unstructured.SetNestedField(obj.Object, "default-scheduler", "spec", "schedulerName")
	//if err != nil {
	//	err = fmt.Errorf("%w: cannot set default-scheduler for pod %q: %w", api.ErrLoadObj, obj.GetName(), err)
	//	return
	//}
	return
}

func writeObjectList(objList *unstructured.UnstructuredList, resourceDir string, ns string) (err error) {
	var filename string
	for _, obj := range objList.Items {
		if ns != "" {
			filename = filepath.Join(resourceDir, sanitizeFileName(ns+"@"+obj.GetName())+".yaml")
		} else {
			filename = filepath.Join(resourceDir, sanitizeFileName(obj.GetName())+".yaml")
		}
		err = writeObjToYAMLFile(filename, &obj)
		if err != nil {
			return fmt.Errorf("%w: %w", api.ErrDownloadFailed, err)
		}
		slog.Info("Downloaded object", "filename", filename)
	}
	return
}

func getAllNamespaces(ctx context.Context, dc dynamic.Interface) ([]string, error) {
	namespaceList, err := dc.Resource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: failed to list namespaces: %w", api.ErrDownloadFailed, err)
	}
	var allNamespaces []string
	for _, ns := range namespaceList.Items {
		allNamespaces = append(allNamespaces, ns.GetName())
	}
	return allNamespaces, nil
}

func (g *GardenerShootCopier) GetConfig() api.CopierConfig {
	return g.cfg
}

func (g *GardenerShootCopier) GetClient() dynamic.Interface {
	return g.dynamicClient
}

func (g *GardenerShootCopier) GetShootCoordinate() api.ShootCoords {
	//TODO implement me
	panic("implement me")
}

func (g *GardenerShootCopier) GetSeedCoordinate() api.ShootCoords {
	//TODO implement me
	panic("implement me")
}

func CreateRegisterScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	return RegisterToScheme(scheme)

}
func RegisterToScheme(scheme *runtime.Scheme) *runtime.Scheme {
	for _, fn := range schemeAdders {
		utilruntime.Must(fn(scheme))
	}
	return scheme
}

func ValidateGVRs(groupResources []*restmapper.APIGroupResources, gvrs []schema.GroupVersionResource) error {
	// Build a lookup map of all available GVRs
	supported := make(map[schema.GroupVersionResource]struct{})
	for _, group := range groupResources {
		groupName := group.Group.Name
		for version, resources := range group.VersionedResources {
			for _, res := range resources {
				gvr := schema.GroupVersionResource{
					Group:    groupName,
					Version:  version,
					Resource: res.Name,
				}
				supported[gvr] = struct{}{}
			}
		}
	}

	for _, gvr := range gvrs {
		if _, ok := supported[gvr]; ok {
			continue
		}
		return fmt.Errorf("%w: resource not found in API: %s", api.ErrNotFoundGVR, gvr.String())
	}
	slog.Info("GVR validation successful", "GVRs", gvrs)

	return nil

}

func isNamespacedResource(apiGroupResources []*restmapper.APIGroupResources, gvr schema.GroupVersionResource) (bool, error) {
	for _, agr := range apiGroupResources {
		if agr.Group.Name != gvr.Group {
			continue
		}
		apiResources := agr.VersionedResources[gvr.Version]
		for _, res := range apiResources {
			if res.Name == gvr.Resource {
				return res.Namespaced, nil
			}
		}
	}
	return false, fmt.Errorf("%w: resource %s not found in discovery for group/version %q", api.ErrNotFoundGVR, gvr.Resource, gvr.GroupVersion().String())
}

func sanitizeFileName(name string) string {
	return strings.ReplaceAll(name, "/", "__")
}

func toAPIResources(apiGroupResources []*restmapper.APIGroupResources) (allAPIResources []metav1.APIResource) {
	for _, agr := range apiGroupResources {
		prefVersion := agr.Group.PreferredVersion.Version
		var groupAPIResources []metav1.APIResource
		if prefVersion != "" {
			groupAPIResources = agr.VersionedResources[prefVersion]
		} else {
			iter := maps.Values(agr.VersionedResources)
			for p := range iter {
				groupAPIResources = p
				break //pick first
			}
		}
		allAPIResources = append(allAPIResources, groupAPIResources...)
	}
	return
}

func writeAPIResources(objBaseDir string, apiResources []metav1.APIResource) error {
	path := filepath.Join(objBaseDir, APIResourcesFilename)
	apiResourceList := metav1.APIResourceList{APIResources: apiResources}
	err := writeValueToYAMLFile(path, apiResourceList)
	if err != nil {
		err = fmt.Errorf("%w: cannot write apiResourceList to path %q: %w", api.ErrSaveObj, path, err)
	}
	slog.Info("Wrote APIResourceList to path.", "path", path, "numAPIResources", len(apiResourceList.APIResources))
	return err
}
func loadAPIResources(objBaseDir string) (apiResources []metav1.APIResource, err error) {
	path := filepath.Join(objBaseDir, APIResourcesFilename)
	apiResourceList := metav1.APIResourceList{}
	err = loadValueFromYAMLFile(path, &apiResourceList)
	if err != nil {
		return
	}
	apiResources = apiResourceList.APIResources
	return
}
func writeObjToYAMLFile(path string, obj *unstructured.Unstructured) error {
	err := writeValueToYAMLFile(path, obj.Object)
	if err != nil {
		err = fmt.Errorf("%w: cannot write yaml file %q for obj named %q in namespace %q: %w", api.ErrSaveObj, path, obj.GetName(), obj.GetNamespace(), err)
	}
	return err
}

func writeValueToYAMLFile(path string, val any) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	yamlData, err := yaml.JSONToYAML(data)
	if err != nil {
		return err
	}
	err = os.WriteFile(path, yamlData, 0644)
	if err != nil {
		return err
	}
	return nil
}

func loadValueFromYAMLFile(path string, obj any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		err = fmt.Errorf("%w: failed to read %q: %w", api.ErrLoadObj, path, err)
		return err
	}
	err = yaml.Unmarshal(data, obj)
	if err != nil {
		err = fmt.Errorf("%w: failed to unmarshal YAML %q: %w", api.ErrLoadObj, path, err)
	}
	return err
}

func chunkObjectsByPriority(objs []*unstructured.Unstructured, apiResources []metav1.APIResource) (chunks [][]*unstructured.Unstructured) {
	apiResourcesByKind := make(map[string]metav1.APIResource)
	for _, ar := range apiResources {
		apiResourcesByKind[ar.Kind] = ar
	}
	slices.SortFunc(objs, func(a, b *unstructured.Unstructured) int {
		return cmp.Compare(getObjPriority(apiResourcesByKind, a), getObjPriority(apiResourcesByKind, b))
	})
	currPrio := getObjPriority(apiResourcesByKind, objs[0])
	var chunk []*unstructured.Unstructured
	for _, o := range objs {
		oPrio := getObjPriority(apiResourcesByKind, o)
		if currPrio != oPrio { //when priority changes then add chunk to chunks and reset chunk
			currPrio = oPrio
			chunks = append(chunks, chunk)
			chunk = nil
		}
		chunk = append(chunk, o)
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	return
}

func getObjPriority(apiResourcesByKind map[string]metav1.APIResource, o *unstructured.Unstructured) int {
	leastPriority := math.MaxInt32
	nsPriority := 0
	clusterScopedPriority := 1
	nodePriority := 2
	nodeRelatedPriority := 3
	saPriority := 5
	cmPriority := 6
	namespacesScopedPriority := 7
	kind := o.GetKind()
	switch kind {
	case "Namespace":
		return nsPriority
	case "Node", "CSINode":
		return nodePriority
	case "VolumeAttachment":
		return nodeRelatedPriority
	case "ServiceAccount":
		return saPriority
	case "ConfigMap", "Secret":
		return cmPriority
	case "Pod":
		return leastPriority
	}
	res, ok := apiResourcesByKind[kind]
	if !ok { // if resource is not found for o's kind then give least priority
		return leastPriority
	}
	if res.Namespaced {
		return namespacesScopedPriority
	} else {
		return clusterScopedPriority
	}
}

//func getNumKinds(objs []*unstructured.Unstructured) int {
//	var count int
//	var lastKind string
//	sortByKind(objs)
//	for _, obj := range objs {
//		if obj.GetKind() == lastKind {
//			continue
//		}
//		lastKind = obj.GetKind()
//		count++
//	}
//	return count
//}
