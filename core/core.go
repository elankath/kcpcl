package core

import (
	"cmp"
	"context"
	"fmt"
	"github.com/alitto/pond/v2"
	"github.com/elankath/copyshoot/api"
	clientutil "github.com/elankath/copyshoot/core/clientutil"
	authenticationv1alpha1 "github.com/gardener/gardener/pkg/apis/authentication/v1alpha1"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	extensionsv1alpha1 "github.com/gardener/gardener/pkg/apis/extensions/v1alpha1"
	"io/fs"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/restmapper"
	"maps"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

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
	SupportedScheme = CreateRegisterScheme()
	KindToPriority  = map[string]int{
		"CustomResourceDefinition": 0,
		"PriorityClass":            1,
		"Namespace":                2,
		"ServiceAccount":           3,
		"Secret":                   4,
		"ConfigMap":                5,
		"PersistentVolume":         6,
		"PersistentVolumeClaim":    7,
		"Deployment":               8,
		"StatefulSet":              8,
		"Pod":                      10,
	}
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
	scheme := runtime.NewScheme()
	utilruntime.Must(gardencorev1beta1.AddToScheme(scheme))
	utilruntime.Must(extensionsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(storagev1.AddToScheme(scheme))
	utilruntime.Must(schedulingv1.AddToScheme(scheme))
	utilruntime.Must(authenticationv1alpha1.AddToScheme(scheme))

	err = gardencorev1beta1.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}

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
	objs, err := loadObjects(baseObjDir)
	if err != nil {
		err = fmt.Errorf("%w: failed to load objects: %w", api.ErrUploadFailed, err)
		return
	}
	apiGroupResources, err := restmapper.GetAPIGroupResources(g.discoveryClient)
	if err != nil {
		return fmt.Errorf("%w: failed to fetch API group resources: %w", api.ErrDiscovery, err)
	}
	restMap := restmapper.NewDiscoveryRESTMapper(apiGroupResources)

	uploadGroups, err := createUploadTaskGroups(ctx, g.dynamicClient, restMap, g.pool, objs)
	if err != nil {
		err = fmt.Errorf("%w: failed to create upload task groups: %w", api.ErrUploadFailed, err)
		return
	}
	uploadCount := &atomic.Uint32{}
	for _, ug := range uploadGroups {
		err = ug.UploadAndWait(uploadCount)
		if err != nil {
			return
		}
	}
	return
}

type UploadGroup struct {
	Ctx            context.Context
	GVK            schema.GroupVersionKind
	GVR            schema.GroupVersionResource
	Namespace      string
	TaskGroup      pond.TaskGroup
	ResourceFacade dynamic.ResourceInterface
	Objects        []*unstructured.Unstructured
}

func (u *UploadGroup) UploadAndWait(count *atomic.Uint32) error {
	slog.Info("Commencing UploadGroup", "kind", u.GVK.Kind)
	for _, obj := range u.Objects {
		u.TaskGroup.SubmitErr(func() error {
			_, err := u.ResourceFacade.Create(u.Ctx, obj, metav1.CreateOptions{})
			if err != nil {
				if errors.IsAlreadyExists(err) {
					slog.Warn("object already exists, skipping upload", "name", obj.GetName(), "namespace", obj.GetNamespace())
					return nil
				}
				err = fmt.Errorf("failed to create obj of kind %q, name %q and namespace %q: %w",
					obj.GetKind(), obj.GetName(), obj.GetNamespace(), err)
				return err
			}
			if count.Load()%2000 == 0 {
				slog.Info("object created", "count", count.Load(), "kind", obj.GetKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())
			} else {
				slog.Debug("object created", "count", count.Load(), "kind", obj.GetKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())
			}
			count.Add(1)
			return nil
		})
	}
	err := u.TaskGroup.Wait()
	if err != nil {
		return fmt.Errorf("%w: failed to upload task group for GVR %q: %w", api.ErrUploadFailed, u.GVR, err)
	}
	slog.Info("Finished UploadGroup", "kind", u.GVK.Kind)
	return nil
}

// createUploadTaskGroups creates a map of Kind to UploadGroup for objects of that kind
func createUploadTaskGroups(ctx context.Context, dynamicClient dynamic.Interface, restMap meta.RESTMapper, pool pond.Pool, objs []*unstructured.Unstructured) (uploadGroups []UploadGroup, err error) {
	sortObjsByPriority(objs)
	numKinds := getNumKinds(objs)
	uploadGroups = make([]UploadGroup, numKinds)

	var kindIndex = -1
	var restMapping *meta.RESTMapping
	var resourceFacade dynamic.ResourceInterface

	for _, obj := range objs {
		kind := obj.GetKind()
		ns := obj.GetNamespace()
		gvk := obj.GroupVersionKind()

		if kindIndex != -1 && kind == uploadGroups[kindIndex].GVK.Kind {
			uploadGroups[kindIndex].Objects = append(uploadGroups[kindIndex].Objects, obj)
			continue
		}
		kindIndex++

		restMapping, err = restMap.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			err = fmt.Errorf("%w: failed to fetch REST mapping for %q: %w", api.ErrDiscovery, gvk, err)
			return
		}

		gvr := restMapping.Resource
		if obj.GetNamespace() != "" {
			resourceFacade = dynamicClient.Resource(gvr).Namespace(obj.GetNamespace())
		} else {
			resourceFacade = dynamicClient.Resource(gvr)
		}

		uploadGroups[kindIndex] = UploadGroup{
			Ctx:            ctx,
			GVK:            gvk,
			GVR:            gvr,
			Namespace:      ns,
			TaskGroup:      pool.NewGroupContext(ctx),
			ResourceFacade: resourceFacade,
			Objects:        []*unstructured.Unstructured{obj},
		}
	}
	return
}

func loadObjects(baseObjDir string) (objs []*unstructured.Unstructured, err error) {
	slog.Info("Loading objects.", "baseObjDir", baseObjDir)
	//entries, err := os.ReadDir(baseObjDir)
	//if err != nil {
	//	err = fmt.Errorf("%w: %w", api.ErrUploadFailed, err)
	//	return
	//}
	var obj *unstructured.Unstructured
	count := 0
	err = filepath.WalkDir(baseObjDir, func(path string, e fs.DirEntry, err error) error {
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
		obj, err = LoadAndCleanObj(path)
		if err != nil {
			return err
		}
		objs = append(objs, obj)
		count++
		if count%1000 == 0 {
			slog.Info("Loaded object", "count", count, "path", path)
		} else {
			slog.Debug("Loaded object", "count", count, "path", path)
		}
		return nil
	})
	slog.Info("Loaded total objects", "count", count, "baseObjDir", baseObjDir)

	//for _, resourcesDirName := range entries {
	//	if !resourcesDirName.IsDir() {
	//		continue
	//	}
	//	parts := strings.SplitN(resourcesDirName.Name(), "-", 3)
	//	if len(parts) != 3 {
	//		err = fmt.Errorf("%w: invalid object resourcesDirName: %s", api.ErrUploadFailed, resourcesDirName.Name())
	//		return
	//	}
	//	resourceDirPath := filepath.Join(baseObjDir, resourcesDirName.Name())
	//	files, _ := os.ReadDir(resourceDirPath)
	//	for _, f := range files {
	//		objPath := filepath.Join(resourceDirPath, f.Name())
	//		obj, err = LoadAndCleanObj(objPath)
	//		if err != nil {
	//			return
	//		}
	//		objs = append(objs, obj)
	//	}
	//}
	return
}

func LoadAndCleanObj(objPath string) (obj *unstructured.Unstructured, err error) {
	// Parse the YAML
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
	obj.SetResourceVersion("")
	obj.SetCreationTimestamp(metav1.Time{})
	obj.SetUID("")
	//obj.SetManagedFields(nil)
	obj.SetGeneration(0)
	//unstructured.RemoveNestedField(obj.Object, "status")

	return
}

func writeObjectList(objList *unstructured.UnstructuredList, resourceDir string, ns string) (err error) {
	var filename string
	for _, obj := range objList.Items {
		if ns != "" {
			filename = filepath.Join(resourceDir, sanitizeFileName(ns+"-"+obj.GetName())+".yaml")
		} else {
			filename = filepath.Join(resourceDir, sanitizeFileName(obj.GetName())+".yaml")
		}
		err = writeYAMLFile(filename, &obj)
		if err != nil {
			return
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
	return strings.ReplaceAll(name, "/", "_")
}

func writeYAMLFile(path string, obj *unstructured.Unstructured) error {
	f := func() error {
		data, err := json.Marshal(obj.Object)
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
	err := f()
	if err != nil {
		err = fmt.Errorf("%w: failed to write yaml file %q for obj named %q in namespace %q: %w", api.ErrDownloadFailed, path, obj.GetName(), obj.GetNamespace(), err)
	}
	return err
}

func sortByKind(objs []*unstructured.Unstructured) {
	sort.Slice(objs, func(i, j int) bool {
		ki := KindToPriority[objs[i].GetKind()]
		kj := KindToPriority[objs[j].GetKind()]
		return ki < kj
	})
}

func createPriorityToKind(kindToPriority map[string]int) []string {
	kinds := slices.Collect(maps.Keys(kindToPriority))
	slices.SortFunc(kinds, func(a, b string) int {
		return kindToPriority[a] - kindToPriority[b]
	})
	return kinds
}

func sortObjsByPriority(objs []*unstructured.Unstructured) {
	slices.SortFunc(objs, func(a, b *unstructured.Unstructured) int {
		ap, ok := KindToPriority[a.GetKind()]
		if !ok {
			ap = 1 //If not present in KindToPriority it always has lesser priority
		}
		bp, ok := KindToPriority[b.GetKind()]
		if !ok {
			bp = 1
		}
		return cmp.Compare(ap, bp)
	})
}

func getNumKinds(objs []*unstructured.Unstructured) int {
	var count int
	var lastKind string
	sortByKind(objs)
	for _, obj := range objs {
		if obj.GetKind() == lastKind {
			continue
		}
		lastKind = obj.GetKind()
		count++
	}
	return count
}
