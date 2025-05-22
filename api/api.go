package api

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"log/slog"
	"strings"
)

var (
	ProgramName = "kcpcl"
	DefaultGVRs = []string{
		"namespaces",
		"scheduling.k8s.io/v1/priorityclasses",
		"storage.k8s.io/v1/csidrivers",
		"storage.k8s.io/v1/csistoragecapacities",
		"storage.k8s.io/v1/storageclasses",
		"serviceaccounts",
		"configmaps",
		"persistentvolumes",
		"persistentvolumeclaims",
		"storage.k8s.io/v1/volumeattachments",
		"apps/v1/deployments",
		"apps/v1/statefulsets",
		"apps/v1/replicasets",
		"nodes",
		"storage.k8s.io/v1/csinodes",
		"pods"}
)

// CopierConfig represents input configuration for creating and initializing a ShootCopier
type CopierConfig struct {
	// KubeConfigPath represents path to source or Target kubeconfig
	KubeConfigPath string

	// ControlKubeConfigPath represents path to shoot control cluster kubeconfig.
	ControlKubeConfigPath string

	// GVRStrings represent list of GVR to download/upload int the form: '[group/][version/]resource. Ex: pods nodes
	GVRStrings []string

	PoolSize   int
	OrderKinds bool
}

// ShootCoords represents the coordinates of a gardner shoot cluster. It can be used to represent both the shoot and seed.
type ShootCoords struct {
	Landscape string
	Project   string
	Name      string
}

func (s ShootCoords) String() string {
	return fmt.Sprintf("(%s|%s|%s)", s.Landscape, s.Project, s.Name)
}

type ShootInfo struct {
	Coords    ShootCoords
	Namespace string
}

// ShootCopier offers methods to copy k8s objects from gardener shoot data and control planes (seed) to a target cluster.
// TODO: also offer way to copy from one shoot to another shoot later
type ShootCopier interface {
	GetConfig() CopierConfig

	GetClient() dynamic.Interface

	DownloadObjects(ctx context.Context, baseObjDir string, gvrList []schema.GroupVersionResource) error

	UploadObjects(ctx context.Context, baseObjDir string) error
}

// ParseGVR parses strings like:  "pods" "apps/v1/deployments" "scheduling.k8s.io/v1/priorityclasses"
func ParseGVR(arg string) (gvr schema.GroupVersionResource, err error) {
	parts := strings.Split(arg, "/")
	var gv schema.GroupVersion
	switch len(parts) {
	case 0:
		err = fmt.Errorf("%w: can't parse %q", ErrInvalidGVR, arg)
		return
	case 1: // Assume core group and v1
		gvr = schema.GroupVersionResource{Group: "", Version: "v1", Resource: parts[0]}
	case 2:
		// Treat as version/resource from core group
		gvr = schema.GroupVersionResource{Group: "", Version: parts[0], Resource: parts[1]}
	case 3:
		gv, err = schema.ParseGroupVersion(parts[0] + "/" + parts[1])
		if err != nil {
			err = fmt.Errorf("%w: can't parse %q: %w", ErrInvalidGVR, arg, err)
			return
		}
		gvr = gv.WithResource(parts[2])
	default:
		err = fmt.Errorf("%w: too many elems in %q", ErrInvalidGVR, arg)
		return
	}
	slog.Debug("Parsed gvr.", "GVR", gvr)
	return
}

func ParseGVRs(args []string) (gvrList []schema.GroupVersionResource, err error) {
	var gvr schema.GroupVersionResource
	for _, arg := range args {
		gvr, err = ParseGVR(arg)
		if err != nil {
			return
		}
		gvrList = append(gvrList, gvr)
	}
	return
}
