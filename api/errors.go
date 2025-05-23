package api

import "errors"

var (
	ErrMissingKubeConfig        = errors.New("missing kubeconfig")
	ErrMissingShootKubeConfig   = errors.New("missing shoot kubeconfig")
	ErrMissingControlKubeConfig = errors.New("missing shoot control-plane kubeconfig")
	ErrObjDirNotExist           = errors.New("obj dir not exist")
	ErrCantReadObjDir           = errors.New("cant read obj dir")
	ErrMissingObjDir            = errors.New("missing obj dir")

	ErrMissingGVRs      = errors.New("missing GVRs")
	ErrMissingLandscape = errors.New("missing gardener landscape")
	ErrMissingProject   = errors.New("missing gardener project")
	ErrMissingShoot     = errors.New("missing gardener shoot")

	ErrInvalidGVR                = errors.New("invalid GVR format")
	ErrNotFoundGVR               = errors.New("not found GVR")
	ErrGardenNameNotFound        = errors.New("garden name not found")
	ErrGardenCtlConfigLoadFailed = errors.New("failed to load gardenctl config")
	ErrCreateKubeClient          = errors.New("failed create kube client")
	ErrCreateGardenClient        = errors.New("failed create garden client")

	ErrDiscovery = errors.New("cannot discover resources")

	ErrLoadObj      = errors.New("cannot load object")
	ErrLoadTemplate = errors.New("cannot load template")
	ErrExecTemplate = errors.New("cannot execute template")
	ErrUploadFailed = errors.New("upload failed")

	ErrSaveObj        = errors.New("cannot save object")
	ErrDownloadFailed = errors.New("download failed")
)
