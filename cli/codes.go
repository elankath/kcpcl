package cli

const (
	ExitSuccess int = iota

	ExitMissingSubCommand
	ExitUnknownSubCommand
	ExitOptsParseErr
	ExitMissingArgs

	ExitMandatoryOpt
	ExitKubeClientCreate
	ExitParseGVR

	ExitObjDir
	ExitDownloadFailed
	ExitUploadFailed

	ExitValidateGVR
	ExitGeneral = 255
)
