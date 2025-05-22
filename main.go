package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/elankath/kcpcl/api"
	"github.com/elankath/kcpcl/cli"
	"github.com/elankath/kcpcl/core"
	flag "github.com/spf13/pflag"
	"log/slog"
	"os"
	"runtime/debug"
)

func main() {
	var err error
	var exitCode int

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info, ok := debug.ReadBuildInfo()
	if ok {
		if info.Main.Version != "" {
			fmt.Printf("%s version: %s\n", api.ProgramName, info.Main.Version)
		}
	} else {
		fmt.Printf("%s: binary build info not embedded", api.ProgramName)
	}

	if len(os.Args) < 2 {
		printExpectedSubCommand()
		os.Exit(cli.ExitMissingSubCommand)
	}
	subCommand := os.Args[1]
	subCommandFlags := flag.NewFlagSet(subCommand, flag.ContinueOnError)
	switch subCommand {
	case "download":
		exitCode, err = ExecDownload(ctx, subCommandFlags, os.Args[2:])
	case "upload":
		exitCode, err = ExecUpload(ctx, subCommandFlags, os.Args[2:])
	case "help", "-h", "--help":
		_, _ = fmt.Fprintf(os.Stderr, `Please invoke one of the below:
		%s download -h  
		%s upload -h
`, api.ProgramName, api.ProgramName)
	default:
		printExpectedSubCommand()
		os.Exit(cli.ExitUnknownSubCommand)
	}
	if err == nil {
		return
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(cli.ExitSuccess)
	}
	_, _ = fmt.Fprintf(os.Stderr, "Err: %v\n", err)
	if errors.Is(err, api.ErrUploadFailed) || errors.Is(err, api.ErrDownloadFailed) {
		os.Exit(exitCode)
	}
	subCommandFlags.Usage()
	os.Exit(cli.ExitGeneral)
}

func printExpectedSubCommand() {
	_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf(`Expected one of 
	%s upload <flags> <args> 
	%s download <flags> <args>
See %s upload|download -h
`, api.ProgramName, api.ProgramName, api.ProgramName))
}

func ExecDownload(ctx context.Context, subCommandFlags *flag.FlagSet, args []string) (exitCode int, err error) {
	var mainOpts cli.MainOpts
	cli.SetupDownloadFlagsToOpts(subCommandFlags, &mainOpts)
	err = subCommandFlags.Parse(args)
	if err != nil {
		exitCode = cli.ExitOptsParseErr
		return
	}
	exitCode, err = cli.ValidateMainOptsForDownload(&mainOpts, subCommandFlags.Args())
	if err != nil {
		return
	}

	copier, err := NewShootCopierFromOpts(mainOpts)
	if err != nil {
		if errors.Is(err, api.ErrCreateKubeClient) {
			exitCode = cli.ExitKubeClientCreate
		}
		return
	}

	gvrs := subCommandFlags.Args()
	if len(gvrs) == 0 {
		gvrs = api.DefaultGVRs
		slog.Warn("No gvrs specified. Assuming default.", "gvrs", gvrs)
	}
	gvrList, err := api.ParseGVRs(gvrs)
	if err != nil {
		exitCode = cli.ExitParseGVR
		return
	}

	err = copier.DownloadObjects(ctx, mainOpts.ObjDir, gvrList)
	if err != nil {
		if errors.Is(err, api.ErrUploadFailed) {
			exitCode = cli.ExitUploadFailed // TODO: do exit code mappings in main routine.
		}
		return
	}
	err = core.GenKubeSchedulerConfiguration(mainOpts.KubeSchedulerConfigPath, mainOpts.KubeConfigPath, mainOpts.PoolSize)
	if err != nil {
		return
	}
	return
}
func ExecUpload(ctx context.Context, subCommandFlags *flag.FlagSet, args []string) (exitCode int, err error) {
	var mainOpts cli.MainOpts
	cli.SetupUploadFlagsToOpts(subCommandFlags, &mainOpts)
	err = subCommandFlags.Parse(args)
	if err != nil {
		exitCode = cli.ExitOptsParseErr
		return
	}
	exitCode, err = cli.ValidateMainOptsForUpload(&mainOpts)
	if err != nil {
		return
	}

	copier, err := NewShootCopierFromOpts(mainOpts)
	if err != nil {
		if errors.Is(err, api.ErrCreateKubeClient) {
			exitCode = cli.ExitKubeClientCreate
		}
		return
	}
	err = copier.UploadObjects(ctx, mainOpts.ObjDir)
	if err != nil {
		if errors.Is(err, api.ErrDownloadFailed) {
			exitCode = cli.ExitDownloadFailed
		}
		return
	}
	err = core.GenKubeSchedulerConfiguration(mainOpts.KubeSchedulerConfigPath, mainOpts.KubeConfigPath, mainOpts.PoolSize)
	if err != nil {
		return
	}
	//err = core.GenerateSchedulerProfile(mainOpts.KubeConfigPath, "/tmp/-ksched-config.yaml")
	//if err != nil {
	//	return
	//}
	return
}
func NewShootCopierFromOpts(opts cli.MainOpts) (copier api.ShootCopier, err error) {
	return core.NewShootCopierFromConfig(opts.CopierConfig)
}
