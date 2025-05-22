package core

import (
	"bytes"
	"embed"
	"fmt"
	"github.com/elankath/kcpcl/api"
	"log/slog"
	"os"
	"text/template"
)

//go:embed templates/*yaml
var templateFS embed.FS

var (
	kubeSchedulerConfigTemplate     *template.Template
	kubeSchedulerConfigTemplatePath string = "templates/kube-scheduler-config.yaml"
)

type KubeSchedulerTmplParams struct {
	KubeConfigPath string
	QPS            float32
	Burst          int
}

func GenKubeSchedulerConfiguration(targetPath string, kubeConfigPath string, poolSize int) error {
	cfgTmpl, err := getKubeSchedulerConfigTemplate()
	if err != nil {
		return err
	}
	params := KubeSchedulerTmplParams{
		KubeConfigPath: kubeConfigPath,
		QPS:            float32(poolSize),
		Burst:          poolSize,
	}
	err = executeTemplate(cfgTmpl, targetPath, params)
	return err
}

func executeTemplate(tmpl *template.Template, targetPath string, params any) error {
	var output bytes.Buffer
	err := tmpl.Execute(&output, params)
	if err != nil {
		return fmt.Errorf("%w %q: %w", api.ErrExecTemplate, tmpl.Name(), err)
	}
	err = os.WriteFile(targetPath, output.Bytes(), 0644)
	if err != nil {
		return err
	}
	slog.Info("Executed template and wrote target.", "templateName", tmpl.Name(), "targetPath", targetPath)
	return nil
}

func getKubeSchedulerConfigTemplate() (configTemplate *template.Template, err error) {
	if kubeSchedulerConfigTemplate != nil {
		configTemplate = kubeSchedulerConfigTemplate
		return
	}
	data, err := templateFS.ReadFile(kubeSchedulerConfigTemplatePath)
	if err != nil {
		err = fmt.Errorf("%w: cannot read kube-scheduler config tempalte %q: %w", api.ErrLoadTemplate, kubeSchedulerConfigTemplatePath, err)
		return
	}
	configTemplate, err = template.New("KubeSchedulerConfigTemplate").Parse(string(data))
	if err != nil {
		err = fmt.Errorf("%w: cannot parse kube-scheduler config template %q: %w", api.ErrLoadTemplate, kubeSchedulerConfigTemplatePath, err)
	}
	return
}
