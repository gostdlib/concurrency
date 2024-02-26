/*
Copyright © 2023 John Doak <doak@askdoak.com>
*/
package cmd

import (
	"bytes"
	_ "embed"
	"os"
	"path"
	"text/template"

	"github.com/spf13/cobra"
)

var (
	pkg        string
	createMain bool
)

//go:embed data.tmpl
var dataTmpl string

//go:embed pipeline.tmpl
var pipelineTmpl string

//go:embed main.tmpl
var mainTmpl string

var tmpls = template.New("")

type tmplArgs struct {
	PackagePath string
	PackageName string
}

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Create the barebones structure of a stagepipe application",
	Long: `The stagedpipe framework allows creating concurrent and parallel pipelines.
And while robust and easily extendable, it has a lot of initial boilerplate. This
application simply allows you to create that boilerplate in a single go and get started.
For example, to create the structure needed for a pipeline including "main.go":
	cd <your main directory>
	stagedpipe-cli init -m -p "<the path to main directory + pipeline package name>"
	go mod init <path> // if needed
	go mod tidy
	go fmt ./...

If I was in the "test" directory and ran the following, this would be my outcome:
	stagedpipe-cli init -m -p "github.com/johnsiilver/test/mypipeline"
	
	This will leave the following structure in that directory:
	.
	├── go.mod
	├── go.sum
	├── main.go
	└── mypipeline
		├── data.go
		└── mypipeline.go

The above program will be runable.

If you do not need a main.go, you can simply remove the "-m" from the command.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		tArgs := tmplArgs{
			PackagePath: pkg,
			PackageName: path.Base(pkg),
		}

		dataBuff := &bytes.Buffer{}
		pipeBuff := &bytes.Buffer{}
		mainBuff := &bytes.Buffer{}

		if err := tmpls.ExecuteTemplate(dataBuff, "data", tArgs); err != nil {
			return err
		}
		if err := tmpls.ExecuteTemplate(pipeBuff, "pipeline", tArgs); err != nil {
			return err
		}
		if err := tmpls.ExecuteTemplate(mainBuff, "main", tArgs); err != nil {
			return err
		}

		if err := os.Mkdir(path.Base(tArgs.PackagePath), 0o700); err != nil {
			return err
		}

		if createMain {
			if err := os.WriteFile("main.go", mainBuff.Bytes(), 0o600); err != nil {
				return err
			}
		}

		if err := os.Chdir(path.Base(tArgs.PackagePath)); err != nil {
			return err
		}

		if err := os.WriteFile("data.go", dataBuff.Bytes(), 0o600); err != nil {
			return err
		}

		if err := os.WriteFile(path.Base(tArgs.PackagePath)+".go", pipeBuff.Bytes(), 0o600); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	template.Must(tmpls.New("data").Parse(dataTmpl))
	template.Must(tmpls.New("pipeline").Parse(pipelineTmpl))
	template.Must(tmpls.New("main").Parse(mainTmpl))

	rootCmd.AddCommand(initCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// initCmd.PersistentFlags().String("foo", "", "A help for foo")

	initCmd.Flags().StringVarP(&pkg, "pkg", "p", "", "The package you wish to create")
	initCmd.Flags().BoolVarP(&createMain, "createMain", "m", false, "Create a main file that calls the package")
	initCmd.MarkFlagRequired("pkg")
}
