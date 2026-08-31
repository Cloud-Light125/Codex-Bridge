package taskcenter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistriesInitializeMissingVersionedFiles(t *testing.T) {
	directory := t.TempDir()
	projectsPath := filepath.Join(directory, "projects.json")
	tasksPath := filepath.Join(directory, "tasks.json")

	if _, err := NewProjectRegistry(projectsPath); err != nil {
		t.Fatal(err)
	}
	projectData, err := os.ReadFile(projectsPath)
	if err != nil {
		t.Fatal(err)
	}
	var projects projectDiskModel
	if err := json.Unmarshal(projectData, &projects); err != nil {
		t.Fatal(err)
	}
	if projects.Version != schemaVersion || len(projects.Projects) != 0 {
		t.Fatalf("initialized project model = %#v", projects)
	}

	if _, err := NewTaskRegistry(tasksPath); err != nil {
		t.Fatal(err)
	}
	taskData, err := os.ReadFile(tasksPath)
	if err != nil {
		t.Fatal(err)
	}
	var tasks taskDiskModel
	if err := json.Unmarshal(taskData, &tasks); err != nil {
		t.Fatal(err)
	}
	if tasks.Version != schemaVersion || tasks.NextTaskNumber != 1 || len(tasks.Tasks) != 0 {
		t.Fatalf("initialized task model = %#v", tasks)
	}
}

func TestTaskRegistryNumbersRemainMonotonicAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	registry, err := NewTaskRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Create(TaskInput{ProjectID: "p", Title: "one", Description: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Create(TaskInput{ProjectID: "p", Title: "two", Description: "two"})
	if err != nil {
		t.Fatal(err)
	}
	third, err := registry.Create(TaskInput{ProjectID: "p", Title: "three", Description: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if first.TaskNumber != 1 || second.TaskNumber != 2 || third.TaskNumber != 3 {
		t.Fatalf("task numbers = %d, %d, %d; want 1, 2, 3", first.TaskNumber, second.TaskNumber, third.TaskNumber)
	}
	// Reload the actual durable file. Historical tasks remain available and the
	// allocator resumes after the persisted nextTaskNumber.
	reloaded, err := NewTaskRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if persisted := reloaded.List(TaskFilter{}); len(persisted) != 3 {
		t.Fatalf("reloaded tasks = %d; want 3", len(persisted))
	}
	newTask, err := reloaded.Create(TaskInput{ProjectID: "p", Title: "four", Description: "four"})
	if err != nil {
		t.Fatal(err)
	}
	if newTask.TaskNumber != 4 {
		t.Fatalf("reloaded task number = %d; want 4", newTask.TaskNumber)
	}
}

func TestProjectRegistryAliasAndWorkingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projects.json")
	registry, err := NewProjectRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	project, err := registry.Create(ProjectInput{
		Name: "CloudLight XiaoMi", Aliases: []string{" xiaomi ", "XM"},
		WorkingDirectory: ".", DefaultBackend: BackendCodex,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(project.WorkingDirectory) {
		t.Fatalf("working directory = %q; want absolute path", project.WorkingDirectory)
	}
	resolved, ok := registry.Resolve("XM")
	if !ok || resolved.ProjectID != project.ProjectID {
		t.Fatalf("alias resolved to %#v, %v", resolved, ok)
	}
	reloaded, err := NewProjectRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, ok := reloaded.Resolve("xiaomi")
	if !ok || persisted.ProjectID != project.ProjectID || persisted.DefaultBackend != BackendCodex {
		t.Fatalf("reloaded project = %#v, %v", persisted, ok)
	}
	_, err = registry.Create(ProjectInput{Name: "Other", Aliases: []string{"xiaomi"}, DefaultBackend: BackendOpenClaw})
	if err == nil || !strings.Contains(err.Error(), "已被其他项目使用") {
		t.Fatalf("alias collision error = %v", err)
	}
}

func TestRegistriesPreserveCorruptJSON(t *testing.T) {
	directory := t.TempDir()
	projectsPath := filepath.Join(directory, "projects.json")
	if err := os.WriteFile(projectsPath, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	projects, err := NewProjectRegistry(projectsPath)
	if err != nil {
		t.Fatal(err)
	}
	if projects.LoadWarning() == "" {
		t.Fatal("expected corrupt projects warning")
	}
	matches, err := filepath.Glob(projectsPath + ".corrupt-*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("preserved project files = %v, err=%v", matches, err)
	}

	tasksPath := filepath.Join(directory, "tasks.json")
	if err := os.WriteFile(tasksPath, []byte(`{"version":1,"tasks":[`), 0o600); err != nil {
		t.Fatal(err)
	}
	tasks, err := NewTaskRegistry(tasksPath)
	if err != nil {
		t.Fatal(err)
	}
	if tasks.LoadWarning() == "" {
		t.Fatal("expected corrupt tasks warning")
	}
	if _, err := os.Stat(tasksPath); err != nil {
		t.Fatal(err)
	}
}

func TestTaskRegistryAtomicFileHasVersionAndNextNumber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	registry, err := NewTaskRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Create(TaskInput{ProjectID: "p", Description: "atomic save"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var model taskDiskModel
	if err := json.Unmarshal(data, &model); err != nil {
		t.Fatal(err)
	}
	if model.Version != schemaVersion || model.NextTaskNumber != 2 || len(model.Tasks) != 1 {
		t.Fatalf("disk model = %#v", model)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists: %v", err)
	}
}
