package taskcenter

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type projectDiskModel struct {
	Version  int       `json:"version"`
	Projects []Project `json:"projects"`
}

type ProjectRegistry struct {
	mu          sync.RWMutex
	path        string
	projects    map[string]Project
	loadWarning string
}

func NewProjectRegistry(path string) (*ProjectRegistry, error) {
	registry := &ProjectRegistry{path: strings.TrimSpace(path), projects: map[string]Project{}}
	if registry.path == "" {
		return registry, nil
	}
	model := projectDiskModel{}
	err := decodeFile(registry.path, &model, "projects")
	// A missing file is the normal first-run state; persist the empty, versioned
	// registry so upgrades from releases before Task Center are self-describing.
	if isNotExist(err) {
		if err := writeAtomic(registry.path, projectDiskModel{Version: schemaVersion, Projects: []Project{}}); err != nil {
			return nil, fmt.Errorf("initialize projects: %w", err)
		}
		return registry, nil
	}
	if err == nil {
		if loadErr := registry.loadModel(model); loadErr != nil {
			registry.loadWarning = corruptWarning(registry.path, "projects", loadErr)
		}
		return registry, nil
	}
	var corrupt *corruptDataError
	if errors.As(err, &corrupt) {
		registry.loadWarning = corruptWarning(registry.path, "projects", corrupt)
		return registry, nil
	}
	return nil, err
}

// NewProjectRegistry uses one read to avoid accepting a corrupt file as an
// empty store while still allowing the daemon to start and expose a warning.
func (r *ProjectRegistry) loadModel(model projectDiskModel) error {
	if model.Version != schemaVersion {
		return fmt.Errorf("decode projects: unsupported version %d", model.Version)
	}
	items := make(map[string]Project, len(model.Projects))
	for index, project := range model.Projects {
		project, err := normalizeProject(project)
		if err != nil {
			return fmt.Errorf("decode projects item %d: %w", index, err)
		}
		if _, exists := items[project.ProjectID]; exists {
			return fmt.Errorf("decode projects: duplicate projectId %q", project.ProjectID)
		}
		items[project.ProjectID] = project
	}
	if err := validateAliasUniqueness(items); err != nil {
		return fmt.Errorf("decode projects: %w", err)
	}
	r.projects = items
	return nil
}

func (r *ProjectRegistry) LoadWarning() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadWarning
}

func (r *ProjectRegistry) List() []Project {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Project, 0, len(r.projects))
	for _, project := range r.projects {
		result = append(result, cloneProject(project))
	}
	sort.Slice(result, func(i, j int) bool {
		if strings.EqualFold(result[i].Name, result[j].Name) {
			return result[i].ProjectID < result[j].ProjectID
		}
		return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
	})
	return result
}

func (r *ProjectRegistry) Get(id string) (Project, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	project, ok := r.projects[strings.TrimSpace(id)]
	return cloneProject(project), ok
}

// Resolve accepts an ID, exact name, or case-insensitive unique alias.
func (r *ProjectRegistry) Resolve(reference string) (Project, bool) {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return Project{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if project, ok := r.projects[reference]; ok && project.Enabled {
		return cloneProject(project), true
	}
	for _, project := range r.projects {
		if !project.Enabled {
			continue
		}
		if strings.EqualFold(project.Name, reference) {
			return cloneProject(project), true
		}
		for _, alias := range project.Aliases {
			if strings.EqualFold(alias, reference) {
				return cloneProject(project), true
			}
		}
	}
	return Project{}, false
}

func (r *ProjectRegistry) Create(input ProjectInput) (Project, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	project, err := normalizeProject(Project{
		ProjectID: newProjectID(), Name: input.Name, Aliases: input.Aliases, WorkingDirectory: input.WorkingDirectory,
		Description: input.Description, DefaultBackend: input.DefaultBackend, DefaultConversationNumber: input.DefaultConversationNumber,
		AutoCreateConversation: input.AutoCreateConversation, ReuseStrategy: input.ReuseStrategy, Enabled: boolOrDefault(input.Enabled, true), Tags: input.Tags,
		CreatedAt: nowText(), UpdatedAt: nowText(),
	})
	if err != nil {
		return Project{}, err
	}
	candidate := cloneProjects(r.projects)
	if err := validateProjectCollision(candidate, project); err != nil {
		return Project{}, err
	}
	candidate[project.ProjectID] = project
	if err := validateAliasUniqueness(candidate); err != nil {
		return Project{}, err
	}
	if err := writeAtomic(r.path, projectDiskModel{Version: schemaVersion, Projects: projectsSlice(candidate)}); err != nil {
		return Project{}, fmt.Errorf("persist project: %w", err)
	}
	r.projects = candidate
	r.loadWarning = ""
	return cloneProject(project), nil
}

func (r *ProjectRegistry) Update(id string, input ProjectInput) (Project, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id = strings.TrimSpace(id)
	current, ok := r.projects[id]
	if !ok {
		return Project{}, errors.New("项目不存在")
	}
	project, err := normalizeProject(Project{
		ProjectID: id, Name: input.Name, Aliases: input.Aliases, WorkingDirectory: input.WorkingDirectory,
		Description: input.Description, DefaultBackend: input.DefaultBackend, DefaultConversationNumber: input.DefaultConversationNumber,
		AutoCreateConversation: input.AutoCreateConversation, ReuseStrategy: input.ReuseStrategy, Enabled: boolOrDefault(input.Enabled, current.Enabled), Tags: input.Tags,
		CreatedAt: current.CreatedAt, UpdatedAt: nowText(),
	})
	if err != nil {
		return Project{}, err
	}
	candidate := cloneProjects(r.projects)
	delete(candidate, id)
	if err := validateProjectCollision(candidate, project); err != nil {
		return Project{}, err
	}
	candidate[id] = project
	if err := validateAliasUniqueness(candidate); err != nil {
		return Project{}, err
	}
	if err := writeAtomic(r.path, projectDiskModel{Version: schemaVersion, Projects: projectsSlice(candidate)}); err != nil {
		return Project{}, fmt.Errorf("persist project: %w", err)
	}
	r.projects = candidate
	r.loadWarning = ""
	return cloneProject(project), nil
}

func (r *ProjectRegistry) Delete(id string) (Project, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id = strings.TrimSpace(id)
	project, ok := r.projects[id]
	if !ok {
		return Project{}, errors.New("项目不存在")
	}
	candidate := cloneProjects(r.projects)
	delete(candidate, id)
	if err := writeAtomic(r.path, projectDiskModel{Version: schemaVersion, Projects: projectsSlice(candidate)}); err != nil {
		return Project{}, fmt.Errorf("persist project: %w", err)
	}
	r.projects = candidate
	return cloneProject(project), nil
}

func normalizeProject(project Project) (Project, error) {
	project.ProjectID = strings.TrimSpace(project.ProjectID)
	project.Name = strings.TrimSpace(project.Name)
	if project.ProjectID == "" {
		return Project{}, errors.New("projectId 不能为空")
	}
	if project.Name == "" {
		return Project{}, errors.New("项目名称不能为空")
	}
	project.Aliases = normalizeAliases(project.Aliases)
	project.WorkingDirectory = strings.TrimSpace(project.WorkingDirectory)
	if project.WorkingDirectory != "" {
		absolute, err := filepath.Abs(project.WorkingDirectory)
		if err != nil {
			return Project{}, fmt.Errorf("工作目录无效：%w", err)
		}
		project.WorkingDirectory = filepath.Clean(absolute)
	}
	project.DefaultBackend = normalizeBackend(project.DefaultBackend)
	if !validBackend(project.DefaultBackend) {
		return Project{}, errors.New("DefaultBackend 必须是 codex 或 openclaw")
	}
	if project.DefaultConversationNumber != nil && *project.DefaultConversationNumber < 1 {
		return Project{}, errors.New("DefaultConversationNumber 必须是正整数")
	}
	project.ReuseStrategy = strings.ToLower(strings.TrimSpace(project.ReuseStrategy))
	if project.ReuseStrategy == "" {
		project.ReuseStrategy = ReuseDefault
	}
	if !validReuseStrategy(project.ReuseStrategy) {
		return Project{}, errors.New("ReuseStrategy 必须是 default、latest、create-new 或 fixed")
	}
	if project.CreatedAt == "" {
		project.CreatedAt = nowText()
	}
	if project.UpdatedAt == "" {
		project.UpdatedAt = project.CreatedAt
	}
	project.Tags = normalizeAliases(project.Tags)
	return cloneProject(project), nil
}

func validateProjectCollision(projects map[string]Project, candidate Project) error {
	for _, project := range projects {
		if strings.EqualFold(project.Name, candidate.Name) {
			return fmt.Errorf("项目名称 %q 已存在", candidate.Name)
		}
	}
	return nil
}

func validateAliasUniqueness(projects map[string]Project) error {
	used := map[string]string{}
	for _, project := range projects {
		for _, alias := range append([]string{project.Name}, project.Aliases...) {
			key := strings.ToLower(strings.TrimSpace(alias))
			if key == "" {
				continue
			}
			if prior, exists := used[key]; exists && prior != project.ProjectID {
				return fmt.Errorf("项目名称或别名 %q 已被其他项目使用", alias)
			}
			used[key] = project.ProjectID
		}
	}
	return nil
}

func normalizeAliases(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value != "" && !seen[key] {
			seen[key] = true
			result = append(result, value)
		}
	}
	return result
}

func projectsSlice(projects map[string]Project) []Project {
	result := make([]Project, 0, len(projects))
	for _, project := range projects {
		result = append(result, cloneProject(project))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func cloneProjects(source map[string]Project) map[string]Project {
	result := make(map[string]Project, len(source))
	for id, project := range source {
		result[id] = cloneProject(project)
	}
	return result
}

func cloneProject(project Project) Project {
	project.Aliases = append([]string(nil), project.Aliases...)
	project.Tags = append([]string(nil), project.Tags...)
	if project.DefaultConversationNumber != nil {
		value := *project.DefaultConversationNumber
		project.DefaultConversationNumber = &value
	}
	return project
}

func boolOrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func newProjectID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "project-" + itoa(int(time.Now().UnixNano()))
	}
	return "project-" + hex.EncodeToString(buffer)
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func isNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
