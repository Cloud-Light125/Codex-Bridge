package taskcenter

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type taskDiskModel struct {
	Version        int    `json:"version"`
	NextTaskNumber int    `json:"nextTaskNumber"`
	Tasks          []Task `json:"tasks"`
}

type TaskRegistry struct {
	mu             sync.RWMutex
	path           string
	nextTaskNumber int
	tasks          map[int]Task
	byID           map[string]int
	loadWarning    string
}

func NewTaskRegistry(path string) (*TaskRegistry, error) {
	registry := &TaskRegistry{path: strings.TrimSpace(path), nextTaskNumber: 1, tasks: map[int]Task{}, byID: map[string]int{}}
	if registry.path == "" {
		return registry, nil
	}
	model := taskDiskModel{}
	err := decodeFile(registry.path, &model, "tasks")
	if isNotExist(err) {
		if err := writeAtomic(registry.path, taskDiskModel{Version: schemaVersion, NextTaskNumber: 1, Tasks: []Task{}}); err != nil {
			return nil, fmt.Errorf("initialize tasks: %w", err)
		}
		return registry, nil
	}
	if err == nil {
		if loadErr := registry.loadModel(model); loadErr != nil {
			registry.loadWarning = corruptWarning(registry.path, "tasks", loadErr)
		}
		return registry, nil
	}
	var corrupt *corruptDataError
	if errors.As(err, &corrupt) {
		registry.loadWarning = corruptWarning(registry.path, "tasks", corrupt)
		return registry, nil
	}
	return nil, err
}

func (r *TaskRegistry) loadModel(model taskDiskModel) error {
	if model.Version != schemaVersion {
		return fmt.Errorf("decode tasks: unsupported version %d", model.Version)
	}
	next := model.NextTaskNumber
	if next < 1 {
		next = 1
	}
	items := make(map[int]Task, len(model.Tasks))
	byID := make(map[string]int, len(model.Tasks))
	max := 0
	for index, task := range model.Tasks {
		task, err := normalizeStoredTask(task)
		if err != nil {
			return fmt.Errorf("decode tasks item %d: %w", index, err)
		}
		if _, exists := items[task.TaskNumber]; exists {
			return fmt.Errorf("decode tasks: duplicate taskNumber %d", task.TaskNumber)
		}
		if _, exists := byID[task.TaskID]; exists {
			return fmt.Errorf("decode tasks: duplicate taskId %q", task.TaskID)
		}
		items[task.TaskNumber], byID[task.TaskID] = task, task.TaskNumber
		if task.TaskNumber > max {
			max = task.TaskNumber
		}
	}
	if next <= max {
		next = max + 1
	}
	r.nextTaskNumber, r.tasks, r.byID = next, items, byID
	return nil
}

func (r *TaskRegistry) LoadWarning() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadWarning
}

func (r *TaskRegistry) NextTaskNumber() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextTaskNumber
}

func (r *TaskRegistry) Create(input TaskInput) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	description := strings.TrimSpace(input.Description)
	if description == "" {
		return Task{}, errors.New("任务内容不能为空")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		title = firstLine(description)
	}
	if title == "" {
		return Task{}, errors.New("任务标题不能为空")
	}
	now := nowText()
	task := Task{
		TaskID: newTaskID(), TaskNumber: r.nextTaskNumber, Title: title, Description: description,
		ProjectID: strings.TrimSpace(input.ProjectID), Backend: normalizeBackend(input.Backend),
		ConversationNumber: input.ConversationNumber, TargetID: strings.TrimSpace(input.TargetID), Status: StatusQueued,
		CreatedAt: now, LastActivityAt: now, CreatedFrom: strings.TrimSpace(input.CreatedFrom),
		ChannelProfileID: strings.TrimSpace(input.ChannelProfileID), ConversationID: strings.TrimSpace(input.ConversationID),
		ChannelType: strings.TrimSpace(input.ChannelType), ChannelAccountID: strings.TrimSpace(input.ChannelAccountID),
		ConversationType: strings.TrimSpace(input.ConversationType), ChatID: strings.TrimSpace(input.ChatID), TopicID: strings.TrimSpace(input.TopicID), UserID: strings.TrimSpace(input.UserID),
		ParentTaskNumber: input.ParentTaskNumber, RetryOfTaskNumber: input.RetryOfTaskNumber,
		ActionID: strings.TrimSpace(input.ActionID), ActionSourceTaskNumber: input.ActionSourceTaskNumber,
		DispatchState: DispatchNotDispatched, Metadata: cloneMetadata(input.Metadata),
	}
	if !validBackend(task.Backend) && task.Backend != "" {
		return Task{}, errors.New("任务 Backend 必须是 codex 或 openclaw")
	}
	if task.ConversationNumber < 0 {
		return Task{}, errors.New("ConversationNumber 无效")
	}
	if task.ParentTaskNumber < 0 || task.RetryOfTaskNumber < 0 {
		return Task{}, errors.New("父任务编号无效")
	}
	if err := validateTask(task); err != nil {
		return Task{}, err
	}
	if err := r.persistWithCandidateLocked(task, true); err != nil {
		return Task{}, fmt.Errorf("persist task: %w", err)
	}
	r.tasks[task.TaskNumber] = cloneTask(task)
	r.byID[task.TaskID] = task.TaskNumber
	r.nextTaskNumber++
	r.loadWarning = ""
	return cloneTask(task), nil
}

func (r *TaskRegistry) Update(task Task) (Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task.TaskNumber < 1 || strings.TrimSpace(task.TaskID) == "" {
		return Task{}, errors.New("任务标识无效")
	}
	existing, ok := r.tasks[task.TaskNumber]
	if !ok || existing.TaskID != task.TaskID {
		return Task{}, errors.New("任务不存在")
	}
	task = cloneTask(task)
	if err := validateTask(task); err != nil {
		return Task{}, err
	}
	if err := r.persistWithCandidateLocked(task, false); err != nil {
		return Task{}, fmt.Errorf("persist task: %w", err)
	}
	r.tasks[task.TaskNumber] = task
	r.loadWarning = ""
	return cloneTask(task), nil
}

func (r *TaskRegistry) Get(number int) (Task, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	task, ok := r.tasks[number]
	return cloneTask(task), ok
}

func (r *TaskRegistry) GetByID(id string) (Task, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	number, ok := r.byID[strings.TrimSpace(id)]
	if !ok {
		return Task{}, false
	}
	task, ok := r.tasks[number]
	return cloneTask(task), ok
}

func (r *TaskRegistry) List(filter TaskFilter) []Task {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status := strings.ToLower(strings.TrimSpace(filter.Status))
	search := strings.ToLower(strings.TrimSpace(filter.Search))
	result := make([]Task, 0, len(r.tasks))
	for _, task := range r.tasks {
		if status != "" && task.Status != status {
			continue
		}
		if search != "" && !taskMatchesSearch(task, search) {
			continue
		}
		result = append(result, cloneTask(task))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TaskNumber > result[j].TaskNumber })
	return result
}

func (r *TaskRegistry) ActiveForTarget(backend, targetID string) []Task {
	backend = normalizeBackend(backend)
	targetID = strings.TrimSpace(targetID)
	result := make([]Task, 0)
	for _, task := range r.List(TaskFilter{}) {
		if task.Backend == backend && task.TargetID == targetID && task.IsActive() {
			result = append(result, task)
		}
	}
	return result
}

func (r *TaskRegistry) HasActiveTasksForProject(projectID string) bool {
	for _, task := range r.List(TaskFilter{}) {
		if task.ProjectID == strings.TrimSpace(projectID) && task.IsActive() {
			return true
		}
	}
	return false
}

func (r *TaskRegistry) persistWithCandidateLocked(updated Task, isNew bool) error {
	candidate := make(map[int]Task, len(r.tasks)+1)
	for number, task := range r.tasks {
		candidate[number] = cloneTask(task)
	}
	candidate[updated.TaskNumber] = cloneTask(updated)
	next := r.nextTaskNumber
	if isNew {
		next = r.nextTaskNumber + 1
	}
	return writeAtomic(r.path, taskDiskModel{Version: schemaVersion, NextTaskNumber: next, Tasks: taskSlice(candidate)})
}

func normalizeStoredTask(task Task) (Task, error) {
	task.TaskID = strings.TrimSpace(task.TaskID)
	if task.TaskID == "" || task.TaskNumber < 1 {
		return Task{}, errors.New("taskId and taskNumber are required")
	}
	task.Backend = normalizeBackend(task.Backend)
	if task.Backend != "" && !validBackend(task.Backend) {
		return Task{}, errors.New("invalid backend")
	}
	if !validStatus(task.Status) {
		return Task{}, fmt.Errorf("invalid status %q", task.Status)
	}
	if task.DispatchState == "" {
		task.DispatchState = DispatchNotDispatched
	}
	if !validDispatchState(task.DispatchState) {
		return Task{}, fmt.Errorf("invalid dispatchState %q", task.DispatchState)
	}
	if task.CreatedAt == "" {
		task.CreatedAt = nowText()
	}
	if task.LastActivityAt == "" {
		task.LastActivityAt = task.CreatedAt
	}
	if task.Summary.ChangedFiles == nil {
		task.Summary.ChangedFiles = []string{}
	}
	if task.Summary.BuildResults == nil {
		task.Summary.BuildResults = []string{}
	}
	if task.Summary.TestResults == nil {
		task.Summary.TestResults = []string{}
	}
	task.Result.ChangedFiles = uniqueStrings(task.Result.ChangedFiles)
	task.Result.BuildResults = uniqueStrings(task.Result.BuildResults)
	task.Result.TestResults = uniqueStrings(task.Result.TestResults)
	task.Summary.ChangedFiles = uniqueStrings(task.Summary.ChangedFiles)
	task.Summary.BuildResults = uniqueStrings(task.Summary.BuildResults)
	task.Summary.TestResults = uniqueStrings(task.Summary.TestResults)
	return cloneTask(task), nil
}

func validateTask(task Task) error {
	if task.TaskID == "" || task.TaskNumber < 1 {
		return errors.New("taskId and taskNumber are required")
	}
	if strings.TrimSpace(task.Title) == "" || strings.TrimSpace(task.Description) == "" {
		return errors.New("任务标题和内容不能为空")
	}
	if !validStatus(task.Status) {
		return fmt.Errorf("任务状态无效：%s", task.Status)
	}
	if task.DispatchState == "" {
		task.DispatchState = DispatchNotDispatched
	}
	if !validDispatchState(task.DispatchState) {
		return fmt.Errorf("任务 DispatchState 无效：%s", task.DispatchState)
	}
	if task.Backend != "" && !validBackend(task.Backend) {
		return errors.New("任务 Backend 必须是 codex 或 openclaw")
	}
	if task.ActionSourceTaskNumber < 0 {
		return errors.New("ActionSourceTaskNumber 无效")
	}
	return nil
}

func taskSlice(tasks map[int]Task) []Task {
	result := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, cloneTask(task))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TaskNumber < result[j].TaskNumber })
	return result
}

func taskMatchesSearch(task Task, search string) bool {
	return strings.Contains(strings.ToLower(task.NumberLabel()), search) || strings.Contains(strings.ToLower(task.Title), search) ||
		strings.Contains(strings.ToLower(task.Description), search) || strings.Contains(strings.ToLower(task.ProjectNameSnapshot), search) ||
		strings.Contains(strings.ToLower(task.ProjectID), search)
}

func cloneTask(task Task) Task {
	task.Summary.ChangedFiles = append([]string(nil), task.Summary.ChangedFiles...)
	task.Summary.BuildResults = append([]string(nil), task.Summary.BuildResults...)
	task.Summary.TestResults = append([]string(nil), task.Summary.TestResults...)
	task.Result.ChangedFiles = append([]string(nil), task.Result.ChangedFiles...)
	task.Result.BuildResults = append([]string(nil), task.Result.BuildResults...)
	task.Result.TestResults = append([]string(nil), task.Result.TestResults...)
	task.Metadata = cloneMetadata(task.Metadata)
	return task
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func firstLine(value string) string {
	if index := strings.IndexAny(value, "\r\n"); index >= 0 {
		value = value[:index]
	}
	return strings.TrimSpace(value)
}

func newTaskID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "task-" + itoa(int(time.Now().UnixNano()))
	}
	return "task-" + hex.EncodeToString(buffer)
}
