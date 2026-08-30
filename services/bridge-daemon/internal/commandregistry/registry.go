package commandregistry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const SchemaVersion = 1

const (
	ActionBridgeStart       = "bridge.start"
	ActionBridgeHelp        = "bridge.help"
	ActionBridgeStatus      = "bridge.status"
	ActionThreadsList       = "threads.list"
	ActionThreadInfo        = "thread.info"
	ActionThreadHistory     = "thread.history"
	ActionThreadRunning     = "thread.running"
	ActionThreadWaiting     = "thread.waiting"
	ActionThreadRecent      = "thread.recent"
	ActionThreadFailed      = "thread.failed"
	ActionAccountQuota      = "account.quota"
	ActionThreadBind        = "thread.bind"
	ActionThreadUnbind      = "thread.unbind"
	ActionThreadCurrent     = "thread.current"
	ActionThreadStop        = "thread.stop"
	ActionInteractionCancel = "interaction.cancel"
	ActionOpenClawRefresh   = "openclaw.sessions.refresh"
)

const (
	BackendCapabilityCodex    = "codex"
	BackendCapabilityOpenClaw = "openclaw"
	BackendCapabilityBoth     = "both"
)

var (
	ErrNotFound = errors.New("指令不存在")
	ErrLocked   = errors.New("系统指令已锁定，请先解锁")
)

type ActionDefinition struct {
	ID                string `json:"id"`
	DisplayName       string `json:"displayName"`
	TargetSupport     bool   `json:"targetSupport"`
	BackendCapability string `json:"backendCapability"`
}

type DefaultCommandDefinition struct {
	ID                       string
	DefaultName              string
	DefaultDisplayName       string
	DefaultDescription       string
	DefaultAction            string
	DefaultAliases           []string
	DefaultEnabled           bool
	DefaultParameterHelp     string
	DefaultTelegramMenuLabel string
}

type Definition struct {
	ID                   string   `json:"id"`
	Name                 string   `json:"name"`
	DisplayName          string   `json:"displayName"`
	Aliases              []string `json:"aliases"`
	Description          string   `json:"description"`
	ParameterHelp        string   `json:"parameterHelp"`
	Action               string   `json:"action"`
	BackendCapability    string   `json:"backendCapability"`
	BuiltIn              bool     `json:"builtIn"`
	Locked               bool     `json:"locked"`
	Enabled              bool     `json:"enabled"`
	Modified             bool     `json:"modified"`
	CanDelete            bool     `json:"canDelete"`
	CanRestore           bool     `json:"canRestore"`
	TelegramMenuEligible bool     `json:"telegramMenuEligible"`
	TelegramMenuNotice   string   `json:"telegramMenuNotice,omitempty"`
	CreatedAt            string   `json:"createdAt,omitempty"`
	UpdatedAt            string   `json:"updatedAt,omitempty"`
}

type Mutation struct {
	Name          string   `json:"name"`
	DisplayName   string   `json:"displayName"`
	Aliases       []string `json:"aliases"`
	Description   string   `json:"description"`
	ParameterHelp string   `json:"parameterHelp"`
	Action        string   `json:"action"`
	Enabled       *bool    `json:"enabled"`
}

type Invocation struct {
	Definition Definition
	Trigger    string
	Arguments  []string
}

type ListResponse struct {
	SchemaVersion int                `json:"schemaVersion"`
	Commands      []Definition       `json:"commands"`
	Actions       []ActionDefinition `json:"actions"`
}

type builtInOverride struct {
	Name          string   `json:"name"`
	DisplayName   string   `json:"displayName"`
	Aliases       []string `json:"aliases"`
	Description   string   `json:"description"`
	ParameterHelp string   `json:"parameterHelp"`
	Enabled       bool     `json:"enabled"`
	Locked        bool     `json:"locked"`
	UpdatedAt     string   `json:"updatedAt"`
}

type customRecord struct {
	Current  Definition `json:"current"`
	Baseline Definition `json:"createdBaseline"`
}

type diskModel struct {
	SchemaVersion    int                        `json:"schemaVersion"`
	BuiltInOverrides map[string]builtInOverride `json:"builtInOverrides"`
	CustomCommands   []customRecord             `json:"customCommands"`
}

type Registry struct {
	mu        sync.RWMutex
	path      string
	defaults  []DefaultCommandDefinition
	actions   []ActionDefinition
	overrides map[string]builtInOverride
	customs   map[string]customRecord
	listeners []func()
}

func DefaultActions() []ActionDefinition {
	return []ActionDefinition{
		{ID: ActionBridgeStart, DisplayName: "查看机器人就绪状态", BackendCapability: BackendCapabilityBoth},
		{ID: ActionBridgeHelp, DisplayName: "查看指令帮助", BackendCapability: BackendCapabilityBoth},
		{ID: ActionBridgeStatus, DisplayName: "查看 Bridge 状态", TargetSupport: true, BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadsList, DisplayName: "查看会话列表", BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadInfo, DisplayName: "查看会话详情", TargetSupport: true, BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadHistory, DisplayName: "查看聊天记录", TargetSupport: true, BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadRunning, DisplayName: "查看正在执行", BackendCapability: BackendCapabilityCodex},
		{ID: ActionThreadWaiting, DisplayName: "查看等待处理", BackendCapability: BackendCapabilityCodex},
		{ID: ActionThreadRecent, DisplayName: "查看最近活动", BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadFailed, DisplayName: "查看失败任务", BackendCapability: BackendCapabilityCodex},
		{ID: ActionAccountQuota, DisplayName: "查看额度", BackendCapability: BackendCapabilityCodex},
		{ID: ActionThreadBind, DisplayName: "绑定会话", BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadUnbind, DisplayName: "解除绑定", BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadCurrent, DisplayName: "查看当前绑定", BackendCapability: BackendCapabilityBoth},
		{ID: ActionThreadStop, DisplayName: "停止任务", TargetSupport: true, BackendCapability: BackendCapabilityBoth},
		{ID: ActionInteractionCancel, DisplayName: "取消等待", TargetSupport: true, BackendCapability: BackendCapabilityCodex},
		{ID: ActionOpenClawRefresh, DisplayName: "刷新 OpenClaw Session", BackendCapability: BackendCapabilityOpenClaw},
	}
}

func BuiltInDefaults() []DefaultCommandDefinition {
	return []DefaultCommandDefinition{
		{ID: "builtin.start", DefaultName: "/start", DefaultDisplayName: "开始使用", DefaultDescription: "查看机器人是否可用及当前绑定状态", DefaultAction: ActionBridgeStart, DefaultEnabled: true, DefaultTelegramMenuLabel: "开始使用"},
		{ID: "builtin.help", DefaultName: "/help", DefaultDisplayName: "指令帮助", DefaultDescription: "查看当前已启用的远程指令", DefaultAction: ActionBridgeHelp, DefaultAliases: []string{"/commands"}, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看指令帮助"},
		{ID: "builtin.status", DefaultName: "/status", DefaultDisplayName: "查看状态", DefaultDescription: "查看 Bridge 与当前后端连接状态；配合 #编号查看指定会话", DefaultAction: ActionBridgeStatus, DefaultEnabled: true, DefaultParameterHelp: "[聊天编号]", DefaultTelegramMenuLabel: "查看连接状态"},
		{ID: "builtin.threads", DefaultName: "/threads", DefaultDisplayName: "查看会话列表", DefaultDescription: "查看当前后端的会话编号、标题和状态", DefaultAction: ActionThreadsList, DefaultEnabled: true, DefaultParameterHelp: "[页码]", DefaultTelegramMenuLabel: "查看会话列表"},
		{ID: "builtin.thread", DefaultName: "/thread", DefaultDisplayName: "会话详情", DefaultDescription: "查看当前后端指定或当前会话的状态、模型和更新时间", DefaultAction: ActionThreadInfo, DefaultEnabled: true, DefaultParameterHelp: "[聊天编号]", DefaultTelegramMenuLabel: "查看会话详情"},
		{ID: "builtin.history", DefaultName: "/history", DefaultDisplayName: "聊天记录", DefaultDescription: "查看当前后端指定或当前会话最近几轮聊天记录", DefaultAction: ActionThreadHistory, DefaultEnabled: true, DefaultParameterHelp: "[聊天编号] [数量]", DefaultTelegramMenuLabel: "查看聊天记录"},
		{ID: "builtin.running", DefaultName: "/running", DefaultDisplayName: "查看正在执行", DefaultDescription: "查看当前正在执行的 Codex 任务", DefaultAction: ActionThreadRunning, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看正在执行"},
		{ID: "builtin.waiting", DefaultName: "/waiting", DefaultDisplayName: "查看等待处理", DefaultDescription: "查看等待用户回答或桌面端审批的 Codex 会话", DefaultAction: ActionThreadWaiting, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看等待处理"},
		{ID: "builtin.recent", DefaultName: "/recent", DefaultDisplayName: "最近活动", DefaultDescription: "查看当前后端最近有活动的会话", DefaultAction: ActionThreadRecent, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看最近活动"},
		{ID: "builtin.failed", DefaultName: "/failed", DefaultDisplayName: "查看失败任务", DefaultDescription: "查看最近失败的 Codex 任务", DefaultAction: ActionThreadFailed, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看失败任务"},
		{ID: "builtin.quota", DefaultName: "/quota", DefaultDisplayName: "查看额度", DefaultDescription: "查看 Codex 使用额度", DefaultAction: ActionAccountQuota, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看使用额度"},
		{ID: "builtin.bind", DefaultName: "/bind", DefaultDisplayName: "绑定会话", DefaultDescription: "将当前远程聊天关联到当前后端的会话；也可使用 oc:<SessionKey>", DefaultAction: ActionThreadBind, DefaultEnabled: true, DefaultParameterHelp: "<编号或 ID>", DefaultTelegramMenuLabel: "绑定会话"},
		{ID: "builtin.unbind", DefaultName: "/unbind", DefaultDisplayName: "解除绑定", DefaultDescription: "解除当前远程聊天的会话绑定", DefaultAction: ActionThreadUnbind, DefaultEnabled: true, DefaultTelegramMenuLabel: "解除绑定"},
		{ID: "builtin.current", DefaultName: "/current", DefaultDisplayName: "当前绑定", DefaultDescription: "查看当前远程聊天绑定的后端会话", DefaultAction: ActionThreadCurrent, DefaultEnabled: true, DefaultTelegramMenuLabel: "查看当前绑定"},
		{ID: "builtin.stop", DefaultName: "/stop", DefaultDisplayName: "停止任务", DefaultDescription: "停止当前聊天或指定会话发起的任务", DefaultAction: ActionThreadStop, DefaultEnabled: true, DefaultParameterHelp: "[聊天编号]", DefaultTelegramMenuLabel: "停止任务"},
		{ID: "builtin.cancel", DefaultName: "/cancel", DefaultDisplayName: "取消等待", DefaultDescription: "取消当前正在等待的 Codex 用户输入", DefaultAction: ActionInteractionCancel, DefaultEnabled: true, DefaultParameterHelp: "[聊天编号]", DefaultTelegramMenuLabel: "取消等待"},
		{ID: "builtin.openclaw-refresh", DefaultName: "/oc-refresh", DefaultDisplayName: "刷新 OpenClaw Session", DefaultDescription: "OpenClaw 专属：重新从 Gateway 刷新 Session 列表", DefaultAction: ActionOpenClawRefresh, DefaultEnabled: true, DefaultTelegramMenuLabel: "刷新 OpenClaw Session"},
	}
}

func New(path string) (*Registry, error) {
	r := &Registry{path: path, defaults: BuiltInDefaults(), actions: DefaultActions(), overrides: map[string]builtInOverride{}, customs: map[string]customRecord{}}
	if err := r.load(); err != nil {
		return nil, err
	}
	if err := r.validateAllLocked(r.effectiveLocked()); err != nil {
		return nil, fmt.Errorf("validate commands: %w", err)
	}
	return r, nil
}

func NewInMemory() *Registry {
	return &Registry{defaults: BuiltInDefaults(), actions: DefaultActions(), overrides: map[string]builtInOverride{}, customs: map[string]customRecord{}}
}

func (r *Registry) AddChangeListener(listener func()) {
	if listener == nil {
		return
	}
	r.mu.Lock()
	r.listeners = append(r.listeners, listener)
	r.mu.Unlock()
}

func (r *Registry) List() ListResponse {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return ListResponse{SchemaVersion: SchemaVersion, Commands: r.effectiveLocked(), Actions: append([]ActionDefinition(nil), r.actions...)}
}

func (r *Registry) ListForBackend(backend string) ListResponse {
	backend = normalizeBackendCapability(backend)
	result := r.List()
	if backend == "" {
		return result
	}
	filtered := make([]Definition, 0, len(result.Commands))
	for _, command := range result.Commands {
		if SupportsBackend(command.BackendCapability, backend) {
			filtered = append(filtered, command)
		}
	}
	result.Commands = filtered
	actions := make([]ActionDefinition, 0, len(result.Actions))
	for _, action := range result.Actions {
		if SupportsBackend(action.BackendCapability, backend) {
			actions = append(actions, action)
		}
	}
	result.Actions = actions
	return result
}

func (r *Registry) Resolve(text string) (Invocation, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return Invocation{}, false
	}
	trigger := normalizeTrigger(strings.SplitN(fields[0], "@", 2)[0])
	r.mu.RLock()
	definitions := r.effectiveLocked()
	r.mu.RUnlock()
	for _, definition := range definitions {
		for _, candidate := range append([]string{definition.Name}, definition.Aliases...) {
			if normalizeTrigger(candidate) == trigger {
				return Invocation{Definition: definition, Trigger: fields[0], Arguments: append([]string(nil), fields[1:]...)}, true
			}
		}
	}
	return Invocation{}, false
}

func (r *Registry) Get(id string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, item := range r.effectiveLocked() {
		if item.ID == strings.TrimSpace(id) {
			return item, true
		}
	}
	return Definition{}, false
}

func (r *Registry) Create(input Mutation) (Definition, error) {
	r.mu.Lock()
	definition, err := r.definitionFromMutation(input, false)
	if err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	definition.ID, definition.BuiltIn, definition.Locked, definition.CreatedAt, definition.UpdatedAt = newID(), false, false, now, now
	definition.CanDelete, definition.CanRestore = true, true
	candidate := cloneCustoms(r.customs)
	candidate[definition.ID] = customRecord{Current: definition, Baseline: definition}
	if err := r.validateCandidateLocked(r.overrides, candidate); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	if err := r.saveStateLocked(r.overrides, candidate); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	r.customs = candidate
	listeners := append([]func(){}, r.listeners...)
	r.mu.Unlock()
	notify(listeners)
	return definition, nil
}

func (r *Registry) Update(id string, input Mutation) (Definition, error) {
	r.mu.Lock()
	id = strings.TrimSpace(id)
	current, builtIn, ok := r.findLocked(id)
	if !ok {
		r.mu.Unlock()
		return Definition{}, ErrNotFound
	}
	if builtIn && current.Locked {
		r.mu.Unlock()
		return Definition{}, ErrLocked
	}
	updated, err := r.definitionFromMutation(input, builtIn)
	if err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	updated.ID, updated.BuiltIn, updated.Locked, updated.CreatedAt = current.ID, builtIn, current.Locked, current.CreatedAt
	updated.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	updated.CanDelete, updated.CanRestore = !builtIn, true
	updated.Modified = true
	if builtIn {
		updated.Action = current.Action
	}
	overrides, customs := cloneOverrides(r.overrides), cloneCustoms(r.customs)
	if builtIn {
		overrides[id] = builtInOverride{Name: updated.Name, DisplayName: updated.DisplayName, Aliases: updated.Aliases, Description: updated.Description, ParameterHelp: updated.ParameterHelp, Enabled: updated.Enabled, Locked: updated.Locked, UpdatedAt: updated.UpdatedAt}
	} else {
		record := customs[id]
		record.Current = updated
		customs[id] = record
	}
	if err := r.validateCandidateLocked(overrides, customs); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	if err := r.saveStateLocked(overrides, customs); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	r.overrides, r.customs = overrides, customs
	listeners := append([]func(){}, r.listeners...)
	r.mu.Unlock()
	notify(listeners)
	return updated, nil
}

func (r *Registry) SetLocked(id string, locked bool) (Definition, error) {
	r.mu.Lock()
	id = strings.TrimSpace(id)
	current, builtIn, ok := r.findLocked(id)
	if !ok {
		r.mu.Unlock()
		return Definition{}, ErrNotFound
	}
	if !builtIn {
		r.mu.Unlock()
		return Definition{}, errors.New("自定义指令不支持锁定")
	}
	overrides := cloneOverrides(r.overrides)
	item, exists := overrides[id]
	if !exists {
		item = overrideFromDefinition(current)
	}
	item.Locked, item.UpdatedAt = locked, time.Now().UTC().Format(time.RFC3339Nano)
	overrides[id] = item
	if err := r.saveStateLocked(overrides, r.customs); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	r.overrides = overrides
	current.Locked, current.Modified, current.UpdatedAt = locked, true, item.UpdatedAt
	listeners := append([]func(){}, r.listeners...)
	r.mu.Unlock()
	notify(listeners)
	return current, nil
}

func (r *Registry) Delete(id string) error {
	r.mu.Lock()
	id = strings.TrimSpace(id)
	_, builtIn, ok := r.findLocked(id)
	if !ok {
		r.mu.Unlock()
		return ErrNotFound
	}
	if builtIn {
		r.mu.Unlock()
		return errors.New("系统指令不能删除，可解锁后停用")
	}
	customs := cloneCustoms(r.customs)
	delete(customs, id)
	if err := r.saveStateLocked(r.overrides, customs); err != nil {
		r.mu.Unlock()
		return err
	}
	r.customs = customs
	listeners := append([]func(){}, r.listeners...)
	r.mu.Unlock()
	notify(listeners)
	return nil
}

func (r *Registry) Restore(id string) (Definition, error) {
	r.mu.Lock()
	id = strings.TrimSpace(id)
	_, builtIn, ok := r.findLocked(id)
	if !ok {
		r.mu.Unlock()
		return Definition{}, ErrNotFound
	}
	overrides, customs := cloneOverrides(r.overrides), cloneCustoms(r.customs)
	if builtIn {
		delete(overrides, id)
	} else {
		record := customs[id]
		record.Current = record.Baseline
		customs[id] = record
	}
	if err := r.validateCandidateLocked(overrides, customs); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	if err := r.saveStateLocked(overrides, customs); err != nil {
		r.mu.Unlock()
		return Definition{}, err
	}
	r.overrides, r.customs = overrides, customs
	result, _, _ := r.findLocked(id)
	listeners := append([]func(){}, r.listeners...)
	r.mu.Unlock()
	notify(listeners)
	return result, nil
}

func (r *Registry) HelpText() string {
	return r.helpTextForDefinitions(r.ListForBackend(BackendCapabilityCodex).Commands, BackendCapabilityCodex)
}

func (r *Registry) HelpTextForBackend(backend string) string {
	return r.helpTextForDefinitions(r.ListForBackend(backend).Commands, backend)
}

func (r *Registry) helpTextForDefinitions(list []Definition, backend string) string {
	lines := []string{"可用指令："}
	for _, item := range list {
		if !item.Enabled {
			continue
		}
		line := item.Name
		if item.ParameterHelp != "" {
			line += " " + item.ParameterHelp
		}
		lines = append(lines, "", line)
		if len(item.Aliases) > 0 {
			lines = append(lines, "别名："+strings.Join(item.Aliases, "、"))
		}
		lines = append(lines, item.Description)
	}
	sample := "#63 继续修改这个功能"
	if normalizeBackendCapability(backend) == BackendCapabilityOpenClaw {
		sample = "#12 继续处理这个任务"
	}
	return strings.Join(append(lines, "", "发送任务：", sample), "\n")
}

func (r *Registry) SupportsTarget(action string) bool {
	for _, item := range r.actions {
		if item.ID == action {
			return item.TargetSupport
		}
	}
	return false
}

func (r *Registry) ActionSupportsBackend(action, backend string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, item := range r.actions {
		if item.ID == action {
			return SupportsBackend(item.BackendCapability, backend)
		}
	}
	return false
}

func SupportsBackend(capability, backend string) bool {
	capability = normalizeBackendCapability(capability)
	backend = normalizeBackendCapability(backend)
	return capability == BackendCapabilityBoth || capability == backend
}

func normalizeBackendCapability(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case BackendCapabilityCodex:
		return BackendCapabilityCodex
	case BackendCapabilityOpenClaw:
		return BackendCapabilityOpenClaw
	case BackendCapabilityBoth:
		return BackendCapabilityBoth
	default:
		return ""
	}
}

func capabilityForAction(action string, actions []ActionDefinition) string {
	for _, item := range actions {
		if item.ID == action {
			capability := normalizeBackendCapability(item.BackendCapability)
			if capability != "" {
				return capability
			}
		}
	}
	return BackendCapabilityCodex
}

func (r *Registry) effectiveLocked() []Definition {
	return effective(r.defaults, r.actions, r.overrides, r.customs)
}

func effective(defaults []DefaultCommandDefinition, actions []ActionDefinition, overrides map[string]builtInOverride, customs map[string]customRecord) []Definition {
	result := make([]Definition, 0, len(defaults)+len(customs))
	for _, item := range defaults {
		definition := Definition{ID: item.ID, Name: item.DefaultName, DisplayName: item.DefaultDisplayName, Aliases: append([]string{}, item.DefaultAliases...), Description: item.DefaultDescription, ParameterHelp: item.DefaultParameterHelp, Action: item.DefaultAction, BackendCapability: capabilityForAction(item.DefaultAction, actions), BuiltIn: true, Locked: true, Enabled: item.DefaultEnabled, CanRestore: true}
		if override, ok := overrides[item.ID]; ok {
			definition.Name, definition.DisplayName, definition.Aliases = override.Name, override.DisplayName, append([]string{}, override.Aliases...)
			definition.Description, definition.ParameterHelp, definition.Enabled, definition.Locked = override.Description, override.ParameterHelp, override.Enabled, override.Locked
			definition.Modified, definition.UpdatedAt = true, override.UpdatedAt
		}
		applyTelegramEligibility(&definition)
		result = append(result, definition)
	}
	customList := make([]Definition, 0, len(customs))
	for _, record := range customs {
		item := record.Current
		item.Aliases = append([]string{}, item.Aliases...)
		item.BackendCapability = capabilityForAction(item.Action, actions)
		item.BuiltIn, item.Locked, item.CanDelete, item.CanRestore = false, false, true, true
		applyTelegramEligibility(&item)
		customList = append(customList, item)
	}
	sort.Slice(customList, func(i, j int) bool {
		if customList[i].CreatedAt == customList[j].CreatedAt {
			return customList[i].ID < customList[j].ID
		}
		return customList[i].CreatedAt < customList[j].CreatedAt
	})
	return append(result, customList...)
}

func (r *Registry) findLocked(id string) (Definition, bool, bool) {
	for _, item := range r.effectiveLocked() {
		if item.ID == id {
			return item, item.BuiltIn, true
		}
	}
	return Definition{}, false, false
}

func (r *Registry) definitionFromMutation(input Mutation, builtIn bool) (Definition, error) {
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	definition := Definition{Name: strings.TrimSpace(input.Name), DisplayName: strings.TrimSpace(input.DisplayName), Aliases: normalizeAliases(input.Aliases), Description: strings.TrimSpace(input.Description), ParameterHelp: strings.TrimSpace(input.ParameterHelp), Action: strings.TrimSpace(input.Action), BuiltIn: builtIn, Enabled: enabled}
	if definition.DisplayName == "" {
		definition.DisplayName = definition.Description
	}
	if err := validateDefinition(definition, r.actions); err != nil {
		return Definition{}, err
	}
	applyTelegramEligibility(&definition)
	definition.BackendCapability = capabilityForAction(definition.Action, r.actions)
	return definition, nil
}

func (r *Registry) validateCandidateLocked(overrides map[string]builtInOverride, customs map[string]customRecord) error {
	return r.validateAllLocked(effective(r.defaults, r.actions, overrides, customs))
}

func (r *Registry) validateAllLocked(definitions []Definition) error {
	used := map[string]Definition{}
	for _, item := range definitions {
		if err := validateDefinition(item, r.actions); err != nil {
			return fmt.Errorf("%s: %w", item.ID, err)
		}
		local := map[string]bool{}
		for _, trigger := range append([]string{item.Name}, item.Aliases...) {
			key := normalizeTrigger(trigger)
			if local[key] {
				return fmt.Errorf("指令 %s 在同一条记录中重复", trigger)
			}
			local[key] = true
			if prior, exists := used[key]; exists && prior.ID != item.ID {
				return fmt.Errorf("指令 %s 已被“%s”使用", trigger, prior.DisplayName)
			}
			used[key] = item
		}
	}
	return nil
}

func validateDefinition(item Definition, actions []ActionDefinition) error {
	if err := validateTrigger(item.Name); err != nil {
		return err
	}
	for _, alias := range item.Aliases {
		if err := validateTrigger(alias); err != nil {
			return fmt.Errorf("别名 %s：%w", alias, err)
		}
	}
	if item.DisplayName == "" {
		return errors.New("显示名称不能为空")
	}
	if item.Description == "" {
		return errors.New("说明不能为空")
	}
	for _, action := range actions {
		if action.ID == item.Action {
			return nil
		}
	}
	return errors.New("请选择软件支持的功能")
}

func validateTrigger(value string) error {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "/") || len([]rune(value)) < 2 {
		return errors.New("指令名称必须以 / 开头")
	}
	if strings.ContainsAny(value, " \t\r\n@#") {
		return errors.New("指令名称不能包含空格、@ 或 #")
	}
	return nil
}

func normalizeAliases(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := normalizeTrigger(value)
		if value != "" && !seen[key] {
			seen[key] = true
			result = append(result, value)
		}
	}
	return result
}

func normalizeTrigger(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func applyTelegramEligibility(item *Definition) {
	item.TelegramMenuEligible = false
	for _, trigger := range append([]string{item.Name}, item.Aliases...) {
		if TelegramMenuTriggerEligible(trigger) {
			item.TelegramMenuEligible = true
			break
		}
	}
	if !item.TelegramMenuEligible {
		item.TelegramMenuNotice = "此指令可在聊天中使用，但不能显示在 Telegram 指令菜单中。"
	} else {
		item.TelegramMenuNotice = ""
	}
}

func TelegramMenuTriggerEligible(value string) bool {
	value = strings.TrimPrefix(strings.TrimSpace(value), "/")
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, char := range value {
		if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

func (r *Registry) load() error {
	data, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read commands: %w", err)
	}
	var model diskModel
	if err := json.Unmarshal(data, &model); err != nil {
		return fmt.Errorf("decode commands: %w", err)
	}
	if model.SchemaVersion != SchemaVersion {
		return fmt.Errorf("decode commands: unsupported schemaVersion %d", model.SchemaVersion)
	}
	if model.BuiltInOverrides != nil {
		r.overrides = model.BuiltInOverrides
	}
	for _, record := range model.CustomCommands {
		if record.Current.ID == "" || record.Baseline.ID != record.Current.ID {
			return errors.New("decode commands: invalid custom baseline")
		}
		r.customs[record.Current.ID] = record
	}
	return nil
}

func (r *Registry) saveStateLocked(overrides map[string]builtInOverride, customs map[string]customRecord) error {
	model := diskModel{SchemaVersion: SchemaVersion, BuiltInOverrides: overrides, CustomCommands: make([]customRecord, 0, len(customs))}
	for _, item := range customs {
		model.CustomCommands = append(model.CustomCommands, item)
	}
	sort.Slice(model.CustomCommands, func(i, j int) bool { return model.CustomCommands[i].Current.ID < model.CustomCommands[j].Current.ID })
	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	temporary, backup := r.path+".tmp", r.path+".bak"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return err
	}
	_ = os.Remove(backup)
	if _, err := os.Stat(r.path); err == nil {
		if err := os.Rename(r.path, backup); err != nil {
			_ = os.Remove(temporary)
			return err
		}
	}
	if err := os.Rename(temporary, r.path); err != nil {
		_ = os.Rename(backup, r.path)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

func overrideFromDefinition(item Definition) builtInOverride {
	return builtInOverride{Name: item.Name, DisplayName: item.DisplayName, Aliases: append([]string(nil), item.Aliases...), Description: item.Description, ParameterHelp: item.ParameterHelp, Enabled: item.Enabled, Locked: item.Locked, UpdatedAt: item.UpdatedAt}
}
func cloneOverrides(source map[string]builtInOverride) map[string]builtInOverride {
	result := make(map[string]builtInOverride, len(source))
	for id, item := range source {
		item.Aliases = append([]string{}, item.Aliases...)
		result[id] = item
	}
	return result
}
func cloneCustoms(source map[string]customRecord) map[string]customRecord {
	result := make(map[string]customRecord, len(source))
	for id, record := range source {
		record.Current.Aliases = append([]string{}, record.Current.Aliases...)
		record.Baseline.Aliases = append([]string{}, record.Baseline.Aliases...)
		result[id] = record
	}
	return result
}
func notify(listeners []func()) {
	for _, listener := range listeners {
		listener()
	}
}
func newID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("custom-%d", time.Now().UnixNano())
	}
	return "custom-" + hex.EncodeToString(buffer)
}
