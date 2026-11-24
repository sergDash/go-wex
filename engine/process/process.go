// Package process — подкоманды того же бинарника и управляемые дочерние
// процессы. Подкоманда выполняется вместо служб; процесс запускается в
// отдельной группе, получает мягкий сигнал, затем принудительный, а его
// вывод и код возврата попадают в журнал.
package process

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"sync"
	"syscall"
	"time"

	"go-wex/engine/journal"
)

// Handler — обработчик подкоманды: получает аргументы после её имени.
type Handler func(context.Context, []string) error

// Registry — подкоманды, известные приложению.
type Registry struct {
	mu       sync.Mutex
	commands map[string]Handler
}

// NewRegistry заводит пустой набор подкоманд.
func NewRegistry() *Registry {
	return &Registry{commands: map[string]Handler{}}
}

// Add объявляет подкоманду. Повторное имя — ошибка: два обработчика для
// одного режима делают выбор неявным.
func (r *Registry) Add(name string, h Handler) error {
	if name == "" {
		return errors.New("подкоманда: пустое имя")
	}
	if h == nil {
		return fmt.Errorf("подкоманда %q: обработчик не задан", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.commands[name]; ok {
		return fmt.Errorf("подкоманда %q уже объявлена", name)
	}
	r.commands[name] = h
	return nil
}

// Lookup отдаёт обработчик по имени.
func (r *Registry) Lookup(name string) (Handler, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.commands[name]
	return h, ok
}

// Names отдаёт имена подкоманд по возрастанию: сообщение об ошибке не должно
// зависеть от порядка регистрации.
func (r *Registry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, 0, len(r.commands))
	for name := range r.commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Spec — правило управляемого дочернего процесса.
//
// Path пустой означает текущий бинарник. Args — режим и его аргументы.
// Restart — задержка перед перезапуском; ноль означает, что отказ процесса
// не перезапускается. Grace — срок мягкой остановки, ноль — три секунды.
type Spec struct {
	Name    string
	Path    string
	Args    []string
	Restart time.Duration
	Grace   time.Duration
	Journal journal.Writer
}

// Service — управляемый дочерний процесс как внутренняя служба.
type Service struct {
	spec Spec

	mu      sync.Mutex
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

// running — один запуск процесса и его читатели вывода.
type running struct {
	cmd *exec.Cmd
	wg  sync.WaitGroup
}

// New собирает службу процесса.
func New(spec Spec) *Service {
	return &Service{spec: spec}
}

// Name — имя службы для группы.
func (s *Service) Name() string { return "process " + s.spec.Name }

// Start запускает процесс и возвращается, когда он создан. Дальше служба
// ждёт завершения и, если задано, перезапускает процесс.
func (s *Service) Start(ctx context.Context) error {
	if s.spec.Name == "" {
		return errors.New("процесс: пустое имя")
	}

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("процесс %s: уже запущен", s.spec.Name)
	}
	s.started = true
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done
	s.mu.Unlock()

	p, err := s.launch()
	if err != nil {
		cancel()
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return err
	}

	s.mu.Lock()
	s.cmd = p.cmd
	s.mu.Unlock()

	go s.supervise(runCtx, done, p)
	return nil
}

// Stop отменяет перезапуск, посылает процессу мягкий сигнал, затем
// принудительный через срок и ждёт завершения.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	started, cancel, done, cmd := s.started, s.cancel, s.done, s.cmd
	s.started = false
	s.cancel, s.done = nil, nil
	s.mu.Unlock()

	if !started {
		return nil
	}
	if cancel != nil {
		cancel()
	}
	if cmd != nil && cmd.Process != nil {
		_ = signalGroup(cmd.Process, syscall.SIGTERM)
	}

	grace := s.spec.Grace
	if grace <= 0 {
		grace = 3 * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		if cmd != nil && cmd.Process != nil {
			_ = signalGroup(cmd.Process, syscall.SIGKILL)
		}
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// supervise ждёт процесс, пишет код возврата и решает, перезапускать ли его.
func (s *Service) supervise(ctx context.Context, done chan struct{}, p *running) {
	defer close(done)

	for {
		// Сначала дочитываем вывод: Wait закрывает трубы и может потерять
		// строки, которые процесс успел записать перед выходом.
		p.wg.Wait()
		err := p.cmd.Wait()

		code := -1
		if p.cmd.ProcessState != nil {
			code = p.cmd.ProcessState.ExitCode()
		}
		fields := map[string]any{"process": s.spec.Name, "code": code}
		if err != nil {
			fields["error"] = err.Error()
		}
		s.writeJournal("exit", fields)

		if ctx.Err() != nil {
			return
		}
		if s.spec.Restart <= 0 {
			kind := "stopped"
			if code != 0 {
				kind = "failed"
			}
			s.writeJournal(kind, map[string]any{"process": s.spec.Name, "code": code})
			return
		}

		timer := time.NewTimer(s.spec.Restart)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		next, err := s.launch()
		if err != nil {
			s.writeJournal("launch", map[string]any{"process": s.spec.Name, "error": err.Error()})
			return
		}
		s.mu.Lock()
		s.cmd = next.cmd
		s.mu.Unlock()
		p = next
	}
}

// launch запускает один процесс в отдельной группе и читает его вывод.
func (s *Service) launch() (*running, error) {
	path := s.spec.Path
	if path == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("процесс %s: путь к бинарнику: %w", s.spec.Name, err)
		}
		path = exe
	}

	cmd := exec.Command(path, s.spec.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("процесс %s: stdout: %w", s.spec.Name, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("процесс %s: stderr: %w", s.spec.Name, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("процесс %s: запуск: %w", s.spec.Name, err)
	}

	p := &running{cmd: cmd}
	p.wg.Add(2)
	go s.scan(&p.wg, stdout, "stdout")
	go s.scan(&p.wg, stderr, "stderr")
	return p, nil
}

// scan пишет строки вывода процесса в журнал.
func (s *Service) scan(wg *sync.WaitGroup, r io.Reader, stream string) {
	defer wg.Done()

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		s.writeJournal("output", map[string]any{
			"process": s.spec.Name,
			"stream":  stream,
			"line":    scanner.Text(),
		})
	}
	if err := scanner.Err(); err != nil {
		s.writeJournal("scan", map[string]any{
			"process": s.spec.Name,
			"stream":  stream,
			"error":   err.Error(),
		})
	}
}

// writeJournal пишет событие и не мешает процессу, если журнал недоступен.
func (s *Service) writeJournal(kind string, fields map[string]any) {
	if s.spec.Journal == nil {
		return
	}
	if err := s.spec.Journal.Write("process", kind, fields); err != nil {
		log.Printf("журнал: %v", err)
	}
}

// signalGroup посылает сигнал всей группе процесса; если группа не создана,
// остаётся сигнал самому процессу.
func signalGroup(p *os.Process, sig syscall.Signal) error {
	if err := syscall.Kill(-p.Pid, sig); err != nil {
		return p.Signal(sig)
	}
	return nil
}
