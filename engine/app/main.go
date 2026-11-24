// Точка сборки: читает настройки, находит интерфейс, собирает службы,
// перечисляет модули и запускает движок. Ни маршрутов, ни предметных правил
// здесь нет.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go-wex/domains/health"
	"go-wex/domains/tasks"
	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/service"
	"go-wex/engine/store"
)

func main() {
	cfgPath := flag.String("config", filepath.Join("config", "httpapi.json"), "файл настроек")
	webDir := flag.String("web", "", "переопределить каталог интерфейса из настроек")
	flag.Parse()

	cfg, err := config.LoadHTTPAPI(*cfgPath)
	if err != nil {
		log.Fatalf("%v", err)
	}

	dir := cfg.Interface.Dir
	if *webDir != "" {
		dir = *webDir
	}
	dir, err = resolveDir(dir)
	if err != nil {
		log.Fatalf("интерфейс: %v", err)
	}
	log.Printf("интерфейс: %s", dir)

	srv := httpapi.New(cfg.Server.Addr, os.DirFS(dir))

	// Хранилище открывается до служб: ошибка версии или повреждённого файла
	// должна быть видна сразу, а не при первом запросе.
	st, err := store.Open(cfg.Store.Path, nil)
	if err != nil {
		log.Fatalf("хранилище: %v", err)
	}

	// Виды работ и очередь общие для модулей: модуль объявляет вид, а ставит
	// работу через разборщик. Очередь поверх хранилища переживает перезапуск.
	jobReg := jobs.NewRegistry()
	queue, err := jobs.OpenStoreQueue(st)
	if err != nil {
		log.Fatalf("очередь: %v", err)
	}
	disp := jobs.NewDispatcher(queue, jobReg)

	// registry отдаёт модулю обе возможности: маршруты и работы.
	reg := registry{api: srv, jobs: jobReg}

	// Список модулей — единственное место, где приложение знает о них.
	modules := []core.Module{
		health.New(),
		tasks.New(disp),
	}
	for _, m := range modules {
		if err := m.Register(reg); err != nil {
			log.Fatalf("модуль %s: %v", m.Name(), err)
		}
		log.Printf("модуль: %s", m.Name())
	}

	// Разборщик стартует раньше HTTP, поэтому при остановке HTTP закрывается
	// первым: новые запросы уже не ставят работу в остановленную очередь.
	group := service.New(disp, srv)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("go+wex: http://%s", cfg.Server.Addr)
	if err := group.Run(ctx); err != nil {
		log.Fatalf("остановка: %v", err)
	}
}

// resolveDir выбирает каталог интерфейса.
//
// Явный -web используется как есть. Без флага проверяется значение из
// настроек рядом с каталогом запуска, на уровень выше и на два уровня выше:
// сервер запускают и из корня (go run ./engine/app), и из каталога сборки.
// Проверка идёт по index.html, чтобы ошибка была внятной сразу.
func resolveDir(dir string) (string, error) {
	candidates := []string{dir}
	if !filepath.IsAbs(dir) {
		candidates = append(candidates,
			filepath.Join("..", dir),
			filepath.Join("..", "..", dir),
		)
	}
	for _, c := range candidates {
		if err := checkIndex(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("каталог %q не найден; запустите из корня проекта или укажите -web", dir)
}

func checkIndex(dir string) error {
	info, err := os.Stat(filepath.Join(dir, "index.html"))
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%s/index.html — каталог", dir)
	}
	return nil
}

// registry собирает возможности модуля: маршруты уходят HTTP-службе,
// виды работ — реестру работ. Сам модуль знает только форму из core.
type registry struct {
	api  *httpapi.Server
	jobs *jobs.Registry
}

func (r registry) HTTP(method, path string, h core.Handler) {
	r.api.HTTP(method, path, h)
}

func (r registry) Job(kind string, spec jobs.Spec) error {
	return r.jobs.Add(jobs.Kind(kind), spec)
}
