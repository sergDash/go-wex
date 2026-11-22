// Точка входа: находит фронтенд, собирает HTTP-обработчик и запускает сервер.
// Здесь нет ни доменной логики, ни маршрутов.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"go-wex/internal/httpapi"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "адрес HTTP-сервера")
	webDir := flag.String("web", "", "каталог фронтенда; по умолчанию web-interface")
	flag.Parse()

	dir, err := resolveWebDir(*webDir)
	if err != nil {
		log.Fatalf("фронтенд: %v", err)
	}
	log.Printf("фронтенд: %s", dir)

	handler := httpapi.New(os.DirFS(dir))

	log.Printf("go+wex: http://%s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatalf("HTTP-сервер завершился: %v", err)
	}
}

// resolveWebDir выбирает каталог со статикой.
//
// Явный -web используется как есть. Без флага проверяются два положения,
// в которых обычно оказывается сервер: корень проекта (там же bin/app)
// и сам web-server/ (запуск через go run из каталога модуля). Проверка
// идёт по index.html, чтобы ошибка была внятной сразу, а не 404 на странице.
func resolveWebDir(explicit string) (string, error) {
	if explicit != "" {
		if err := checkIndex(explicit); err != nil {
			return "", fmt.Errorf("-web %s: %w", explicit, err)
		}
		return explicit, nil
	}

	candidates := []string{
		"web-interface",
		filepath.Join("..", "web-interface"),
	}
	for _, dir := range candidates {
		if err := checkIndex(dir); err == nil {
			return dir, nil
		}
	}

	return "", fmt.Errorf("каталог web-interface не найден; запустите из корня проекта или укажите -web")
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
