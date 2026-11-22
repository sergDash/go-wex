// Точка входа: собирает HTTP-обработчик из готовых пакетов и запускает сервер.
// Здесь нет ни доменной логики, ни маршрутов — только выбор источника фронтенда
// и параметры запуска.
package main

import (
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"

	"go-wex/internal/httpapi"
	"go-wex/web"
)

func main() {
	// -addr: адрес прослушивания. По умолчанию только локальная машина,
	// чтобы запуск скелета не открывал порт в сеть.
	addr := flag.String("addr", "127.0.0.1:8080", "адрес HTTP-сервера")

	// -web: каталог фронтенда на диске. Пустое значение — встроенная в бинарник
	// копия. Диск удобен при правке HTML/CSS/JS: не нужно пересобирать Go.
	webDir := flag.String("web", "", "каталог фронтенда вместо встроенной копии")
	flag.Parse()

	var assets fs.FS = web.FS
	if *webDir != "" {
		assets = os.DirFS(*webDir)
	}

	handler := httpapi.New(assets)

	log.Printf("go+wex: http://%s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatalf("HTTP-сервер завершился: %v", err)
	}
}
