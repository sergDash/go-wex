// Package config — настройки приложения из файлов config/<предмет>.json.
// Здесь настройки HTTP-службы (адрес и каталог интерфейса) и путь к файлу
// хранилища. Проверка идёт при загрузке: неизвестный ключ и пустое
// обязательное значение считаются ошибкой, чтобы приложение не работало
// с половиной настроек.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// HTTPAPI — настройки HTTP-службы.
//
// Server.Addr — адрес, который занимает служба. Interface.Dir — каталог
// статики, откуда читаются файлы интерфейса. Store.Path — файл хранилища,
// в котором очередь переживает перезапуск.
type HTTPAPI struct {
	Server    Server    `json:"server"`
	Interface Interface `json:"interface"`
	Store     Store     `json:"store"`
}

// Server — настройки адреса HTTP-службы.
type Server struct {
	Addr string `json:"addr"`
}

// Interface — настройки каталога статики.
type Interface struct {
	Dir string `json:"dir"`
}

// Store — настройки файла хранилища.
type Store struct {
	Path string `json:"path"`
}

// LoadHTTPAPI читает настройки HTTP-службы по пути path. Неизвестные ключи
// запрещены: опечатка в имени настройки должна давать ошибку, а не тихо
// игнорироваться.
func LoadHTTPAPI(path string) (HTTPAPI, error) {
	f, err := os.Open(path)
	if err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки: %w", err)
	}
	defer f.Close()

	var c HTTPAPI
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки %s: %w", path, err)
	}
	return c, nil
}

// validate проверяет обязательные значения. Список короткий: адрес сервера,
// каталог интерфейса и путь к хранилищу, без которых приложение не запустится.
func (c HTTPAPI) validate() error {
	if strings.TrimSpace(c.Server.Addr) == "" {
		return errors.New("server.addr обязателен")
	}
	if strings.TrimSpace(c.Interface.Dir) == "" {
		return errors.New("interface.dir обязателен")
	}
	if strings.TrimSpace(c.Store.Path) == "" {
		return errors.New("store.path обязателен")
	}
	return nil
}
