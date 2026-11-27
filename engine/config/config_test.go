package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write кладёт временный файл настроек и возвращает путь.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("запись настроек: %v", err)
	}
	return path
}

// TestLoad — корректный файл читается целиком.
func TestLoadHTTPAPI(t *testing.T) {
	path := write(t, `{
		"server": {"addr": "127.0.0.1:9000", "open": true},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"modules": {"disabled": ["demo"]}
	}`)

	c, err := LoadHTTPAPI(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.Addr != "127.0.0.1:9000" {
		t.Fatalf("адрес: %q", c.Server.Addr)
	}
	if !c.Server.Open {
		t.Fatal("открытие браузера не прочиталось")
	}
	if len(c.Modules.Disabled) != 1 || c.Modules.Disabled[0] != "demo" {
		t.Fatalf("выключенные модули: %v", c.Modules.Disabled)
	}
	if c.Interface.Dir != "interface" {
		t.Fatalf("каталог: %q", c.Interface.Dir)
	}
	if c.Store.Path != "data/store.json" {
		t.Fatalf("хранилище: %q", c.Store.Path)
	}
}

// TestUnknownKey — опечатка в имени настройки даёт ошибку, а не тишину.
func TestUnknownKey(t *testing.T) {
	path := write(t, `{
		"server": {"addr": "127.0.0.1:9000", "adr": "опечатка"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"}
	}`)

	_, err := LoadHTTPAPI(path)
	if err == nil {
		t.Fatal("жали ошибку неизвестного ключа")
	}
	if !strings.Contains(err.Error(), "adr") {
		t.Fatalf("ошибка не называет ключ: %v", err)
	}
}

// TestEmptyAddr — пустой адрес не допускается.
func TestEmptyAddr(t *testing.T) {
	path := write(t, `{
		"server": {"addr": ""},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"}
	}`)

	_, err := LoadHTTPAPI(path)
	if err == nil {
		t.Fatal("жали ошибку пустого адреса")
	}
	if !strings.Contains(err.Error(), "server.addr") {
		t.Fatalf("ошибка не называет поле: %v", err)
	}
}

// TestShippedConfig — файл из поставки читается: пример не расходится
// с загрузчиком.
func TestShippedConfig(t *testing.T) {
	if _, err := LoadHTTPAPI(filepath.Join("..", "..", "config", "httpapi.json")); err != nil {
		t.Fatalf("config/httpapi.json: %v", err)
	}
}

// TestEmptyStorePath — пустой путь к хранилищу не допускается.
func TestEmptyStorePath(t *testing.T) {
	path := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": ""}
	}`)

	_, err := LoadHTTPAPI(path)
	if err == nil {
		t.Fatal("жали ошибку пустого пути хранилища")
	}
	if !strings.Contains(err.Error(), "store.path") {
		t.Fatalf("ошибка не называет поле: %v", err)
	}
}

// TestScheduleRules — корректные пороги читаются, неверный срок и
// неизвестный класс отвергаются.
func TestScheduleRules(t *testing.T) {
	good := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"schedule": {"rules": [
			{"after": "5m", "class": "background"},
			{"after": "0s", "class": "interactive"}
		]}
	}`)
	c, err := LoadHTTPAPI(good)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Schedule.Rules) != 2 {
		t.Fatalf("порогов: %d", len(c.Schedule.Rules))
	}

	badTime := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"schedule": {"rules": [{"after": "пять минут", "class": "background"}]}
	}`)
	if _, err := LoadHTTPAPI(badTime); err == nil || !strings.Contains(err.Error(), "schedule.rules[0].after") {
		t.Fatalf("неверный срок: %v", err)
	}

	badClass := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"schedule": {"rules": [{"after": "5m", "class": "urgent"}]}
	}`)
	if _, err := LoadHTTPAPI(badClass); err == nil || !strings.Contains(err.Error(), "schedule.rules[0].class") {
		t.Fatalf("неизвестный класс: %v", err)
	}
}

// TestJournalSettings — журнал читается, пустой путь и нулевые пределы
// отвергаются.
func TestJournalSettings(t *testing.T) {
	good := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"journal": {"path": "data/journal.log", "maxSize": 1024, "keep": 3}
	}`)
	c, err := LoadHTTPAPI(good)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Journal == nil || c.Journal.Path != "data/journal.log" || c.Journal.MaxSize != 1024 || c.Journal.Keep != 3 {
		t.Fatalf("журнал: %+v", c.Journal)
	}

	badPath := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"journal": {"path": "", "maxSize": 1024, "keep": 3}
	}`)
	if _, err := LoadHTTPAPI(badPath); err == nil || !strings.Contains(err.Error(), "journal.path") {
		t.Fatalf("пустой путь: %v", err)
	}

	badSize := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"},
		"journal": {"path": "data/journal.log", "maxSize": 0, "keep": 3}
	}`)
	if _, err := LoadHTTPAPI(badSize); err == nil || !strings.Contains(err.Error(), "journal.maxSize") {
		t.Fatalf("нулевой размер: %v", err)
	}
}

// TestTransportSettings — сроки транспорта читаются и проверяются: опечатка
// в сроке и отрицательный предел тела дают ошибку.
func TestTransportSettings(t *testing.T) {
	ok := write(t, `{
		"server": {
			"addr": "127.0.0.1:9000",
			"readHeaderTimeout": "3s",
			"idleTimeout": "90s",
			"maxBody": 1048576
		},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"}
	}`)
	c, err := LoadHTTPAPI(ok)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Server.ReadHeaderTimeout != "3s" || c.Server.IdleTimeout != "90s" {
		t.Fatalf("транспорт: %+v", c.Server)
	}
	if c.Server.MaxBody == nil || *c.Server.MaxBody != 1048576 {
		t.Fatalf("предел тела: %v", c.Server.MaxBody)
	}

	for name, body := range map[string]string{
		"срок": `{
			"server": {"addr": "127.0.0.1:9000", "readHeaderTimeout": "три секунды"},
			"interface": {"dir": "interface"},
			"store": {"path": "data/store.json"}
		}`,
		"ноль": `{
			"server": {"addr": "127.0.0.1:9000", "idleTimeout": "0s"},
			"interface": {"dir": "interface"},
			"store": {"path": "data/store.json"}
		}`,
		"предел": `{
			"server": {"addr": "127.0.0.1:9000", "maxBody": -1},
			"interface": {"dir": "interface"},
			"store": {"path": "data/store.json"}
		}`,
	} {
		if _, err := LoadHTTPAPI(write(t, body)); err == nil {
			t.Fatalf("%s: ждали ошибку", name)
		}
	}
}

// TestLangDefaults — пустой раздел lang означает «включено с умолчаниями»:
// каталог lang и базовая локаль ru-RU. Без раздела переводов нет вовсе.
func TestLangDefaults(t *testing.T) {
	path := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"lang": {},
		"store": {"path": "data/store.json"}
	}`)

	c, err := LoadHTTPAPI(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Lang == nil {
		t.Fatal("раздел lang потерян")
	}
	if c.Lang.Dir != DefaultLangDir || c.Lang.Base != DefaultLangBase {
		t.Fatalf("умолчания: %+v", *c.Lang)
	}

	none := write(t, `{
		"server": {"addr": "127.0.0.1:9000"},
		"interface": {"dir": "interface"},
		"store": {"path": "data/store.json"}
	}`)
	c, err = LoadHTTPAPI(none)
	if err != nil {
		t.Fatalf("Load без раздела: %v", err)
	}
	if c.Lang != nil {
		t.Fatalf("переводы включены без раздела: %+v", *c.Lang)
	}
}
