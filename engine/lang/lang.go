// Пакет lang — словари переводов: файлы по локалям и контекстам, слияние
// с базовой локалью и запасной путь для незнакомого языка.
//
// Раскладка каталога:
//
//	lang/ru-RU/common.json     — подписи самой страницы
//	lang/ru-RU/jobs.json       — раздел интерфейса «Работа в очереди»
//	lang/ru-RU/events.json     — раздел «Отложенные события»
//
// Имя файла — контекст: раздел интерфейса или набор записей, который
// запрашивают вместе. Ключи внутри контекста плоские и короткие, поэтому
// два раздела могут называть свои записи одинаково: в ответе они лежат
// под контекстом и не сталкиваются.
//
// Механизм не знает, какие контексты бывают и какие в них ключи: он читает
// каталог, сливает локали и отдаёт данные. Поэтому словари годятся и
// интерфейсу, и модулям, которым нужны фразы для ответов или записей.
package lang

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Locale — язык в списке доступных: код и как язык называет себя сам.
// Имя берётся из файла этой же локали, поэтому переключатель показывает
// каждый язык на нём, а не на текущем.
type Locale struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// Bundle — словарь для одного запроса: фактический язык, запрошенный язык,
// тексты по контекстам и ключи, которых не нашлось в запрошенной локали.
//
// Lang и Requested расходятся, когда запрошенного языка нет и сработал
// запасной путь: клиент ставит <html lang> по Lang, то есть по тому языку,
// на котором текст действительно пришёл.
type Bundle struct {
	Lang      string                       `json:"lang"`
	Requested string                       `json:"requested"`
	Texts     map[string]map[string]string `json:"texts"`
	Missing   []string                     `json:"missing"`
}

// Catalog — каталог словарей: известные локали и базовая локаль.
//
// Файлы читаются на каждый запрос, а не один раз при запуске: словари лежат
// рядом с приложением и правятся без пересборки, как и статика интерфейса.
// Файлы маленькие, поэтому цена чтения — работа операционной системы.
type Catalog struct {
	dir  string
	base string
}

// Имена проверяются до обращения к файлам: значение приходит из адреса
// запроса, а путь к файлу собирается из него.
var (
	localeName  = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$`)
	contextName = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// Open открывает каталог словарей. base — базовая локаль: она должна
// существовать, потому что на неё опирается запасной путь.
func Open(dir, base string) (*Catalog, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("каталог словарей не задан")
	}
	if !localeName.MatchString(base) {
		return nil, fmt.Errorf("базовая локаль %q: ожидается вид ru-RU", base)
	}

	info, err := os.Stat(filepath.Join(dir, base))
	if err != nil {
		return nil, fmt.Errorf("базовая локаль %s: %w", base, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("базовая локаль %s: это файл, а не каталог", base)
	}

	c := &Catalog{dir: dir, base: base}
	// Ошибка разбора видна при запуске, а не при первом запросе страницы:
	// сломанный словарь иначе проявился бы посреди работы.
	if _, err := c.Locales(); err != nil {
		return nil, err
	}
	return c, nil
}

// Base — базовая локаль каталога.
func (c *Catalog) Base() string { return c.base }

// Locales перечисляет локали каталога. Название языка берётся из его файла
// common.json; если файла или ключа нет, языком считается его код.
func (c *Catalog) Locales() ([]Locale, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, fmt.Errorf("словари: %w", err)
	}

	out := make([]Locale, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		code := entry.Name()
		if !localeName.MatchString(code) {
			continue
		}

		texts, err := c.readContext(code, "common")
		if err != nil {
			return nil, err
		}
		name := texts["language_name"]
		if name == "" {
			name = code
		}
		out = append(out, Locale{Code: code, Name: name})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}

// Load собирает словарь для запрошенного языка. Пустой список контекстов
// означает «все контексты»: объединение файлов запрошенной локали и базовой,
// иначе страница на языке без перевода раздела потеряла бы раздел целиком.
//
// Недостающий в локали ключ берётся из базовой локали, а сам ключ попадает
// в Missing: на экране остаётся осмысленный текст, а расхождение словарей
// видно по ответу.
func (c *Catalog) Load(requested string, contexts []string) (Bundle, error) {
	if requested != "" && !localeName.MatchString(requested) {
		return Bundle{}, badf("язык %q: ожидается вид ru-RU", requested)
	}

	locale := c.resolve(requested)
	names, err := c.contextNames(locale, contexts)
	if err != nil {
		return Bundle{}, err
	}

	bundle := Bundle{
		Lang:      locale,
		Requested: requested,
		Texts:     make(map[string]map[string]string, len(names)),
		Missing:   []string{},
	}

	for _, name := range names {
		base, err := c.readContext(c.base, name)
		if err != nil {
			return Bundle{}, err
		}
		local := map[string]string{}
		if locale != c.base {
			local, err = c.readContext(locale, name)
			if err != nil {
				return Bundle{}, err
			}
		}

		texts := make(map[string]string, len(base)+len(local))
		for key, value := range base {
			texts[key] = value
			if _, ok := local[key]; !ok && locale != c.base {
				bundle.Missing = append(bundle.Missing, name+"."+key)
			}
		}
		for key, value := range local {
			texts[key] = value
		}
		bundle.Texts[name] = texts
	}

	sort.Strings(bundle.Missing)
	return bundle, nil
}

// contextNames отбирает контексты запроса. Без списка берутся все файлы
// обеих локалей; со списком каждый проверяется по файлам, чтобы опечатка
// в имени раздела давала ошибку, а не пустой раздел.
func (c *Catalog) contextNames(locale string, contexts []string) ([]string, error) {
	if len(contexts) > 0 {
		names := make([]string, 0, len(contexts))
		for _, name := range contexts {
			if !contextName.MatchString(name) {
				return nil, badf("контекст %q: только строчные латинские буквы, цифры и подчёркивание", name)
			}
			if !c.hasContext(locale, name) && !c.hasContext(c.base, name) {
				return nil, badf("контекст %q не найден", name)
			}
			names = append(names, name)
		}
		return names, nil
	}

	all := make(map[string]bool)
	for _, dir := range c.implemented(locale) {
		all[dir] = true
	}
	for _, dir := range c.implemented(c.base) {
		all[dir] = true
	}

	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// implemented — контексты, файлы которых есть в локали.
func (c *Catalog) implemented(locale string) []string {
	entries, err := os.ReadDir(filepath.Join(c.dir, locale))
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		out = append(out, strings.TrimSuffix(entry.Name(), ".json"))
	}
	return out
}

// hasContext сообщает, есть ли файл контекста в локали.
func (c *Catalog) hasContext(locale, name string) bool {
	info, err := os.Stat(filepath.Join(c.dir, locale, name+".json"))
	return err == nil && !info.IsDir()
}

// resolve выбирает локаль с запасным путём: точный код, затем язык без
// региона (de-AT → de), затем базовая локаль. Так запрос с незнакомым
// регионом получает хоть какой-то знакомый язык, а не пустой экран.
func (c *Catalog) resolve(requested string) string {
	if requested == "" {
		return c.base
	}
	if c.hasLocale(requested) {
		return requested
	}
	if i := strings.Index(requested, "-"); i > 0 {
		if language := requested[:i]; c.hasLocale(language) {
			return language
		}
	}
	return c.base
}

// hasLocale сообщает, есть ли каталог локали.
func (c *Catalog) hasLocale(code string) bool {
	info, err := os.Stat(filepath.Join(c.dir, code))
	return err == nil && info.IsDir()
}

// readContext читает файл контекста. Отсутствие файла — это не ошибка:
// контекст может быть объявлен только в другой локали. Ошибка разбора
// возвращается всегда: сломанный словарь не должен подменяться тишиной.
func (c *Catalog) readContext(locale, name string) (map[string]string, error) {
	path := filepath.Join(c.dir, locale, name+".json")

	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("словарь %s: %w", path, err)
	}

	texts := map[string]string{}
	if err := json.Unmarshal(raw, &texts); err != nil {
		return nil, fmt.Errorf("словарь %s: %w", path, err)
	}
	return texts, nil
}
