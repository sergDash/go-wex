package lang

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go-wex/engine/httpapi"
)

// writeLocale раскладывает словари во временном каталоге: локаль, контекст,
// ключи. Так же выглядят файлы в lang/, поэтому проверки читают то же, что
// и приложение.
func writeLocale(t *testing.T, dir, locale, context string, texts map[string]string) {
	t.Helper()

	path := filepath.Join(dir, locale)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("каталог локали: %v", err)
	}
	raw, err := json.Marshal(texts)
	if err != nil {
		t.Fatalf("сборка словаря: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path, context+".json"), raw, 0o644); err != nil {
		t.Fatalf("файл словаря: %v", err)
	}
}

// TestLoadMergesBase — недостающий ключ берётся из базовой локали, а сам
// ключ попадает в missing: на экране текст, а расхождение словарей видно.
func TestLoadMergesBase(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"title": "Панель", "lead": "Пояснение"})
	writeLocale(t, dir, "en-GB", "common", map[string]string{"title": "Panel"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	bundle, err := c.Load("en-GB", []string{"common"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if bundle.Lang != "en-GB" || bundle.Requested != "en-GB" {
		t.Fatalf("язык: %+v", bundle)
	}
	if got := bundle.Texts["common"]["title"]; got != "Panel" {
		t.Fatalf("перевод: %q", got)
	}
	if got := bundle.Texts["common"]["lead"]; got != "Пояснение" {
		t.Fatalf("запасной текст: %q", got)
	}
	if len(bundle.Missing) != 1 || bundle.Missing[0] != "common.lead" {
		t.Fatalf("missing: %v", bundle.Missing)
	}
}

// TestFallbackForUnknownLocale — незнакомый регион сводится к языку, а если
// и языка нет — к базовому. Ответ называет язык, на котором текст пришёл.
func TestFallbackForUnknownLocale(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"title": "Панель"})
	writeLocale(t, dir, "de", "common", map[string]string{"title": "Tafel"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	byLanguage, err := c.Load("de-AT", nil)
	if err != nil {
		t.Fatalf("Load de-AT: %v", err)
	}
	if byLanguage.Lang != "de" || byLanguage.Texts["common"]["title"] != "Tafel" {
		t.Fatalf("сведение к языку: %+v", byLanguage)
	}

	byBase, err := c.Load("fr-CA", nil)
	if err != nil {
		t.Fatalf("Load fr-CA: %v", err)
	}
	if byBase.Lang != "ru-RU" || byBase.Requested != "fr-CA" {
		t.Fatalf("базовая локаль: %+v", byBase)
	}
	if byBase.Texts["common"]["title"] != "Панель" {
		t.Fatalf("текст базовой локали: %+v", byBase.Texts)
	}
}

// TestLoadAllContexts — без перечня отдаются все контексты обеих локалей:
// иначе страница на языке без перевода раздела потеряла бы раздел целиком.
func TestLoadAllContexts(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"title": "Панель"})
	writeLocale(t, dir, "ru-RU", "jobs", map[string]string{"title": "Очередь"})
	writeLocale(t, dir, "en-GB", "common", map[string]string{"title": "Panel"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	bundle, err := c.Load("en-GB", nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(bundle.Texts) != 2 {
		t.Fatalf("контексты: %+v", bundle.Texts)
	}
	if bundle.Texts["jobs"]["title"] != "Очередь" {
		t.Fatalf("контекст из базовой локали: %+v", bundle.Texts["jobs"])
	}
}

// TestUnknownContextIsBadRequest — опечатка в имени раздела даёт отказ
// входа, а не пустой раздел: иначе страница молча осталась бы без текстов.
func TestUnknownContextIsBadRequest(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"title": "Панель"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_, err = c.Load("ru-RU", []string{"nope"})
	var bad *badRequest
	if !errors.As(err, &bad) {
		t.Fatalf("ожидался отказ входа, получили %v", err)
	}
}

// TestOpenFailsOnBrokenJSON — сломанный словарь виден при запуске, а не
// посреди работы страницы.
func TestOpenFailsOnBrokenJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ru-RU"), 0o755); err != nil {
		t.Fatalf("каталог: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ru-RU", "common.json"), []byte("{"), 0o644); err != nil {
		t.Fatalf("файл: %v", err)
	}

	if _, err := Open(dir, "ru-RU"); err == nil {
		t.Fatal("сломанный словарь принят")
	}
}

// TestLocalesNamesItself — каждый язык называет себя сам: переключатель
// должен быть понятен тому, кто уже видит чужой язык.
func TestLocalesNamesItself(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"language_name": "Русский"})
	writeLocale(t, dir, "en-GB", "common", map[string]string{"language_name": "English"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	locales, err := c.Locales()
	if err != nil {
		t.Fatalf("Locales: %v", err)
	}
	if len(locales) != 2 {
		t.Fatalf("список: %+v", locales)
	}
	if locales[0].Code != "en-GB" || locales[0].Name != "English" {
		t.Fatalf("порядок и имя: %+v", locales)
	}
}

// TestRoutes — маршруты отдают конверт движка и понимают адрес вида
// /api/lang/ru-RU/jobs,events: перечень контекстов через запятую.
func TestRoutes(t *testing.T) {
	dir := t.TempDir()
	writeLocale(t, dir, "ru-RU", "common", map[string]string{"title": "Панель"})
	writeLocale(t, dir, "ru-RU", "jobs", map[string]string{"title": "Очередь"})
	writeLocale(t, dir, "ru-RU", "events", map[string]string{"title": "События"})

	c, err := Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	srv := httpapi.New("127.0.0.1:0", os.DirFS(dir))
	c.Register(srv)

	listRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(listRec, httptest.NewRequest(http.MethodGet, "/api/lang", nil))
	if listRec.Code != http.StatusOK {
		t.Fatalf("список: код %d, тело %s", listRec.Code, listRec.Body.String())
	}
	var listBody struct {
		Result struct {
			Base    string   `json:"base"`
			Locales []Locale `json:"locales"`
		} `json:"result"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("разбор списка: %v", err)
	}
	if listBody.Result.Base != "ru-RU" || len(listBody.Result.Locales) != 1 {
		t.Fatalf("список: %s", listRec.Body.String())
	}

	partRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(partRec, httptest.NewRequest(http.MethodGet, "/api/lang/ru-RU/jobs,events", nil))
	if partRec.Code != http.StatusOK {
		t.Fatalf("часть контекстов: код %d, тело %s", partRec.Code, partRec.Body.String())
	}
	var partBody struct {
		Result Bundle `json:"result"`
	}
	if err := json.Unmarshal(partRec.Body.Bytes(), &partBody); err != nil {
		t.Fatalf("разбор части: %v", err)
	}
	if len(partBody.Result.Texts) != 2 {
		t.Fatalf("контексты: %s", partRec.Body.String())
	}
	if _, ok := partBody.Result.Texts["common"]; ok {
		t.Fatalf("лишний контекст: %s", partRec.Body.String())
	}

	badRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(badRec, httptest.NewRequest(http.MethodGet, "/api/lang/ru-RU/nope", nil))
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("неизвестный контекст: код %d", badRec.Code)
	}
}
