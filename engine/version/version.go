// Package version — версия сборки: знак того, какой код запущен. Значения
// подставляются при сборке, а не читаются из файла, поэтому версия всегда
// совпадает с бинарником:
//
//	go build -ldflags "-X go-wex/engine/version.Version=1.2.3 \
//	  -X go-wex/engine/version.Commit=$(git rev-parse --short HEAD)" ./engine/app
//
// Версия нужна для двух вещей: сказать человеку, что запущено, и сверить
// родителя с дочерним процессом, который запущен тем же бинарником.
package version

var (
	// Version — версия сборки. Без ldflags остаётся "dev".
	Version = "dev"

	// Commit — коммит сборки. Без ldflags остаётся "none".
	Commit = "none"

	// Built — момент сборки. Без ldflags остаётся "unknown".
	Built = "unknown"
)

// String отдаёт версию для журнала и ответов: версия и коммит, если он есть.
func String() string {
	if Commit == "" || Commit == "none" {
		return Version
	}
	return Version + " (" + Commit + ")"
}
