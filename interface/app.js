// Поток данных страницы.
//
// 1. Стейт создаётся синхронно: связь не проверена, снимок пуст, очередь пуста.
// 2. mount() вставляет шаблон #app в DOM и подписывает его на стейт.
// 3. call() — единственное место разбора конверта: сначала error, затем
//    result, как в Go сначала проверяют err, а потом берут значение.
// 4. Ответ ложится в поля стейта, и wex перерисовывает изменившиеся узлы.
//    Обновление идёт по кнопке, без опроса: канал обновлений — сценарий
//    SC-16, он ещё не сделан.
$(function () {
    var view = $('#app').state({
        checking: true,
        ok: false,
        status: 'проверяем связь с сервером…',
        demo: {
            tcp: '',
            udp: '',
            lines: 0,
            event: { kind: '', period: '', fired: 0 }
        },
        jobs: [],
        text: 'раз',
        failure: ''
    });

    // call разбирает конверт и отдаёт только result; ошибка становится
    // исключением, чтобы её ловил один общий catch на каждый запрос.
    function call(url, options) {
        return fetch(url, options)
            .then(function (response) {
                return response.json();
            })
            .then(function (body) {
                const { result, error } = body;

                if (error) {
                    throw new Error(error);
                }
                return result;
            });
    }

    // rowState приводит строку очереди к полям стейта. Имена совпадают
    // с ответом; время показывается до секунд, а пустое место
    // в столбце «Начата» — прочерком.
    function rowState(row) {
        return {
            id: row.id,
            kind: row.kind,
            status: row.status,
            text: row.text,
            attempts: row.attempts,
            createdAt: momentText(row.createdAt, '—'),
            startedAt: momentText(row.startedAt, '—')
        };
    }

    // momentText — момент для показа в местном времени. Сервер отдаёт
    // полную точность, панели хватает секунд, а год в таблице повторяется
    // в каждой строке и только отнимает ширину.
    function momentText(value, empty) {
        if (!value) {
            return empty;
        }
        return new Date(value).toLocaleString('ru-RU', {
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    // loadJobs перечитывает очередь целиком. Строки идут сверху вниз
    // в том порядке, который отдал сервер: он ставит новые работы первыми.
    function loadJobs(state) {
        return call('/api/demo/jobs')
            .then(function (rows) {
                state.jobs = rows.map(rowState);
                state.failure = '';
            });
    }

    // snapshot читает связь, снимок демо и очередь одним заходом: запросы
    // идут параллельно, шаблон перерисовывается один раз.
    function snapshot(state) {
        state.checking = true;

        Promise.all([call('/api/health'), call('/api/demo'), call('/api/demo/jobs')])
            .then(function (values) {
                const health = values[0];
                const demo = values[1];
                const rows = values[2];

                state.checking = false;
                state.ok = health.ok;
                state.status = health.status;
                state.demo = demo;
                state.jobs = rows.map(rowState);
                state.failure = '';
            })
            .catch(function (error) {
                state.checking = false;
                state.ok = false;
                state.status = 'сервер не ответил: ' + error.message;
                state.failure = error.message;
            });
    }

    // refresh перечитывает снимок по кнопке: опроса нет.
    view.method('refresh', function (state) {
        return function () {
            snapshot(state);
        };
    });

    // enqueue ставит работу и перечитывает очередь: поставленная строка
    // появляется сверху вместе со своим статусом и временами.
    view.method('enqueue', function (state) {
        return function () {
            call('/api/demo/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text: state.text })
            })
                .then(function () {
                    return loadJobs(state);
                })
                .catch(function (error) {
                    state.failure = error.message;
                });
        };
    });

    // refreshQueue перечитывает только очередь: снимок демо и связь
    // при этом не запрашиваются.
    view.method('refreshQueue', function (state) {
        return function () {
            loadJobs(state).catch(function (error) {
                state.failure = error.message;
            });
        };
    });

    // w-stick — держать прокрутку в конце: таблица растёт вниз, и после
    // каждого обновления нужно видеть последние строки. Директива живёт
    // в разметке, логика по-прежнему меняет только стейт.
    view.directive('w-stick', null, function ($el) {
        $el[0].scrollTop = $el[0].scrollHeight;
    });

    // Первый снимок читается сразу после появления страницы: $mount —
    // документированное место для начальной загрузки данных.
    view.on('$mount', function (state) {
        snapshot(state);
    });

    view.mount();
});
