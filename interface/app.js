// Поток данных страницы.
//
// 1. Стейт создаётся синхронно: связь не проверена, снимок пуст.
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
        job: null,
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

    // jobState приводит работу к полям стейта. Имена совпадают с ответом;
    // время показывается до секунд, а отсутствующее начало объясняется
    // словами, чтобы в таблице не оставалось пустого места.
    function jobState(job) {
        return {
            id: job.id,
            status: job.status,
            text: job.text,
            attempts: job.attempts,
            createdAt: momentText(job.createdAt, '—'),
            startedAt: momentText(job.startedAt, 'ещё не начиналась')
        };
    }

    // momentText — момент для показа в местном времени. Сервер отдаёт
    // полную точность, панели хватает секунд.
    function momentText(value, empty) {
        if (!value) {
            return empty;
        }
        return new Date(value).toLocaleString('ru-RU');
    }

    // snapshot читает связь и снимок демо одним заходом: оба запроса идут
    // параллельно, шаблон перерисовывается один раз.
    function snapshot(state) {
        state.checking = true;

        Promise.all([call('/api/health'), call('/api/demo')])
            .then(function (values) {
                const health = values[0];
                const demo = values[1];

                state.checking = false;
                state.ok = health.ok;
                state.status = health.status;
                state.demo = demo;
                state.failure = '';
            })
            .catch(function (error) {
                state.checking = false;
                state.ok = false;
                state.status = 'сервер не ответил: ' + error.message;
            });
    }

    // refresh перечитывает снимок по кнопке: опроса нет.
    view.method('refresh', function (state) {
        return function () {
            snapshot(state);
        };
    });

    // enqueue ставит работу и сразу показывает её состояние: ответ уже
    // содержит номер, статус и времена.
    view.method('enqueue', function (state) {
        return function () {
            call('/api/demo/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text: state.text })
            })
                .then(function (job) {
                    state.job = jobState(job);
                    state.failure = '';
                })
                .catch(function (error) {
                    state.failure = error.message;
                });
        };
    });

    // refreshJob перечитывает работу по номеру. Пока разборщик её не взял,
    // время начала пустое; после выполнения оно остаётся.
    view.method('refreshJob', function (state) {
        return function () {
            if (!state.job) {
                return;
            }

            call('/api/demo/jobs/' + state.job.id)
                .then(function (job) {
                    state.job = jobState(job);
                    state.failure = '';
                })
                .catch(function (error) {
                    state.failure = error.message;
                });
        };
    });

    // Первый снимок читается сразу после появления страницы: $mount —
    // документированное место для начальной загрузки данных.
    view.on('$mount', function (state) {
        snapshot(state);
    });

    view.mount();
});
