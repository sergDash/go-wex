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
    var colors = ['красный', 'зелёный', 'синий', 'жёлтый'];

    // options переводит строки в пункты списка: компоненту нужны значение
    // и подпись, даже когда они совпадают.
    function options(list) {
        var out = [];

        for (var i = 0; i < list.length; i++) {
            out.push({ value: list[i], label: list[i] });
        }
        return out;
    }

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
        services: [],
        events: [],
        deletable: [],
        eventText: 'напоминание',
        eventSeconds: 10,
        selects: {
            deleteMode: { open: false, value: 'event', label: 'только событие' },
            deleteId: { open: false, value: '', label: '— событие —' },
            color: { open: false, value: 'зелёный', label: 'зелёный' },
            city: { open: false, value: '', label: '— город —', query: '' },
            tags: { open: false, value: [] }
        },
        colors: colors,
        colorItems: options(colors),
        componentColor: '',
        cities: ['Москва', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Екатеринбург'],
        cityMatches: ['Москва', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Екатеринбург'],
        tags: ['гость', 'подписчик', 'автор', 'редактор'],
        text: 'раз',
        failure: ''
    });

    // x-select — выпадающий список как компонент: разметка одна на все
    // экземпляры, состояние у каждого своё, выбранное значение уходит
    // родителю событием change. Внешняя функция метода вызывается с вью
    // в this, поэтому emit отправляется через сохранённую ссылку.
    var xSelect = $('#x-select')
        .state(function () {
            return { items: [], value: '', open: false };
        })
        .method('toggle', function (state) {
            return function () {
                state.open = !state.open;
            };
        })
        .method('pick', function (state) {
            var component = this;

            return function (event) {
                state.value = event.currentTarget.dataset.value;
                state.open = false;
                component.emit('change', state.value);
            };
        })
        .on('$mount', function (state) {
            return function () {
                // Клик мимо закрывает список: обработчик на документе,
                // клики внутри любого списка пропускаются.
                document.addEventListener('click', function (event) {
                    if (!event.target.closest('.select')) {
                        state.open = false;
                    }
                });
            };
        });

    view.component('x-select', xSelect);

    // onComponentColor принимает выбранное от компонента: значение приходит
    // аргументом события, а не через DOM.
    view.method('onComponentColor', function (state) {
        return function (value) {
            state.componentColor = value;
        };
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

    // eventState приводит событие к полям стейта: период и срок показываются
    // готовыми строками, а источник называется словом.
    function eventState(row) {
        return {
            id: row.id,
            kind: row.kind,
            period: row.period,
            next: momentText(row.next, '—'),
            declared: row.declared,
            source: row.declared ? 'код' : 'данные'
        };
    }

    // deletableRows — события, которые можно удалить: объявленные кодом
    // в список не попадают, они вернутся на следующем проходе.
    function deletableRows(rows) {
        var out = [];

        for (var i = 0; i < rows.length; i++) {
            if (!rows[i].declared) {
                out.push({ id: rows[i].id });
            }
        }
        return out;
    }

    // loadEvents перечитывает список событий: и объявленных кодом, и
    // добавленных формой.
    function loadEvents(state) {
        return call('/api/events').then(function (rows) {
            state.events = rows.map(eventState);
            state.deletable = deletableRows(rows);
            state.failure = '';
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

    // snapshot читает связь, снимок демо, очередь и список служб одним
    // заходом: запросы идут параллельно, шаблон перерисовывается один раз.
    function snapshot(state) {
        state.checking = true;
        state.status = 'проверяем связь с сервером…';

        Promise.all([
            call('/api/health'),
            call('/api/demo'),
            call('/api/demo/jobs'),
            call('/api/services'),
            call('/api/events')
        ])
            .then(function (values) {
                const health = values[0];
                const demo = values[1];
                const rows = values[2];
                const services = values[3];
                const events = values[4];

                state.checking = false;
                state.ok = health.ok;
                state.status = health.status;
                state.demo = demo;
                state.jobs = rows.map(rowState);
                state.services = services;
                state.events = events.map(eventState);
                state.deletable = deletableRows(events);
                state.failure = '';
            })
            .catch(function (error) {
                state.checking = false;
                state.ok = false;
                state.status = 'сервер не ответил: ' + error.message;
                state.failure = error.message;
            });
    }

    // check повторяет тот запрос, которым страница проверяет связь. Кнопка
    // нужна не для данных, а чтобы запрос было видно в инструментах
    // веб-разработчика: нажатие оставляет одну строку в сетевой панели.
    view.method('check', function (state) {
        return function () {
            state.checking = true;
            state.status = 'проверяем связь с сервером…';

            call('/api/health')
                .then(function (health) {
                    state.checking = false;
                    state.ok = health.ok;
                    state.status = health.status;
                })
                .catch(function (error) {
                    state.checking = false;
                    state.ok = false;
                    state.status = 'сервер не ответил: ' + error.message;
                });
        };
    });

    // addEvent заводит событие данными: период и текст работы. Сразу после
    // добавления обновляются и события, и очередь — событие пока не сработало,
    // но строка в таблице должна появиться.
    view.method('addEvent', function (state) {
        return function () {
            call('/api/events', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    text: state.eventText,
                    seconds: Number(state.eventSeconds)
                })
            })
                .then(function () {
                    return loadEvents(state);
                })
                .then(function () {
                    return loadJobs(state);
                })
                .catch(function (error) {
                    state.failure = error.message;
                });
        };
    });

    // deleteEvent убирает выбранное событие выбранным способом. По умолчанию
    // исчезает только событие; работы, которые оно поставило, остаются
    // в очереди. Выбор лежит в стейте, а не приходит аргументом: w-on
    // не передаёт параметров.
    view.method('deleteEvent', function (state) {
        return function () {
            var id = state.selects.deleteId.value;
            if (!id) {
                return;
            }
            var mode = state.selects.deleteMode.value;

            call('/api/events/' + encodeURIComponent(id) + '?mode=' + encodeURIComponent(mode), {
                method: 'DELETE'
            })
                .then(function () {
                    state.selects.deleteId.value = '';
                    state.selects.deleteId.label = '— событие —';
                    return loadEvents(state);
                })
                .catch(function (error) {
                    state.failure = error.message;
                });
        };
    });

    // filterCities отбирает города по введённой строке. Значение читается
    // из самого поля: вместе с w-bind это дало бы гонку, а так список
    // пересобирается в том же обработчике, где получен ввод.
    view.method('filterCities', function (state) {
        return function (event) {
            var query = event.currentTarget.value.trim().toLowerCase();
            var matches = [];

            for (var i = 0; i < state.cities.length; i++) {
                if (state.cities[i].toLowerCase().indexOf(query) >= 0) {
                    matches.push(state.cities[i]);
                }
            }
            state.cityMatches = matches;
        };
    });

    // toggleSelect открывает выпадающий список и закрывает остальные:
    // одновременно открытым может быть только один.
    view.method('toggleSelect', function (state) {
        return function (event) {
            var name = event.currentTarget.dataset.select;
            var open = state.selects[name].open;

            for (var key in state.selects) {
                state.selects[key].open = false;
            }
            state.selects[name].open = !open;
        };
    });

    // pickSelect записывает выбор. Значение и подпись берутся из data-атрибутов
    // пункта: w-on не передаёт аргументов, обработчик получает только событие.
    // Множественный выбор не закрывает список и переключает пункт.
    view.method('pickSelect', function (state) {
        return function (event) {
            var item = event.currentTarget;
            var name = item.dataset.select;
            var value = item.dataset.value;
            var select = state.selects[name];

            if (name === 'tags') {
                var picked = select.value;
                var next = [];
                var found = false;

                for (var i = 0; i < picked.length; i++) {
                    if (picked[i] === value) {
                        found = true;
                    } else {
                        next.push(picked[i]);
                    }
                }
                if (!found) {
                    next.push(value);
                }

                // Новый массив вместо splice: прокси видит замену значения,
                // и снятые пункты теряют подсветку.
                select.value = next;
                return;
            }

            select.value = value;
            select.label = item.textContent.trim();
            select.open = false;
        };
    });

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

    // w-focus ставит курсор в поле, когда оно появляется: поиск готов
    // к вводу сразу после открытия списка.
    view.directive('w-focus', null, function ($el) {
        $el[0].focus();
    });

    // Первый снимок читается сразу после появления страницы: $mount —
    // документированное место для начальной загрузки данных.
    view.on('$mount', function (state) {
        snapshot(state);
    });

    // Клик вне списка закрывает открытые. Слушатель на документе, потому что
    // списки разбросаны по странице; клик внутри списка пропускается,
    // иначе он закрыл бы тот список, в котором выбирают.
    document.addEventListener('click', function (event) {
        if (event.target.closest('.select')) {
            return;
        }
        view.update(function (state) {
            for (var key in state.selects) {
                state.selects[key].open = false;
            }
        });
    });

    view.mount();
});
