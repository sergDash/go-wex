// Поток данных страницы.
//
// 1. Стейт создаётся синхронно: связь не проверена, снимок пуст, очередь пуста.
// 2. mount() вставляет шаблон #app в DOM и подписывает его на стейт.
// 3. call() — единственное место разбора конверта: сначала error, затем
//    result, как в Go сначала проверяют err, а потом берут значение.
// 4. Ответ ложится в поля стейта, и wex перерисовывает изменившиеся узлы.
//    Обновление идёт по кнопке, без опроса: канал обновлений — сценарий
//    SC-16, он ещё не сделан.
//
// --- Аудит wex, 2026-11-26 ---
//
// Помеченные ниже места («Аудит wex») опираются на поведение ядра wex,
// а не на договорённость из книги. Это НЕ задание на переделку: сейчас всё
// работает, код остаётся как есть. Это список для перепроверки, если wex
// обновят: сначала прочитать эти пункты, потом проверять списки руками.
//
// Проверено на сборке wex 3.1.1 из interface/vendor/wex.min.js
// (sha256 06a13649…30f4; файл побайтно совпадает с dist форка sergDash/wex,
// коммит a584ca0; в апстриме wemmbley/wex ветка 3.x, коммит c3926b2 —
// поведение в перечисленных местах то же).
//
// Что перепроверять после обновления:
// 1. w-on подставляет элемент, на котором нашёлся обработчик, в
//    event.currentTarget. На этом стоят все pick в компонентах.
// 2. Хуки $init/$mount/$unmount принимают одноуровневую функцию
//    .on('$mount', function (state) {...}), в this — вью экземпляра.
//    Двухуровневая форма (return function () {}) у хуков молчит; поэтому
//    watchOutside и висит на первом toggle, а не на $mount.
// 3. Пропс кладётся в состояние по ссылке, поэтому массив из :items общий
//    с родителем; ключ, которого нет в состоянии компонента, отбрасывается
//    молча — опечатка в имени пропса ничего не сообщает.
// 4. Значения пунктов проходят через data-value и возвращаются строкой,
//    хотя в items они могут быть числами.
//
// Быстрая проверка после обновления: открыть панель, по очереди открыть все
// три списка, выбрать пункт в каждом, кликнуть мимо. Подпись выбранного
// должна меняться, в консоли — ни одной ошибки; в инструментах разработчика
// видно, что на документе столько же click-слушателей, сколько было.
$(function () {
    // streamSource — открытый поток порций. Объект живёт вне стейта:
    // это транспорт, а не данные страницы.
    var streamSource = null;

    var colors = ['красный', 'зелёный', 'синий', 'жёлтый'];
    var cities = ['Москва', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Екатеринбург'];
    var tags = ['гость', 'подписчик', 'автор', 'редактор'];

    // options переводит строки в пункты списка: компоненту нужны значение
    // и подпись, даже когда они совпадают.
    function options(list) {
        var out = [];

        for (var i = 0; i < list.length; i++) {
            out.push({ value: list[i], label: list[i] });
        }
        return out;
    }

    // word берёт текст из словаря по контексту и ключу. Если ключа нет,
    // возвращается запасное значение: код статуса на экране лучше пустого
    // места, а расхождение словарей видно по ответу словаря в поле missing.
    function word(texts, context, key, fallback) {
        var group = texts[context];

        if (group && typeof group[key] === 'string' && group[key] !== '') {
            return group[key];
        }
        return fallback;
    }

    // fill подставляет значения по местам {1}, {2} в самом тексте словаря.
    // Склеивать «начало + число + хвост» нельзя: в другом языке порядок слов
    // другой, и хвост оказался бы не там.
    function fill(template, values) {
        var out = String(template);

        for (var i = 0; i < values.length; i++) {
            out = out.replace('{' + (i + 1) + '}', values[i]);
        }
        return out;
    }

    // filterItems отбирает пункты по строке без учёта регистра.
    function filterItems(items, query) {
        var needle = query.trim().toLowerCase();
        var out = [];

        for (var i = 0; i < items.length; i++) {
            if (items[i].label.toLowerCase().indexOf(needle) >= 0) {
                out.push(items[i]);
            }
        }
        return out;
    }

    // watchOutside закрывает список компонента кликом мимо своего окна:
    // клик по другому списку — старый или компонент — тоже считается мимо,
    // поэтому одновременно открыт только один.
    //
    // Аудит wex 2026-11-26: слушатель вешается один раз за жизнь страницы и
    // не снимается — removeEventListener в этом файле не встречается. Для
    // статичной панели это нормально. Если компонент окажется внутри w-if или
    // w-for и начнёт пересоздаваться, слушатели накопятся вместе с замыканием
    // на view и state: на документе сейчас по одному click на каждый открытый
    // компонент плюс один от страницы. Не переделка, а напоминание: после
    // обновления wex подписку уместно перенести в $mount, а снятие — в
    // $unmount (форма хука одноуровневая, в this — вью экземпляра).
    // WeakSet остаётся страховкой от повторной подписки на каждом toggle.
    var watched = new WeakSet();

    function watchOutside(view, state) {
        if (watched.has(state)) {
            return;
        }
        watched.add(state);
        document.addEventListener('click', function (event) {
            var root = view.find('.select')[0];

            if (!root || !root.contains(event.target)) {
                state.open = false;
            }
        });
    }

    // Стейт собирается отдельным объектом, а не прямо в .state(...): словарь
    // читается до первой отрисовки, а view.state появляется только после
    // mount. Объект на этом этапе обычный; wex делает его реактивным при
    // mount, поэтому запись в него сверх поля не теряется и не требует
    // перерисовки — рисовать ещё нечего.
    var state0 = {
        // texts — словарь переводов по контекстам; lang — фактический язык
        // ответа. Шаблон выводит подписи из texts, поэтому страница
        // монтируется после того, как словарь прочитан.
        texts: {},
        lang: '',
        langError: '',
        checking: true,
        ok: false,
        // Статус связи приходит из словаря вместе с остальными текстами,
        // поэтому литерала здесь нет: его ставит loadLang до отрисовки.
        status: '',
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
        gate: { capacity: 0, active: 0, peak: 0 },
        generateMs: 1500,
        generateResult: '',
        logResult: '',
        streamCount: 5,
        streamMs: 300,
        streamParts: [],
        streamDone: false,
        eventText: 'напоминание',
        eventSeconds: 10,
        selects: {
            // Подписи пустые: пока выбор не сделан, шаблон показывает текст
            // из словаря, поэтому подпись не дублируется в двух местах.
            deleteMode: { open: false, value: 'event', label: '' },
            deleteId: { open: false, value: '', label: '' },
            color: { open: false, value: 'зелёный', label: 'зелёный' },
            city: { open: false, value: '', label: '', query: '' },
            tags: { open: false, value: [] }
        },
        colors: colors,
        colorItems: options(colors),
        componentColor: '',
        cityItems: options(cities),
        componentCity: '',
        tagItems: options(tags),
        componentTags: [],
        cities: cities,
        cityMatches: cities.slice(),
        tags: tags,
        text: 'раз',
        delaySeconds: 0,
        slowMs: 0,
        failure: ''
    };

    var view = $('#app').state(state0);

    // fill — фильтр для текста с подстановкой: {{ texts.gate.stats | fill:a:b }}.
    // Значения приходят аргументами фильтра, а места подстановки стоят
    // в самом переводе, поэтому порядок слов остаётся делом языка.
    view.filter('fill', function (template) {
        return fill(template, Array.prototype.slice.call(arguments, 1));
    });

    // x-select — выпадающий список как компонент: разметка одна на все
    // экземпляры, состояние у каждого своё, выбранное значение уходит
    // родителю событием change. Внешняя функция метода вызывается с вью
    // в this, поэтому emit отправляется через сохранённую ссылку.
    //
    // Аудит wex 2026-11-26: pick читает event.currentTarget.dataset.value.
    // Это работает потому, что w-on сам подставляет найденный элемент в
    // currentTarget — поведение ядра при делегировании, а не правило из книги.
    // Если после обновления wex семантика события изменится (например, в this
    // окажется вью, а элемент придётся брать отдельно), этот pick и два
    // однотипных ниже отдадут undefined молча, без ошибки в консоли.
    // Проверка после обновления: открыть список, выбрать пункт, убедиться, что
    // подпись в списке сменилась и наружу ушло значение.
    // Запись, не зависящая от этой подстановки: event.target.closest('[data-value]').
    // Значения проходят через data-value, то есть через DOM-атрибут, и
    // возвращаются строкой. Пока в items строки (цвета, города, теги) —
    // сходится; с числовыми значениями подсветка value === item.value в
    // шаблоне перестанет совпадать.
    var xSelect = $('#x-select')
        .state(function () {
            return { items: [], value: '', open: false, empty: '' };
        })
        .method('toggle', function (state) {
            watchOutside(this, state);

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

    view.component('x-select', xSelect);

    // x-select-search — список с поиском: пункты фильтруются по вводу,
    // поле получает фокус при открытии.
    //
    // Аудит wex 2026-11-26: matches — производное состояние, оно считается
    // только при открытии и при вводе. Если родитель подменит :items, пока
    // меню открыто, список продолжит показывать старые пункты до закрытия и
    // повторного открытия (проверено). Сейчас items статические, поэтому не
    // проявляется. Когда список станет обновляемым из сети, поле matches
    // придётся либо пересчитывать по $change (следить за items и query), либо
    // убрать, а фильтровать прямо в шаблоне:
    // w-for="item in items | byQuery:query traceby item.value" — фильтры в
    // выражении w-for работают.
    // Имя метода filter совпадает с именем шаблонного view.filter(); здесь это
    // действие для w-on, а не фильтр шаблона — при чтении не путать.
    // Аудит wex 2026-11-26: w-focus объявлена ещё и на вью страницы (ниже,
    // перед $mount). Директивы не наследуются между вью, поэтому копия нужна;
    // но два одинаковых объявления разъедутся при правке. Если появится третий
    // список, регистрировать глобально: Wex.directive('w-focus', ...).
    var xSelectSearch = $('#x-select-search')
        .state(function () {
            return {
                items: [], matches: [], value: '', open: false, query: '',
                empty: '', search: ''
            };
        })
        .directive('w-focus', null, function ($el) {
            $el[0].focus();
        })
        .method('toggle', function (state) {
            watchOutside(this, state);

            return function () {
                state.open = !state.open;
                // Ввод не стирается: при повторном открытии список
                // пересобирается по тому, что уже набрано.
                if (state.open) {
                    state.matches = filterItems(state.items, state.query);
                }
            };
        })
        .method('filter', function (state) {
            return function (event) {
                state.matches = filterItems(state.items, event.currentTarget.value);
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

    view.component('x-select-search', xSelectSearch);

    // x-select-multi — множественный выбор: пункт переключается, список
    // остаётся открытым, наружу уходит массив значений.
    //
    // Аудит wex 2026-11-26: шаблон обращается с value как с массивом
    // (value.join, value.includes). Если в :value придёт не массив, выражения
    // шаблона будут падать на каждом рендере. Переключение пункта — тот же
    // алгоритм, что в pickSelect по ветке name === 'tags' выше: править нужно
    // оба места, иначе разъедутся. Значение пункта, как и в двух списках выше,
    // приходит строкой из data-value.
    var xSelectMulti = $('#x-select-multi')
        .state(function () {
            return { items: [], value: [], open: false, empty: '' };
        })
        .method('toggle', function (state) {
            watchOutside(this, state);

            return function () {
                state.open = !state.open;
            };
        })
        .method('pick', function (state) {
            var component = this;

            return function (event) {
                var chosen = event.currentTarget.dataset.value;
                var next = [];
                var found = false;

                for (var i = 0; i < state.value.length; i++) {
                    if (state.value[i] === chosen) {
                        found = true;
                    } else {
                        next.push(state.value[i]);
                    }
                }
                if (!found) {
                    next.push(chosen);
                }

                state.value = next;
                component.emit('change', next);
            };
        })

    view.component('x-select-multi', xSelectMulti);

    // onComponentColor принимает выбранное от компонента: значение приходит
    // аргументом события, а не через DOM.
    view.method('onComponentColor', function (state) {
        return function (value) {
            state.componentColor = value;
        };
    });

    view.method('onComponentCity', function (state) {
        return function (value) {
            state.componentCity = value;
        };
    });

    view.method('onComponentTags', function (state) {
        return function (value) {
            state.componentTags = value;
        };
    });

    // loadLang читает словарь до первой отрисовки. Язык берётся из адреса
    // (?lang=en-GB), иначе — базовый из каталога. Сервер сам сводит незнакомый
    // язык к знакомому и называет в ответе фактический, поэтому <html lang>
    // ставится по ответу, а не по запросу: страница не должна заявлять язык,
    // на котором текста нет.
    //
    // Заодно читается список доступных языков: переключатель, который
    // появится следующим шагом, не должен предлагать язык без перевода.
    function loadLang(state) {
        var wanted = new URLSearchParams(window.location.search).get('lang') || '';

        return call('/api/lang').then(function (catalog) {
            var code = wanted || catalog.base;

            return call('/api/lang/' + encodeURIComponent(code)).then(function (bundle) {
                state.texts = bundle.texts;
                state.lang = bundle.lang;
                // Стартовый статус — тоже текст из словаря: без него строка
                // связи стояла бы на языке, которого больше нигде нет.
                state.status = word(bundle.texts, 'common', 'checking', 'проверяем связь с сервером…');
                document.documentElement.lang = bundle.lang;
            });
        });
    }

    // call разбирает конверт и отдаёт только result; ошибка становится
    // исключением, чтобы её ловил один общий catch на каждый запрос.
    function call(url, options) {
        return fetch(url, options)
            .then(function (response) {
                return response.json();
            })
            .then(function (body) {
                const { result, error, data } = body;

                if (error) {
                    // Данные ошибки кладутся в объект исключения: по ним форма
                    // показывает, какие поля исправить, а не только текст.
                    const failure = new Error(error);
                    failure.data = data;
                    throw failure;
                }
                return result;
            });
    }

    // describeError собирает текст отказа: если модуль прислал поля, они
    // называются рядом с сообщением.
    function describeError(error) {
        if (!error.data || !error.data.fields || !error.data.fields.length) {
            return error.message;
        }
        return error.message + ': ' + error.data.fields.join(', ');
    }

    // rowState приводит строку очереди к полям стейта. Имена совпадают
    // с ответом; время показывается до секунд, а пустое место
    // в столбце «Начата» — прочерком. Срок постановки пустой, когда работа
    // доступна сразу, поэтому в столбце стоит слово, а не прочерк.
    //
    // Статус на сервере — код (queued, running…), а не фраза: слово берётся
    // из словаря, а неизвестный код показывается как есть, чтобы новая
    // строка состояния не пропала из таблицы.
    function rowState(row, texts, locale) {
        return {
            id: row.id,
            kind: row.kind,
            status: word(texts, 'jobs', 'status_' + row.status, row.status),
            text: row.text,
            attempts: row.attempts,
            notBefore: momentText(row.notBefore, word(texts, 'jobs', 'immediate', 'сразу'), locale),
            createdAt: momentText(row.createdAt, word(texts, 'common', 'dash', '—'), locale),
            startedAt: momentText(row.startedAt, word(texts, 'common', 'dash', '—'), locale)
        };
    }

    // momentText — момент для показа в местном времени. Сервер отдаёт
    // полную точность, панели хватает секунд, а год в таблице повторяется
    // в каждой строке и только отнимает ширину.
    function momentText(value, empty, locale) {
        if (!value) {
            return empty;
        }
        return new Date(value).toLocaleString(locale || 'ru-RU', {
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    // eventState приводит событие к полям стейта: период и срок показываются
    // готовыми строками, а источник называется словом.
    function eventState(row, texts, locale) {
        return {
            id: row.id,
            kind: row.kind,
            period: row.period,
            next: momentText(row.next, word(texts, 'common', 'dash', '—'), locale),
            declared: row.declared,
            source: row.declared ?
                word(texts, 'events', 'source_code', 'код') :
                word(texts, 'events', 'source_data', 'данные')
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
            state.events = rows.map(function (row) {
                return eventState(row, state.texts, state.lang);
            });
            state.deletable = deletableRows(rows);
            state.failure = '';
        });
    }

    // loadJobs перечитывает очередь целиком. Строки идут сверху вниз
    // в том порядке, который отдал сервер: он ставит новые работы первыми.
    function loadJobs(state) {
        return call('/api/demo/jobs')
            .then(function (rows) {
                state.jobs = rows.map(function (row) {
                    return rowState(row, state.texts, state.lang);
                });
                state.failure = '';
            });
    }

    // serviceState приводит состояние службы к полям таблицы: булевы
    // значения показываются словами, пустая причина отказа — прочерком.
    function serviceState(row, texts) {
        var yes = word(texts, 'service', 'yes', 'да');
        var no = word(texts, 'service', 'no', 'нет');

        return {
            name: row.name,
            running: row.running ? yes : no,
            ready: row.ready ? yes : no,
            restarts: row.restarts,
            failure: row.failure || word(texts, 'common', 'dash', '—')
        };
    }

    // loadServices перечитывает состояние служб: по нему видно, что работает,
    // что отказало и сколько было перезапусков.
    function loadServices(state) {
        return call('/api/services')
            .then(function (rows) {
                state.services = rows.map(function (row) {
                    return serviceState(row, state.texts);
                });
                state.failure = '';
            });
    }

    // snapshot читает связь, снимок демо, очередь и список служб одним
    // заходом: запросы идут параллельно, шаблон перерисовывается один раз.
    function snapshot(state) {
        state.checking = true;
        state.status = word(state.texts, 'common', 'checking', 'проверяем связь с сервером…');

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
                state.gate = demo.gate;
                state.jobs = rows.map(function (row) {
                    return rowState(row, state.texts, state.lang);
                });
                state.services = services.map(function (row) {
                    return serviceState(row, state.texts);
                });
                state.events = events.map(function (row) {
                    return eventState(row, state.texts, state.lang);
                });
                state.deletable = deletableRows(events);
                state.failure = '';
            })
            .catch(function (error) {
                state.checking = false;
                state.ok = false;
                state.status = fill(word(state.texts, 'common', 'check_failed', 'сервер не ответил: {1}'), [error.message]);
                state.failure = error.message;
            });
    }

    // check повторяет тот запрос, которым страница проверяет связь. Кнопка
    // нужна не для данных, а чтобы запрос было видно в инструментах
    // веб-разработчика: нажатие оставляет одну строку в сетевой панели.
    view.method('check', function (state) {
        return function () {
            state.checking = true;
            state.status = word(state.texts, 'common', 'checking', 'проверяем связь с сервером…');

            call('/api/health')
                .then(function (health) {
                    state.checking = false;
                    state.ok = health.ok;
                    state.status = health.status;
                })
                .catch(function (error) {
                    state.checking = false;
                    state.ok = false;
                    state.status = fill(word(state.texts, 'common', 'check_failed', 'сервер не ответил: {1}'), [error.message]);
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
                    state.failure = describeError(error);
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
                    state.selects.deleteId.label = '';
                    return loadEvents(state);
                })
                .catch(function (error) {
                    state.failure = describeError(error);
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
    // появляется сверху вместе со своим статусом и временами. Числа из
    // полей ввода приходят строками, поэтому приводятся здесь, а не на сервере.
    view.method('enqueue', function (state) {
        return function () {
            call('/api/demo/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    text: state.text,
                    delaySeconds: Number(state.delaySeconds) || 0,
                    slowMs: Number(state.slowMs) || 0
                })
            })
                .then(function () {
                    return loadJobs(state);
                })
                .catch(function (error) {
                    state.failure = describeError(error);
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

    // generateNow — синхронный контур хода: ответ приходит, когда слот
    // свободен и работа закончена. Время ожидания слота и есть признак
    // столкновения — второй ход ждал первого.
    view.method('generateNow', function (state) {
        return function () {
            call('/api/demo/generate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ms: Number(state.generateMs) || 0 })
            })
                .then(function (res) {
                    state.gate = res.gate;
                    state.generateResult = fill(
                        word(state.texts, 'gate', 'result_sync', 'ждал слот {1} мс, работал {2} мс'),
                        [res.waitedMs, res.tookMs]
                    );
                })
                .catch(function (error) {
                    state.failure = describeError(error);
                });
        };
    });

    // generateQueued — тот же ход, но работой: ответ приходит сразу
    // с номером, а выполняет его разборщик, когда слот свободен.
    view.method('generateQueued', function (state) {
        return function () {
            call('/api/demo/generate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ms: Number(state.generateMs) || 0, queued: true })
            })
                .then(function (res) {
                    state.gate = res.gate;
                    state.generateResult = fill(
                        word(state.texts, 'gate', 'result_queued', 'ход {1} поставлен в очередь'),
                        [res.id]
                    );
                    return loadJobs(state);
                })
                .catch(function (error) {
                    state.failure = describeError(error);
                });
        };
    });

    // writeLog просит модуль записать фразу из словаря в журнал работы.
    // Текст приходит от сервера: он собран там из того же файла, что и подписи
    // страницы, поэтому совпадение записи в журнале с экраном видно сразу.
    view.method('writeLog', function (state) {
        return function () {
            call('/api/log_ru/write', { method: 'POST' })
                .then(function (res) {
                    state.logResult = fill(
                        word(state.texts, 'log_ru', 'result', 'записали: {1} — язык {2}'),
                        [res.text, res.lang]
                    );
                })
                .catch(function (error) {
                    state.failure = describeError(error);
                });
        };
    });

    // crashWatch просит службу упасть на следующем тике. Список служб
    // обновляется с задержкой: группа ждёт срок перед перезапуском, и сразу
    // после нажатия состояние ещё старое.
    view.method('crashWatch', function (state) {
        return function () {
            call('/api/demo/watch/crash', { method: 'POST' })
                .then(function () {
                    setTimeout(function () {
                        loadServices(state);
                    }, 1800);
                })
                .catch(function (error) {
                    state.failure = describeError(error);
                });
        };
    });

    // startStream открывает поток порций и складывает их в стейт по мере
    // прихода. Соединение живёт вне стейта: это транспорт, а не данные.
    view.method('startStream', function (state) {
        return function () {
            if (streamSource) {
                streamSource.close();
                streamSource = null;
            }

            state.streamParts = [];
            state.streamDone = false;

            const parts = Number(state.streamCount) || 5;
            const ms = Number(state.streamMs) || 0;
            const source = new EventSource('/api/demo/stream?parts=' + parts + '&ms=' + ms);
            streamSource = source;

            source.onmessage = function (event) {
                const item = JSON.parse(event.data);

                if (item.done) {
                    source.close();
                    streamSource = null;
                    state.streamDone = true;
                    return;
                }
                state.streamParts = state.streamParts.concat([{
                    id: item.part,
                    text: fill(word(state.texts, 'httpapi', 'stream_part', 'порция {1}'), [item.part])
                }]);
            };

            source.onerror = function () {
                // Ошибка приходит и при собственном закрытии, поэтому
                // проверяется, что поток всё ещё тот же.
                if (streamSource !== source) {
                    return;
                }
                source.close();
                streamSource = null;
                state.failure = word(state.texts, 'httpapi', 'stream_failed', 'поток прерван');
            };
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
    // Аудит wex 2026-11-26: второе объявление той же директивы — на вью
    // компонента x-select-search выше. Директивы между вью не наследуются,
    // поэтому копия нужна, но две реализации одного поведения при правке
    // разъедутся; при появлении третьего списка вынести в Wex.directive().
    view.directive('w-focus', null, function ($el) {
        $el[0].focus();
    });

    // Первый снимок читается сразу после появления страницы: $mount —
    // документированное место для начальной загрузки данных.
    //
    // Аудит wex 2026-11-26: здесь форма хука правильная и требует проверки
    // после обновления: обработчик — одна функция, state первым аргументом,
    // this — вью. Внешняя функция, возвращающая function () {}, у хуков не
    // вызывается (см. список в начале файла, пункт 2): у компонентов из-за
    // этого watchOutside живёт на toggle, а не на $mount.
    view.on('$mount', function (state) {
        snapshot(state);
    });

    // Клик вне списка закрывает открытые. Слушатель на документе, потому что
    // списки разбросаны по странице; клик внутри списка пропускается,
    // иначе он закрыл бы тот список, в котором выбирают.
    // Аудит wex 2026-11-26: этот слушатель не снимается: страница живёт до
    // перезагрузки, поэтому вреда нет. Вместе с тремя компонентными
    // (watchOutside) на документе висит по одному click на каждый открытый
    // компонент — после обновления wex это первое, что стоит пересчитать.
    document.addEventListener('click', function (event) {
        var inSelect = event.target.closest('.select');
        var inComponent = event.target.closest('x-select, x-select-search, x-select-multi');

        if (inSelect && !inComponent) {
            return;
        }
        view.update(function (state) {
            for (var key in state.selects) {
                state.selects[key].open = false;
            }
        });
    });

    // Словарь читается до mount: шаблон выводит заголовки из стейта, и
    // показать пустые заголовки, чтобы через миг заменить их текстом, хуже
    // короткой паузы. Отказ словаря страницу не снимает — подписи будут
    // пустыми, а причина видна отдельной строкой; общий failure для этого
    // не годится, его стирает успешный снимок данных.
    loadLang(state0)
        .catch(function (error) {
            state0.langError = fill(
                word(state0.texts, 'common', 'lang_failed', 'словарь не загружен: {1}'),
                [error.message]
            );
        })
        .then(function () {
            view.mount();
        });
});
