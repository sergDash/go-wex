// Поток данных страницы.
//
// 1. Стейт создаётся синхронно с исходным состоянием «проверяем».
// 2. mount() вставляет шаблон #app в DOM и подписывает его на стейт.
// 3. Ответ /api/health приходит в конверте {"result": {...}}; поля результата
//    заменяют поля стейта, и wex перерисовывает шаблон сам.
//
// Ответ разбирается деструктуризацией: сначала error, затем result —
// тот же порядок, что в Go при `result, err := ...`.
$(function () {
    var view = $('#app').state({
        checking: true,
        ok: false,
        status: 'проверяем связь с сервером…'
    });

    view.mount();

    fetch('/api/health')
        .then(function (response) {
            return response.json();
        })
        .then(function (body) {
            const { result, error } = body;

            if (error) {
                throw new Error(error);
            }

            view.update({
                checking: false,
                ok: result.ok,
                status: result.status
            });
        })
        .catch(function (error) {
            view.update({
                checking: false,
                ok: false,
                status: 'сервер не ответил: ' + error.message
            });
        });
});
