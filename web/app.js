// Поток данных страницы.
//
// 1. Стейт создаётся синхронно с исходным состоянием «проверяем».
// 2. mount() вставляет шаблон #app в DOM и подписывает его на стейт.
// 3. Ответ /api/health заменяет поля стейта; wex перерисовывает шаблон сам.
//
// Никакой прямой работы с DOM здесь нет: всё, что видит пользователь,
// описано в index.html и выводится из полей стейта.
$(function () {
    var view = $('#app').state({
        checking: true,
        ok: false,
        status: 'проверяем связь с сервером…'
    });

    view.mount();

    fetch('/api/health')
        .then(function (response) {
            if (!response.ok) {
                throw new Error('HTTP ' + response.status);
            }
            return response.json();
        })
        .then(function (data) {
            view.update({
                checking: false,
                ok: data.ok,
                status: data.status
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
