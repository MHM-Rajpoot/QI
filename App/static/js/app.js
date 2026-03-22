(function () {
    function ensureContainer() {
        let container = document.getElementById('appNotifications');
        if (!container) {
            container = document.createElement('div');
            container.id = 'appNotifications';
            container.className = 'app-toast-stack';
            container.setAttribute('aria-live', 'polite');
            container.setAttribute('aria-atomic', 'false');
            document.body.appendChild(container);
        }
        return container;
    }

    function dismissToast(toast) {
        if (!toast) {
            return;
        }
        toast.classList.add('is-leaving');
        window.setTimeout(() => toast.remove(), 180);
    }

    function notify(message, options = {}) {
        const container = ensureContainer();
        const tone = options.tone || options.type || 'info';
        const title = options.title || '';
        const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : 5000;

        const toast = document.createElement('section');
        toast.className = `app-toast app-toast--${tone}`;
        toast.setAttribute('role', tone === 'error' ? 'alert' : 'status');

        const body = document.createElement('div');
        body.className = 'app-toast__body';

        if (title) {
            const heading = document.createElement('div');
            heading.className = 'app-toast__title';
            heading.textContent = title;
            body.appendChild(heading);
        }

        const text = document.createElement('div');
        text.className = 'app-toast__message';
        text.textContent = message;
        body.appendChild(text);

        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'app-toast__close';
        button.setAttribute('aria-label', 'Dismiss notification');
        button.innerHTML = '&times;';
        button.addEventListener('click', () => dismissToast(toast));

        toast.appendChild(body);
        toast.appendChild(button);
        container.appendChild(toast);

        if (timeoutMs > 0) {
            window.setTimeout(() => dismissToast(toast), timeoutMs);
        }

        return toast;
    }

    window.appUI = {
        notify,
    };
})();
