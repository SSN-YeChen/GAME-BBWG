const ACCOUNT_STATUS = {
  pending: 0,
  redeemed: 1,
  failed: 2
};

const app = document.querySelector('#app');
let currentRoute = 'home';
let authChecked = false;
let isAuthenticated = false;
let authUsername = '';
let authRole = '';
let authError = '';
let accountIdFilter = '';
let gameNameFilter = '';
let importIsRunning = false;
let importProcessed = 0;
let importTotal = 0;
let importInserted = 0;
let importSkipped = 0;
let importFailed = 0;
let importCurrentAccountId = '';
let redeemCode = '';
let redeemToken = '';
let redeemIsRunning = false;
let redeemProcessed = 0;
let redeemTotal = 0;
let redeemSummary = null;
let redeemLogs = [];
let redeemProgressSubscribed = false;
let redeemConfigLoaded = false;
let redeemAccounts = [];
let redeemStatuses = {};
let listAccountsCache = [];
let draggedAccountId = '';
let touchDraggedAccountId = '';
let touchDragChanged = false;
let touchDragTimer = null;
let touchDragReady = false;

let eventSource = null;
let importEventSource = null;
let namePopupDismissBound = false;

function getRedeemStatusView(account) {
  return redeemStatuses[account.accountId] ?? getDefaultRedeemStatus(account.status);
}

function isAdminUser() {
  return authRole === 'admin';
}

function ensureEventSource() {
  if (!isAuthenticated) {
    return;
  }

  if (eventSource) {
    return;
  }

  eventSource = new EventSource('/api/redeem/events');
  eventSource.onmessage = (event) => {
    if (!redeemProgressSubscribed) {
      return;
    }

    try {
      const payload = JSON.parse(event.data);

      if (payload.type === 'start') {
        redeemTotal = payload.total ?? 0;
        redeemProcessed = payload.processed ?? 0;
        redeemLogs = [];
        redeemSummary = null;
      }

      if (payload.type === 'log' && payload.message) {
        redeemLogs = [
          ...redeemLogs,
          {
            level: payload.level ?? 'info',
            message: payload.message
          }
        ];
        syncRedeemStatusFromLog(payload.message);
      }

      if (payload.type === 'progress') {
        redeemProcessed = payload.processed ?? redeemProcessed;
        redeemTotal = payload.total ?? redeemTotal;
      }

      if (payload.type === 'done' && payload.summary) {
        redeemSummary = payload.summary;
      }

      refreshRedeemUi();
    } catch {
      // ignore
    }
  };
  eventSource.onerror = () => {
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    setTimeout(ensureEventSource, 1500);
  };
}

function ensureImportEventSource() {
  if (!isAuthenticated) {
    return;
  }

  if (importEventSource) {
    return;
  }

  importEventSource = new EventSource('/api/accounts/import-events');
  importEventSource.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);

      if (payload.type === 'start') {
        importIsRunning = true;
        importTotal = payload.total ?? 0;
        importProcessed = payload.processed ?? 0;
        importInserted = payload.inserted ?? 0;
        importSkipped = payload.skipped ?? 0;
        importFailed = payload.failed ?? 0;
        importCurrentAccountId = '';
      }

      if (payload.type === 'progress') {
        importIsRunning = true;
        importTotal = payload.total ?? importTotal;
        importProcessed = payload.processed ?? importProcessed;
        importInserted = payload.inserted ?? importInserted;
        importSkipped = payload.skipped ?? importSkipped;
        importFailed = payload.failed ?? importFailed;
        importCurrentAccountId = payload.accountId ?? '';
      }

      if (payload.type === 'done') {
        importIsRunning = false;
        importTotal = payload.total ?? importTotal;
        importProcessed = payload.processed ?? importProcessed;
        importInserted = payload.inserted ?? importInserted;
        importSkipped = payload.skipped ?? importSkipped;
        importFailed = payload.failed ?? importFailed;
        importCurrentAccountId = '';
      }

      refreshCreateUi();
    } catch {
      // ignore
    }
  };
  importEventSource.onerror = () => {
    if (importEventSource) {
      importEventSource.close();
      importEventSource = null;
    }
    setTimeout(ensureImportEventSource, 1500);
  };
}

function api(path, options) {
  return fetch(path, {
    headers: {
      'Content-Type': 'application/json'
    },
    ...options
  }).then(async (res) => {
    const data = await res.json();
    if (!res.ok) {
      if (res.status === 401) {
        isAuthenticated = false;
        authChecked = true;
        authUsername = '';
        authRole = '';
        closeEventSource();
        closeImportEventSource();
        void render();
      }
      throw new Error(data?.error || '请求失败');
    }
    return data;
  });
}

function closeEventSource() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }

  redeemProgressSubscribed = false;
}

function closeImportEventSource() {
  if (importEventSource) {
    importEventSource.close();
    importEventSource = null;
  }
}

function getRouteFromHash() {
  const hash = window.location.hash.replace('#', '');
  if (hash === 'create' || hash === 'list' || hash === 'redeem') {
    return hash;
  }
  return 'home';
}

function navigate(route) {
  currentRoute = route;
  window.location.hash = route === 'home' ? '' : route;
  void render();
}

function createShell(content, pageClass = '') {
  const showHomeActions = currentRoute === 'home';
  return `
    <main class="shell ${pageClass}">
      <section class="frame">
        <header class="topbar">
          <button class="back-link" data-route="home" ${currentRoute === 'home' ? 'hidden' : ''}>返回首页</button>
          ${
            showHomeActions
              ? `
          <div class="topbar-actions">
            <span class="user-chip">${escapeHtml(authUsername)}</span>
            <button class="secondary-button" id="logout-button">退出登录</button>
          </div>
          `
              : ''
          }
        </header>
        ${content}
      </section>
      <div class="avatar-lightbox" id="avatar-lightbox" hidden>
        <img class="avatar-lightbox-image" id="avatar-lightbox-image" alt="头像预览" />
      </div>
    </main>
  `;
}

function renderLoginPage() {
  return `
    <main class="shell auth-page">
      <section class="frame">
        <section class="hero">
          <div class="hero-panel auth-panel">
            <div>
              <p class="lead">请输入后端配置的账号和密码。</p>
            </div>
            <input id="login-username" class="search-input auth-input" type="text" placeholder="账号" />
            <input id="login-password" class="search-input auth-input" type="password" placeholder="密码" />
            <button class="primary-button" id="login-button">登录</button>
            <div id="login-feedback" class="feedback" data-state="error" ${authError ? '' : 'hidden'}>${escapeHtml(authError)}</div>
          </div>
        </section>
      </section>
    </main>
  `;
}

function renderHome() {
  return createShell(
    `
    <section class="hero">
      <div class="hero-panel home-hero-panel">
        <div class="home-card-grid">
          <button class="nav-card home-nav-card" data-route="create">
            <span class="home-card-glow"></span>
            <span class="home-card-label">新增账号</span>
            <span class="home-card-meta">批量导入并自动拉取资料</span>
          </button>
          <button class="nav-card home-nav-card" data-route="list">
            <span class="home-card-glow"></span>
            <span class="home-card-label">账号列表</span>
            <span class="home-card-meta">检索现有账号与角色信息</span>
          </button>
          <button class="nav-card home-nav-card" data-route="redeem">
            <span class="home-card-glow"></span>
            <span class="home-card-label">批量兑换</span>
            <span class="home-card-meta">实时查看兑换状态与结果</span>
          </button>
        </div>
      </div>
    </section>
  `,
    'home-page'
  );
}

function renderCreatePage() {
  const progressPercent = importTotal > 0 ? Math.round((importProcessed / importTotal) * 100) : 0;
  const progressSection =
    importIsRunning || importTotal > 0
      ? `
      <div class="create-progress">
        <div class="redeem-progress-bar"><span style="width: ${progressPercent}%"></span></div>
        <div class="redeem-progress-text">录入进度 ${importProcessed} / ${importTotal}，成功 ${importInserted}，跳过 ${importSkipped}，失败 ${importFailed}</div>
        ${importCurrentAccountId ? `<div class="redeem-progress-text">当前处理：${escapeHtml(importCurrentAccountId)}</div>` : ''}
      </div>
    `
      : '';
  return createShell(`
    <section class="page-head">
      <div>
        <p class="lead">每行一个，已存在的不会重复写入。</p>
      </div>
    </section>
    <section class="panel form-panel">
      <label class="field-label" for="account-ids">账号列表</label>
      <textarea id="account-ids" class="textarea" placeholder="一行一个ID" ${importIsRunning ? 'disabled' : ''}></textarea>
      ${progressSection}
      <div class="actions">
        <button class="primary-button" id="submit-accounts" ${importIsRunning ? 'disabled' : ''}>${importIsRunning ? '录入中...' : '提交'}</button>
      </div>
      <div id="create-feedback" class="feedback" hidden></div>
    </section>
  `);
}

function accountRowTemplate(account) {
  const gameAvatar = account.details?.avatar_image?.trim() || '';
  const gameZone = account.details?.kid?.toString().trim() || '-';
  const gameLevel = account.details?.stove_lv?.toString().trim() || '-';
  const gameName = account.name?.trim() || '-';
  const avatarContent = gameAvatar
    ? `<img class="avatar-image" src="${escapeAttribute(gameAvatar)}" alt="${escapeAttribute(gameName || account.accountId)}" loading="lazy" />`
    : '<span class="avatar-fallback">无头像</span>';

  return `
    <tr
      ${isAdminUser() ? `data-sort-account-id="${escapeAttribute(account.accountId)}"` : ''}
    >
      <td data-label="游戏头像">${avatarContent}</td>
      <td data-label="账号ID">
        ${
          isAdminUser() && accountIdFilter.trim() === '' && gameNameFilter.trim() === ''
            ? `<button class="sort-trigger-button" type="button" data-sort-trigger="true">${escapeHtml(account.accountId)}</button>`
            : escapeHtml(account.accountId)
        }
      </td>
      <td data-label="游戏名">
        <div class="name-cell">
          <button class="name-preview-button" type="button" data-full-game-name="${escapeAttribute(gameName)}">${escapeHtml(gameName)}</button>
        </div>
      </td>
      <td data-label="游戏区">${escapeHtml(gameZone)}</td>
      <td data-label="游戏等级">${escapeHtml(gameLevel)}</td>
      ${
        isAdminUser()
          ? `
      <td class="table-actions" data-label="操作">
        <button class="danger-button" data-delete-account="${escapeAttribute(account.accountId)}">删除</button>
      </td>
      `
          : ''
      }
    </tr>
  `;
}

async function renderListPage() {
  const accounts = await api('/api/accounts');
  listAccountsCache = accounts;
  const filteredAccounts = accounts.filter((account) => {
    const accountIdMatches =
      accountIdFilter.trim() === '' || account.accountId.toLowerCase().includes(accountIdFilter.trim().toLowerCase());
    const gameNameMatches =
      gameNameFilter.trim() === '' || account.name.trim().toLowerCase().includes(gameNameFilter.trim().toLowerCase());
    return accountIdMatches && gameNameMatches;
  });

  return createShell(`
    <section class="page-head">
      <div class="list-toolbar">
        <input id="search-account-id" class="search-input" type="text" placeholder="搜索账号ID" value="${escapeAttribute(accountIdFilter)}" />
        <input id="search-game-name" class="search-input" type="text" placeholder="搜索游戏名" value="${escapeAttribute(gameNameFilter)}" />
        <button class="secondary-button toolbar-button" id="apply-search">搜索</button>
        <button class="secondary-button toolbar-button" id="clear-search">清空搜索条件</button>
      </div>
      <div class="page-actions">
        <button class="secondary-button" id="refresh-accounts">刷新</button>
        ${isAdminUser() ? '<button class="danger-button" id="delete-all-accounts">一键删除</button>' : ''}
      </div>
    </section>
    <section class="panel table-panel">
      ${
        isAdminUser() && (accountIdFilter.trim() !== '' || gameNameFilter.trim() !== '')
          ? '<div class="feedback" data-state="success">排序仅在未使用搜索筛选时可拖动调整。</div>'
          : ''
      }
      <div class="name-popup-layer" id="name-popup" hidden></div>
      ${
        accounts.length === 0
          ? '<div class="empty-state">当前还没有账号数据。</div>'
          : filteredAccounts.length === 0
            ? '<div class="empty-state">没有匹配到符合条件的账号。</div>'
            : `
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>游戏头像</th>
                    <th>账号ID</th>
                    <th>游戏名</th>
                    <th>游戏区</th>
                    <th>游戏等级</th>
                    ${isAdminUser() ? '<th>操作</th>' : ''}
                  </tr>
                </thead>
                <tbody>${filteredAccounts.map(accountRowTemplate).join('')}</tbody>
              </table>
            </div>
          `
      }
    </section>
  `);
}

function renderRedeemPage() {
  const progressPercent = redeemTotal > 0 ? Math.round((redeemProcessed / redeemTotal) * 100) : 0;
  const retryableAccountIds = getRetryableAccountIds();
  const redeemToolbar = isAdminUser()
    ? `
      <div class="redeem-toolbar">
        <input id="redeem-token" class="search-input redeem-input" type="text" placeholder="输入兑换 TOKEN" value="${escapeAttribute(redeemToken)}" ${redeemIsRunning ? 'disabled' : ''} />
        <button class="secondary-button toolbar-button" id="save-redeem-token" ${redeemIsRunning ? 'disabled' : ''}>保存TOKEN</button>
      </div>
      <div class="redeem-toolbar">
        <input id="redeem-code" class="search-input redeem-input" type="text" placeholder="输入兑换码" value="${escapeAttribute(redeemCode)}" ${redeemIsRunning ? 'disabled' : ''} />
        <button class="primary-button toolbar-button" id="start-redeem" ${redeemIsRunning ? 'disabled' : ''}>${redeemIsRunning ? '处理中...' : '开始兑换'}</button>
        <button class="danger-button toolbar-button" id="stop-redeem" ${redeemIsRunning ? '' : 'disabled'}>停止兑换</button>
        <button class="secondary-button toolbar-button" id="retry-failed-redeem" ${redeemIsRunning || retryableAccountIds.length === 0 ? 'disabled' : ''}>重新兑换失败用户</button>
        <button class="secondary-button toolbar-button" id="force-complete-redeem" ${redeemIsRunning ? 'disabled' : ''}>强制全部设为已兑换</button>
      </div>
    `
    : '<div class="feedback" data-state="success">当前为临时账号，只可查看兑换状态。</div>';
  const rows =
    redeemAccounts.length === 0
      ? '<div class="empty-state">当前没有可兑换账号。</div>'
      : `
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>头像</th>
              <th>名字</th>
              <th>兑换状态</th>
            </tr>
          </thead>
          <tbody>${redeemAccounts.map((account) => redeemAccountRowTemplate(account)).join('')}</tbody>
        </table>
      </div>
    `;

  return createShell(`
    <section class="panel redeem-panel">
      ${redeemToolbar}
      <div class="redeem-progress">
        <div class="redeem-progress-bar"><span style="width: ${progressPercent}%"></span></div>
        <div class="redeem-progress-text">进度 ${redeemProcessed} / ${redeemTotal}</div>
      </div>
      ${rows}
    </section>
  `);
}

function redeemAccountRowTemplate(account) {
  const gameAvatar = account.details?.avatar_image?.trim() || '';
  const gameName = account.name?.trim() || account.accountId;
  const avatarContent = gameAvatar
    ? `<img class="avatar-image" src="${escapeAttribute(gameAvatar)}" alt="${escapeAttribute(gameName)}" loading="lazy" />`
    : '<span class="avatar-fallback">无头像</span>';
  const statusView = getRedeemStatusView(account);

  return `
    <tr data-redeem-account-id="${escapeAttribute(account.accountId)}">
      <td data-label="头像">${avatarContent}</td>
      <td data-label="名字">${escapeHtml(gameName)}</td>
      <td data-label="兑换状态">
        <span class="status-badge status-${statusView.code}" data-redeem-status="${escapeAttribute(account.accountId)}">${escapeHtml(statusView.text)}</span>
      </td>
    </tr>
  `;
}

async function render() {
  if (!app) {
    return;
  }

  if (!authChecked) {
    app.innerHTML = '';
    return;
  }

  if (!isAuthenticated) {
    app.innerHTML = renderLoginPage();
    bindEvents();
    return;
  }

  if (!redeemConfigLoaded) {
    const config = await api('/api/config/redeem');
    redeemToken = config.redeemToken;
    redeemConfigLoaded = true;
  }

  if (currentRoute === 'redeem') {
    redeemAccounts = await api('/api/accounts');
  }

  if (currentRoute === 'home') {
    app.innerHTML = renderHome();
  } else if (currentRoute === 'create') {
    app.innerHTML = renderCreatePage();
  } else if (currentRoute === 'list') {
    app.innerHTML = await renderListPage();
  } else {
    app.innerHTML = renderRedeemPage();
  }

  bindEvents();
}

function bindEvents() {
  if (!authChecked) {
    return;
  }

  if (!isAuthenticated) {
    const loginButton = document.querySelector('#login-button');
    loginButton?.addEventListener('click', async () => {
      const usernameInput = document.querySelector('#login-username');
      const passwordInput = document.querySelector('#login-password');
      const feedback = document.querySelector('#login-feedback');
      const username = usernameInput?.value.trim() ?? '';
      const password = passwordInput?.value ?? '';

      authError = '';
      if (feedback) {
        feedback.hidden = true;
      }

      loginButton.disabled = true;
      try {
        const result = await api('/api/auth/login', {
          method: 'POST',
          body: JSON.stringify({ username, password })
        });
        isAuthenticated = true;
        authUsername = result.username || username;
        authRole = result.role || '';
        redeemConfigLoaded = false;
        authError = '';
        ensureEventSource();
        void render();
      } catch (error) {
        authError = error instanceof Error ? error.message : '登录失败';
        if (feedback) {
          feedback.hidden = false;
          feedback.textContent = authError;
        }
      } finally {
        loginButton.disabled = false;
      }
    });

    document.querySelector('#login-password')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        loginButton?.click();
      }
    });

    document.querySelector('#login-username')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        loginButton?.click();
      }
    });

    return;
  }

  ensureEventSource();
  ensureImportEventSource();
  ensureRedeemProgressSubscription();

  document.querySelectorAll('[data-route]').forEach((element) => {
    element.addEventListener('click', () => {
      const route = element.dataset.route;
      if (route) {
        navigate(route);
      }
    });
  });

  const logoutButton = document.querySelector('#logout-button');
  logoutButton?.addEventListener('click', async () => {
    try {
      await api('/api/auth/logout', { method: 'POST' });
    } catch {
      // ignore logout request failure and clear local state anyway
    } finally {
      isAuthenticated = false;
      authUsername = '';
      authRole = '';
      authChecked = true;
      redeemConfigLoaded = false;
      redeemAccounts = [];
      redeemStatuses = {};
      closeEventSource();
      closeImportEventSource();
      void render();
    }
  });

  const submitButton = document.querySelector('#submit-accounts');
  submitButton?.addEventListener('click', async () => {
    const textarea = document.querySelector('#account-ids');
    const feedback = document.querySelector('#create-feedback');
    if (!textarea || !feedback) {
      return;
    }

    const accountIds = textarea.value.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
    if (accountIds.length === 0) {
      showFeedback(feedback, '请至少输入一个 account_id。', true);
      return;
    }

    submitButton.disabled = true;
    importIsRunning = true;
    importProcessed = 0;
    importTotal = accountIds.length;
    importInserted = 0;
    importSkipped = 0;
    importFailed = 0;
    importCurrentAccountId = '';
    refreshCreateUi();
    try {
      const result = await api('/api/accounts/batch', {
        method: 'POST',
        body: JSON.stringify({ accountIds })
      });
      const failedText = result.failed > 0 ? `，请求失败 ${result.failed} 个` : '';
      showFeedback(feedback, `已写入 ${result.inserted} 个，已存在跳过 ${result.skipped} 个${failedText}。`, false);
      textarea.value = '';
    } catch (error) {
      importIsRunning = false;
      showFeedback(feedback, error instanceof Error ? error.message : '写入失败，请稍后重试。', true);
    } finally {
      importIsRunning = false;
      refreshCreateUi();
      submitButton.disabled = false;
    }
  });

  const refreshButton = document.querySelector('#refresh-accounts');
  refreshButton?.addEventListener('click', () => void render());

  const deleteAllButton = document.querySelector('#delete-all-accounts');
  deleteAllButton?.addEventListener('click', async () => {
    if (!window.confirm('确定要删除全部账号吗？此操作不可恢复。')) {
      return;
    }
    deleteAllButton.disabled = true;
    try {
      await api('/api/accounts', { method: 'DELETE' });
      void render();
    } finally {
      deleteAllButton.disabled = false;
    }
  });

  const applySearchButton = document.querySelector('#apply-search');
  applySearchButton?.addEventListener('click', () => {
    const accountIdInput = document.querySelector('#search-account-id');
    const gameNameInput = document.querySelector('#search-game-name');
    accountIdFilter = accountIdInput?.value ?? '';
    gameNameFilter = gameNameInput?.value ?? '';
    void render();
  });

  const clearSearchButton = document.querySelector('#clear-search');
  clearSearchButton?.addEventListener('click', () => {
    accountIdFilter = '';
    gameNameFilter = '';
    void render();
  });

  const namePopup = document.querySelector('#name-popup');
  const tablePanel = document.querySelector('.table-panel');
  document.querySelectorAll('[data-full-game-name]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.stopPropagation();
      if (!namePopup || !tablePanel) {
        return;
      }

      const fullName = button.dataset.fullGameName ?? '';
      const wasHidden = namePopup.hidden;
      const isSameName = namePopup.textContent === fullName;
      const buttonRect = button.getBoundingClientRect();
      const panelRect = tablePanel.getBoundingClientRect();

      namePopup.textContent = fullName;
      namePopup.style.left = `${buttonRect.left - panelRect.left + buttonRect.width / 2}px`;
      namePopup.style.top = `${buttonRect.top - panelRect.top - 10}px`;

      namePopup.hidden = isSameName ? !wasHidden : false;
    });
  });

  if (!namePopupDismissBound) {
    document.addEventListener('click', () => {
      const currentPopup = document.querySelector('#name-popup');
      if (currentPopup) {
        currentPopup.hidden = true;
      }
    });
    namePopupDismissBound = true;
  }

  const reorderEnabled = isAdminUser() && currentRoute === 'list' && accountIdFilter.trim() === '' && gameNameFilter.trim() === '';
  if (reorderEnabled) {
    document.querySelectorAll('[data-sort-trigger]').forEach((trigger) => {
      trigger.addEventListener('mousedown', () => {
        const row = trigger.closest('[data-sort-account-id]');
        if (!(row instanceof HTMLElement)) {
          return;
        }
        row.dataset.dragArmed = 'true';
        row.draggable = true;
      });

      trigger.addEventListener(
        'touchstart',
        () => {
          const row = trigger.closest('[data-sort-account-id]');
          if (!(row instanceof HTMLElement)) {
            return;
          }

          if (touchDragTimer) {
            clearTimeout(touchDragTimer);
          }

          touchDraggedAccountId = row.dataset.sortAccountId ?? '';
          touchDragChanged = false;
          touchDragReady = false;
          row.classList.add('touch-drag-armed');
          touchDragTimer = setTimeout(() => {
            touchDragReady = true;
            row.classList.remove('touch-drag-armed');
            row.classList.add('is-dragging');
          }, 1000);
        },
        { passive: true }
      );
    });

    document.querySelectorAll('[data-sort-account-id]').forEach((row) => {
      row.addEventListener('dragstart', (event) => {
        if (row.dataset.dragArmed !== 'true') {
          event.preventDefault();
          row.draggable = false;
          return;
        }
        draggedAccountId = row.dataset.sortAccountId ?? '';
        row.classList.add('is-dragging');
      });

      row.addEventListener('dragend', () => {
        draggedAccountId = '';
        row.dataset.dragArmed = '';
        row.draggable = false;
        row.classList.remove('is-dragging');
        document.querySelectorAll('[data-sort-account-id]').forEach((item) => {
          item.classList.remove('drag-over');
        });
      });

      row.addEventListener('dragover', (event) => {
        event.preventDefault();
        if (!draggedAccountId || draggedAccountId === row.dataset.sortAccountId) {
          return;
        }
        row.classList.add('drag-over');
      });

      row.addEventListener('dragleave', () => {
        row.classList.remove('drag-over');
      });

      row.addEventListener('drop', async (event) => {
        event.preventDefault();
        row.classList.remove('drag-over');

        const targetAccountId = row.dataset.sortAccountId ?? '';
        if (!draggedAccountId || !targetAccountId || draggedAccountId === targetAccountId) {
          return;
        }

        const nextOrder = [...listAccountsCache];
        const draggedIndex = nextOrder.findIndex((item) => item.accountId === draggedAccountId);
        const targetIndex = nextOrder.findIndex((item) => item.accountId === targetAccountId);
        if (draggedIndex === -1 || targetIndex === -1) {
          return;
        }

        const [draggedAccount] = nextOrder.splice(draggedIndex, 1);
        nextOrder.splice(targetIndex, 0, draggedAccount);
        listAccountsCache = nextOrder;

        try {
          await api('/api/accounts/reorder', {
            method: 'POST',
            body: JSON.stringify({ accountIds: nextOrder.map((item) => item.accountId) })
          });
          void render();
        } catch (error) {
          window.alert(error instanceof Error ? error.message : '排序保存失败');
          void render();
        }
      });

      row.addEventListener(
        'touchmove',
        (event) => {
          if (!touchDraggedAccountId) {
            return;
          }

          const touch = event.touches[0];
          if (!touch) {
            return;
          }

          if (!touchDragReady) {
            return;
          }

          const target = document.elementFromPoint(touch.clientX, touch.clientY)?.closest('[data-sort-account-id]');
          if (!target || target === row || !(target instanceof HTMLElement)) {
            return;
          }

          event.preventDefault();
          touchDragChanged = true;
          const parent = row.parentElement;
          if (!parent) {
            return;
          }

          const targetRect = target.getBoundingClientRect();
          const shouldInsertAfter = touch.clientY > targetRect.top + targetRect.height / 2;
          target.classList.add('drag-over');

          if (shouldInsertAfter) {
            parent.insertBefore(row, target.nextElementSibling);
          } else {
            parent.insertBefore(row, target);
          }
        },
        { passive: false }
      );

      row.addEventListener('touchend', async () => {
        if (touchDragTimer) {
          clearTimeout(touchDragTimer);
          touchDragTimer = null;
        }

        row.classList.remove('touch-drag-armed');
        row.classList.remove('is-dragging');
        document.querySelectorAll('[data-sort-account-id]').forEach((item) => {
          item.classList.remove('drag-over');
        });

        if (!touchDraggedAccountId || !touchDragReady) {
          touchDraggedAccountId = '';
          touchDragChanged = false;
          touchDragReady = false;
          row.dataset.dragArmed = '';
          return;
        }

        const nextOrderIds = Array.from(document.querySelectorAll('[data-sort-account-id]')).map(
          (item) => item.dataset.sortAccountId ?? ''
        );
        const currentOrderIds = listAccountsCache.map((item) => item.accountId);
        const changed =
          touchDragChanged &&
          nextOrderIds.length === currentOrderIds.length &&
          nextOrderIds.some((item, index) => item !== currentOrderIds[index]);

        touchDraggedAccountId = '';
        touchDragChanged = false;
        touchDragReady = false;
        row.dataset.dragArmed = '';

        if (!changed) {
          return;
        }

        const cacheMap = new Map(listAccountsCache.map((item) => [item.accountId, item]));
        listAccountsCache = nextOrderIds.map((accountId) => cacheMap.get(accountId)).filter(Boolean);

        try {
          await api('/api/accounts/reorder', {
            method: 'POST',
            body: JSON.stringify({ accountIds: nextOrderIds })
          });
          void render();
        } catch (error) {
          window.alert(error instanceof Error ? error.message : '排序保存失败');
          void render();
        }
      });

      row.addEventListener('touchcancel', () => {
        if (touchDragTimer) {
          clearTimeout(touchDragTimer);
          touchDragTimer = null;
        }

        touchDraggedAccountId = '';
        touchDragChanged = false;
        touchDragReady = false;
        row.dataset.dragArmed = '';
        row.classList.remove('touch-drag-armed');
        row.classList.remove('is-dragging');
        document.querySelectorAll('[data-sort-account-id]').forEach((item) => {
          item.classList.remove('drag-over');
        });
        void render();
      });

      row.addEventListener('mouseup', () => {
        row.dataset.dragArmed = '';
        row.draggable = false;
      });

      row.addEventListener('mouseleave', () => {
        if (!draggedAccountId) {
          row.dataset.dragArmed = '';
          row.draggable = false;
        }
      });
    });
  }

  const startRedeemButton = document.querySelector('#start-redeem');
  startRedeemButton?.addEventListener('click', async () => {
    const redeemCodeInput = document.querySelector('#redeem-code');
    const nextRedeemCode = redeemCodeInput?.value.trim() ?? '';

    if (!nextRedeemCode) {
      redeemLogs = [{ level: 'error', message: '请输入兑换码。' }];
      redeemSummary = null;
      void render();
      return;
    }

    redeemCode = nextRedeemCode;
    redeemIsRunning = true;
    redeemProcessed = 0;
    redeemTotal = 0;
    redeemSummary = null;
    redeemLogs = [{ level: 'info', message: `准备开始兑换，兑换码: ${redeemCode}` }];
    redeemStatuses = Object.fromEntries(
      redeemAccounts
        .filter((account) => account.status === ACCOUNT_STATUS.pending)
        .map((account) => [account.accountId, { code: ACCOUNT_STATUS.pending, text: '等待处理' }])
    );
    void render();

    try {
      const result = await api('/api/redeem/run', {
        method: 'POST',
        body: JSON.stringify({ giftCode: redeemCode })
      });

      if (result.ok) {
        redeemSummary = result.data;
      } else {
        redeemLogs = [
          ...redeemLogs,
          {
            level: result.error === '兑换已手动停止。' ? 'warn' : 'error',
            message: result.error
          }
        ];
      }
    } finally {
      redeemIsRunning = false;
      void render();
    }
  });

  const retryFailedRedeemButton = document.querySelector('#retry-failed-redeem');
  retryFailedRedeemButton?.addEventListener('click', async () => {
    const failedAccountIds = getRetryableAccountIds();

    if (failedAccountIds.length === 0) {
      redeemLogs = [...redeemLogs, { level: 'warn', message: '当前没有失败账号可重新兑换。' }];
      void render();
      return;
    }

    const redeemCodeInput = document.querySelector('#redeem-code');
    const nextRedeemCode = redeemCodeInput?.value.trim() ?? redeemCode;
    if (!nextRedeemCode) {
      redeemLogs = [...redeemLogs, { level: 'error', message: '重新兑换前请先输入兑换码。' }];
      void render();
      return;
    }

    redeemCode = nextRedeemCode;
    redeemIsRunning = true;
    redeemProcessed = 0;
    redeemTotal = 0;
    redeemSummary = null;

    for (const accountId of failedAccountIds) {
      redeemStatuses[accountId] = { code: ACCOUNT_STATUS.pending, text: '等待重试' };
    }

    void render();

    try {
      const result = await api('/api/redeem/retry-failed', {
        method: 'POST',
        body: JSON.stringify({ giftCode: redeemCode, accountIds: failedAccountIds })
      });
      if (result.ok) {
        redeemSummary = result.data;
      } else {
        redeemLogs = [
          ...redeemLogs,
          {
            level: result.error === '兑换已手动停止。' ? 'warn' : 'error',
            message: result.error
          }
        ];
      }
    } finally {
      redeemIsRunning = false;
      void render();
    }
  });

  const stopRedeemButton = document.querySelector('#stop-redeem');
  stopRedeemButton?.addEventListener('click', async () => {
    if (!redeemIsRunning) {
      return;
    }

    stopRedeemButton.disabled = true;

    try {
      await api('/api/redeem/stop', { method: 'POST' });
      redeemLogs = [...redeemLogs, { level: 'warn', message: '已发送停止请求，等待当前任务终止。' }];
    } catch (error) {
      redeemLogs = [...redeemLogs, { level: 'error', message: error instanceof Error ? error.message : '停止兑换失败。' }];
      stopRedeemButton.disabled = false;
    } finally {
      if (currentRoute === 'redeem') {
        void render();
      }
    }
  });

  const forceCompleteRedeemButton = document.querySelector('#force-complete-redeem');
  forceCompleteRedeemButton?.addEventListener('click', async () => {
    try {
      const result = await api('/api/redeem/force-complete-all', { method: 'POST' });
      redeemLogs = [...redeemLogs, { level: 'warn', message: `已强制将 ${result.updated} 个账号设为已兑换。` }];
      redeemStatuses = {};
      redeemSummary = null;
      redeemAccounts = await api('/api/accounts');
    } catch (error) {
      redeemLogs = [...redeemLogs, { level: 'error', message: error instanceof Error ? error.message : '强制设置失败。' }];
    } finally {
      if (currentRoute === 'redeem') {
        void render();
      }
    }
  });

  const saveRedeemTokenButton = document.querySelector('#save-redeem-token');
  saveRedeemTokenButton?.addEventListener('click', async () => {
    const redeemTokenInput = document.querySelector('#redeem-token');
    const nextRedeemToken = redeemTokenInput?.value.trim() ?? '';
    try {
      const config = await api('/api/config/redeem-token', {
        method: 'POST',
        body: JSON.stringify({ token: nextRedeemToken })
      });
      redeemToken = config.redeemToken;
      redeemLogs = [...redeemLogs, { level: 'success', message: '兑换 TOKEN 已保存。' }];
    } catch (error) {
      redeemLogs = [...redeemLogs, { level: 'error', message: error instanceof Error ? error.message : 'TOKEN 保存失败。' }];
    } finally {
      if (currentRoute === 'redeem') {
        void render();
      }
    }
  });

  document.querySelectorAll('[data-delete-account]').forEach((button) => {
    button.addEventListener('click', async () => {
      const accountId = button.dataset.deleteAccount;
      if (!accountId) {
        return;
      }
      button.disabled = true;
      try {
        await api(`/api/accounts/${encodeURIComponent(accountId)}`, { method: 'DELETE' });
        await render();
      } finally {
        button.disabled = false;
      }
    });
  });

  document.querySelectorAll('.avatar-image').forEach((image) => {
    image.addEventListener('click', (event) => {
      event.stopPropagation();
      const lightbox = document.querySelector('#avatar-lightbox');
      const lightboxImage = document.querySelector('#avatar-lightbox-image');
      const src = image.getAttribute('src');
      const alt = image.getAttribute('alt') || '头像预览';

      if (!lightbox || !lightboxImage || !src) {
        return;
      }

      lightboxImage.setAttribute('src', src);
      lightboxImage.setAttribute('alt', alt);
      lightbox.hidden = false;
    });

    image.addEventListener(
      'error',
      () => {
        const fallback = document.createElement('span');
        fallback.className = 'avatar-fallback';
        fallback.textContent = '无头像';
        image.replaceWith(fallback);
      },
      { once: true }
    );
  });

  const avatarLightbox = document.querySelector('#avatar-lightbox');
  const avatarLightboxImage = document.querySelector('#avatar-lightbox-image');
  avatarLightbox?.addEventListener('click', () => {
    avatarLightbox.hidden = true;
    avatarLightboxImage?.removeAttribute('src');
  });

  avatarLightboxImage?.addEventListener('click', (event) => {
    event.stopPropagation();
    const currentLightbox = document.querySelector('#avatar-lightbox');
    const currentLightboxImage = document.querySelector('#avatar-lightbox-image');
    if (currentLightbox) {
      currentLightbox.hidden = true;
    }
    currentLightboxImage?.removeAttribute('src');
  });
}

function ensureRedeemProgressSubscription() {
  if (redeemProgressSubscribed) {
    return;
  }

  redeemProgressSubscribed = true;
}

function refreshRedeemUi() {
  if (currentRoute !== 'redeem') {
    return;
  }

  const progressPercent = redeemTotal > 0 ? Math.round((redeemProcessed / redeemTotal) * 100) : 0;
  const progressBar = document.querySelector('.redeem-progress-bar span');
  const progressText = document.querySelector('.redeem-progress-text');
  const startRedeemButton = document.querySelector('#start-redeem');
  const stopRedeemButton = document.querySelector('#stop-redeem');
  const retryFailedRedeemButton = document.querySelector('#retry-failed-redeem');
  const forceCompleteRedeemButton = document.querySelector('#force-complete-redeem');
  const saveRedeemTokenButton = document.querySelector('#save-redeem-token');
  const redeemCodeInput = document.querySelector('#redeem-code');
  const redeemTokenInput = document.querySelector('#redeem-token');
  const retryableAccountIds = getRetryableAccountIds();

  if (progressBar) {
    progressBar.style.width = `${progressPercent}%`;
  }
  if (progressText) {
    progressText.textContent = `进度 ${redeemProcessed} / ${redeemTotal}`;
  }
  if (startRedeemButton) {
    startRedeemButton.disabled = redeemIsRunning;
    startRedeemButton.textContent = redeemIsRunning ? '处理中...' : '开始兑换';
  }
  if (stopRedeemButton) {
    stopRedeemButton.disabled = !redeemIsRunning;
  }
  if (retryFailedRedeemButton) {
    retryFailedRedeemButton.disabled = redeemIsRunning || retryableAccountIds.length === 0;
  }
  if (forceCompleteRedeemButton) {
    forceCompleteRedeemButton.disabled = redeemIsRunning;
  }
  if (saveRedeemTokenButton) {
    saveRedeemTokenButton.disabled = redeemIsRunning;
  }
  if (redeemCodeInput) {
    redeemCodeInput.disabled = redeemIsRunning;
  }
  if (redeemTokenInput) {
    redeemTokenInput.disabled = redeemIsRunning;
  }

  redeemAccounts.forEach((account) => {
    const statusView = getRedeemStatusView(account);
    const badge = document.querySelector(`[data-redeem-status="${CSS.escape(account.accountId)}"]`);
    if (!badge) {
      return;
    }

    badge.className = `status-badge status-${statusView.code}`;
    badge.textContent = statusView.text;
  });
}

function refreshCreateUi() {
  if (currentRoute !== 'create') {
    return;
  }

  const submitButton = document.querySelector('#submit-accounts');
  const textarea = document.querySelector('#account-ids');
  const progressBar = document.querySelector('.create-progress .redeem-progress-bar span');
  const progressTexts = document.querySelectorAll('.create-progress .redeem-progress-text');
  const progressPercent = importTotal > 0 ? Math.round((importProcessed / importTotal) * 100) : 0;

  if (submitButton) {
    submitButton.disabled = importIsRunning;
    submitButton.textContent = importIsRunning ? '录入中...' : '提交';
  }

  if (textarea) {
    textarea.disabled = importIsRunning;
  }

  if (progressBar) {
    progressBar.style.width = `${progressPercent}%`;
  }

  if (progressTexts[0]) {
    progressTexts[0].textContent = `录入进度 ${importProcessed} / ${importTotal}，成功 ${importInserted}，跳过 ${importSkipped}，失败 ${importFailed}`;
  }

  if (progressTexts[1]) {
    progressTexts[1].textContent = importCurrentAccountId ? `当前处理：${importCurrentAccountId}` : '';
  }

  if (!document.querySelector('.create-progress') && (importIsRunning || importTotal > 0)) {
    void render();
  }
}

function syncRedeemStatusFromLog(message) {
  const matched = message.match(/\(([^)]+)\)/);
  if (!matched) {
    return;
  }
  const accountId = matched[1];

  if (message.includes('开始处理')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.pending, text: '处理中' };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.pending);
    return;
  }
  if (message.includes('登录成功')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.pending, text: '登录成功' };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.pending);
    return;
  }
  if (message.includes('兑换成功')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.redeemed, text: '兑换成功' };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.redeemed);
    return;
  }
  if (message.includes('已领取')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.redeemed, text: '已领取' };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.redeemed);
    return;
  }
  if (message.includes('登录失败')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.failed, text: extractFailureReason(message, '登录失败') };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.failed);
    return;
  }
  if (message.includes('登录请求失败')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.failed, text: extractFailureReason(message, '登录请求失败') };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.failed);
    return;
  }
  if (message.includes('兑换请求失败')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.failed, text: extractFailureReason(message, '兑换请求失败') };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.failed);
    return;
  }
  if (message.includes('兑换失败')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.failed, text: extractFailureReason(message, '兑换失败') };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.failed);
    return;
  }
  if (message.includes('异常')) {
    redeemStatuses[accountId] = { code: ACCOUNT_STATUS.failed, text: extractFailureReason(message, '异常') };
    updateLocalAccountStatus(accountId, ACCOUNT_STATUS.failed);
  }
}

function extractFailureReason(message, fallback) {
  const separatorIndex = message.lastIndexOf(' - ');
  if (separatorIndex !== -1) {
    const separatedReason = message.slice(separatorIndex + 3).trim();
    return separatedReason || fallback;
  }

  const colonIndex = message.indexOf(':');
  if (colonIndex === -1) {
    return fallback;
  }

  const reason = message.slice(colonIndex + 1).trim();
  const cleanedReason = reason
    .replace(/^[^(]+?\([^)]+\)\s*/u, '')
    .replace(/\([^)]+\)\s*$/u, '')
    .trim();

  return cleanedReason || fallback;
}

function getRetryableAccountIds() {
  return redeemAccounts
    .filter((account) => getRedeemStatusView(account).code === ACCOUNT_STATUS.failed)
    .map((account) => account.accountId);
}

function getDefaultRedeemStatus(status) {
  if (status === ACCOUNT_STATUS.redeemed) {
    return { code: ACCOUNT_STATUS.redeemed, text: '已兑换' };
  }
  if (status === ACCOUNT_STATUS.failed) {
    return { code: ACCOUNT_STATUS.failed, text: '兑换失败' };
  }
  return { code: ACCOUNT_STATUS.pending, text: '未兑换' };
}

function updateLocalAccountStatus(accountId, status) {
  const account = redeemAccounts.find((item) => item.accountId === accountId);
  if (account) {
    account.status = status;
  }
}

function showFeedback(target, message, isError) {
  target.hidden = false;
  target.textContent = message;
  target.dataset.state = isError ? 'error' : 'success';
}

function escapeHtml(value) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

window.addEventListener('hashchange', () => {
  currentRoute = getRouteFromHash();
  void render();
});

currentRoute = getRouteFromHash();

async function bootstrap() {
  try {
    const status = await api('/api/auth/status');
    authChecked = true;
    isAuthenticated = Boolean(status.authenticated);
    authUsername = status.username || '';
    authRole = status.role || '';
  } catch {
    authChecked = true;
    isAuthenticated = false;
    authUsername = '';
    authRole = '';
  }

  if (isAuthenticated) {
    ensureEventSource();
    ensureImportEventSource();
  }

  await render();
}

void bootstrap();
