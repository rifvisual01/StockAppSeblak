// ── State ──
// Offline-first storage adapter: IndexedDB is the source of truth, Supabase syncs when online.
(function setupStockStore() {
  const DB_NAME = 'stock-seblak-nyakcut-offline-db';
  const DB_VERSION = 1;
  const RECORD_STORE = 'records';
  const OUTBOX_STORE = 'outbox';
  const cfg = window.STOCK_APP_CONFIG || {};
  const supabaseKeyCandidates = [
    cfg.supabasePublishableKey,
    cfg.supabaseAnonKey
  ].filter((key, index, keys) => key && key !== 'PASTE_SUPABASE_ANON_KEY_HERE' && keys.indexOf(key) === index);
  let activeSupabaseKey = supabaseKeyCandidates[0] || '';
  const tables = {
    users: cfg.supabaseTables?.users || 'users',
    items: cfg.supabaseTables?.items || 'items',
    stockLogs: cfg.supabaseTables?.stockLogs || 'stock_logs'
  };
  const isSupabaseReady = Boolean(
    cfg.supabaseUrl &&
    activeSupabaseKey &&
    window.supabase?.createClient
  );

  let db = null;
  let handler = null;
  let syncPromise = null;
  let client = null;
  let lastSyncStatus = {
    configured: isSupabaseReady,
    keyType: activeSupabaseKey?.startsWith('sb_publishable_') ? 'publishable' : 'anon',
    syncing: false,
    lastSyncedAt: null,
    lastError: null,
    pendingCount: 0,
    localCount: 0
  };

  function ok(data) { return { isOk: true, data }; }
  function fail(error) { return { isOk: false, error: error?.message || String(error) }; }

  function openDb() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const nextDb = req.result;
        if (!nextDb.objectStoreNames.contains(RECORD_STORE)) {
          nextDb.createObjectStore(RECORD_STORE, { keyPath: '__backendId' });
        }
        if (!nextDb.objectStoreNames.contains(OUTBOX_STORE)) {
          nextDb.createObjectStore(OUTBOX_STORE, { keyPath: 'queueId', autoIncrement: true });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function tx(storeName, mode, action) {
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(storeName, mode);
      const store = transaction.objectStore(storeName);
      const request = action(store);
      transaction.oncomplete = () => resolve(request?.result);
      transaction.onerror = () => reject(transaction.error);
      transaction.onabort = () => reject(transaction.error);
    });
  }

  function getAll(storeName) {
    return tx(storeName, 'readonly', store => store.getAll());
  }

function emitDataChanged(records) {
    const clean = (records || [])
      .filter(record => !record._deleted)
      .sort((a, b) => String(a.__backendId).localeCompare(String(b.__backendId)));
    handler?.onDataChanged?.(clean);
  }

  async function readLocalRecords() {
    return await getAll(RECORD_STORE);
  }

  async function getLocal(id) {
    return await tx(RECORD_STORE, 'readonly', store => store.get(id));
  }

  async function putLocal(record) {
    await tx(RECORD_STORE, 'readwrite', store => store.put(record));
  }

  async function removeLocal(id) {
    await tx(RECORD_STORE, 'readwrite', store => store.delete(id));
  }

  async function enqueue(operation, record) {
    await tx(OUTBOX_STORE, 'readwrite', store => store.add({
      operation,
      record,
      createdAt: new Date().toISOString()
    }));
  }

  async function removeQueueItem(queueId) {
    await tx(OUTBOX_STORE, 'readwrite', store => store.delete(queueId));
  }

  async function refreshSyncStatus(patch = {}) {
    const [records, queued] = db
      ? await Promise.all([readLocalRecords(), getAll(OUTBOX_STORE)])
      : [[], []];
    lastSyncStatus = {
      ...lastSyncStatus,
      configured: Boolean(client),
      keyType: activeSupabaseKey?.startsWith('sb_publishable_') ? 'publishable' : 'anon',
      localCount: records.filter(record => !record._deleted).length,
      pendingCount: queued.length,
      ...patch
    };
    window.stockAppSyncStatus = { ...lastSyncStatus };
    return window.stockAppSyncStatus;
  }

  function deletedAt(record) {
    return record._deleted ? new Date().toISOString() : null;
  }

  function tableForType(type) {
    if (type === 'user') return tables.users;
    if (type === 'item') return tables.items;
    if (type === 'tx') return tables.stockLogs;
    return null;
  }

  function toUserRow(record) {
    return {
      id: record.__backendId,
      name: record.name || '',
      pin: record.pin || '',
      role: record.role || 'staff',
      updated_at: record.updated_at || new Date().toISOString(),
      deleted_at: deletedAt(record)
    };
  }

  function toItemRow(record) {
    return {
      id: record.__backendId,
      name: record.name || '',
      category: record.category || '',
      price: Number(record.price) || 0,
      tracks_stock: record.tracks_stock !== false,
      stock: Number(record.stock) || 0,
      min_stock: Number(record.min_stock) || 0,
      updated_at: record.updated_at || new Date().toISOString(),
      deleted_at: deletedAt(record)
    };
  }

  function toStockLogRow(record) {
    return {
      id: record.__backendId,
      item_id: record.item_id || null,
      item_name: record.name || '',
      category: record.category || '',
      price: Number(record.price) || 0,
      user_name: record.user_name || '',
      tx_type: record.tx_type || 'OUT',
      qty: Number(record.qty) || 0,
      stock_before: Number(record.stock_before) || 0,
      stock_after: Number(record.stock_after) || 0,
      note: record.note || '',
      created_at: record.timestamp || record.updated_at || new Date().toISOString(),
      updated_at: record.updated_at || record.timestamp || new Date().toISOString(),
      deleted_at: deletedAt(record)
    };
  }

  function toSupabaseRow(record) {
    if (record.type === 'user') return toUserRow(record);
    if (record.type === 'item') return toItemRow(record);
    if (record.type === 'tx') return toStockLogRow(record);
    return null;
  }

  function fromUserRow(row) {
    return {
      __backendId: row.id,
      type: 'user',
      name: row.name,
      pin: row.pin,
      role: row.role,
      updated_at: row.updated_at,
      _deleted: Boolean(row.deleted_at)
    };
  }

  function fromItemRow(row) {
    return {
      __backendId: row.id,
      type: 'item',
      name: row.name,
      category: row.category || '',
      price: row.price || 0,
      tracks_stock: row.tracks_stock !== false,
      stock: row.stock || 0,
      min_stock: row.min_stock || 0,
      updated_at: row.updated_at,
      _deleted: Boolean(row.deleted_at)
    };
  }

  function fromStockLogRow(row) {
    return {
      __backendId: row.id,
      type: 'tx',
      name: row.item_name,
      category: row.category || '',
      price: row.price || 0,
      stock: 0,
      min_stock: 0,
      pin: '',
      role: '',
      item_id: row.item_id,
      user_name: row.user_name,
      tx_type: row.tx_type,
      qty: row.qty || 0,
      stock_before: row.stock_before || 0,
      stock_after: row.stock_after || 0,
      note: row.note || '',
      timestamp: row.created_at || row.updated_at,
      updated_at: row.updated_at,
      _deleted: Boolean(row.deleted_at)
    };
  }

  async function upsertRecord(record) {
    const table = tableForType(record.type);
    const row = toSupabaseRow(record);
    if (!table || !row) return;

    const { error } = await client
      .from(table)
      .upsert(row, { onConflict: 'id' });

    if (error) throw error;
  }

  async function deleteRemoteRecord(record) {
    const table = tableForType(record.type);
    if (!table || !record.__backendId) return;

    const { error } = await client
      .from(table)
      .delete()
      .eq('id', record.__backendId);

    if (error) throw error;
  }

  async function pushOutbox() {
    if (!client || !navigator.onLine) return;
    const queued = (await getAll(OUTBOX_STORE)).sort((a, b) => a.queueId - b.queueId);
    for (const item of queued) {
      if (item.operation === 'delete') {
        await deleteRemoteRecord(item.record);
        await removeLocal(item.record.__backendId);
      } else {
        await upsertRecord(item.record);
      }
      await removeQueueItem(item.queueId);
    }
  }

  async function pullRemote() {
    if (!client || !navigator.onLine) return;
    const queries = [
      client.from(tables.users).select('id,name,pin,role,updated_at,deleted_at').order('updated_at', { ascending: true }),
      client.from(tables.items).select('id,name,category,price,tracks_stock,stock,min_stock,updated_at,deleted_at').order('updated_at', { ascending: true }),
      client.from(tables.stockLogs).select('id,item_id,item_name,category,price,user_name,tx_type,qty,stock_before,stock_after,note,created_at,updated_at,deleted_at').order('updated_at', { ascending: true })
    ];

    const [usersResult, itemsResult, logsResult] = await Promise.all(queries);
    const error = usersResult.error || itemsResult.error || logsResult.error;
    if (error) throw error;

    const remoteRecords = [
      ...(usersResult.data || []).map(fromUserRow),
      ...(itemsResult.data || []).map(fromItemRow),
      ...(logsResult.data || []).map(fromStockLogRow)
    ];
    const remoteIds = new Set(remoteRecords.map(record => record.__backendId));
    const queued = await getAll(OUTBOX_STORE);
    const queuedIds = new Set(queued.map(item => item.record?.__backendId).filter(Boolean));

    for (const record of remoteRecords) {
      if (record._deleted) {
        await removeLocal(record.__backendId);
      } else {
        const local = await getLocal(record.__backendId);
        if (local?._deleted && new Date(local.updated_at) >= new Date(record.updated_at)) {
          continue;
        }
        await putLocal(record);
      }
    }

    const localRecords = await readLocalRecords();
    for (const local of localRecords) {
      if (queuedIds.has(local.__backendId)) continue;
      if (!['user', 'item', 'tx'].includes(local.type)) continue;
      if (!remoteIds.has(local.__backendId)) {
        await removeLocal(local.__backendId);
      }
    }
    emitDataChanged(await readLocalRecords());
  }

  async function syncNow() {
    if (!client || syncPromise) return syncPromise;
    syncPromise = (async () => {
      try {
        await refreshSyncStatus({ syncing: true, lastError: null });
        await pushOutbox();
        await pullRemote();
        await refreshSyncStatus({ syncing: false, lastSyncedAt: new Date().toISOString(), lastError: null });
      } catch (error) {
        await refreshSyncStatus({ syncing: false, lastError: error.message || String(error) });
        console.warn('[SYNC] Pending until online/configured:', error.message || error);
      } finally {
        syncPromise = null;
      }
    })();
    return syncPromise;
  }

  async function syncAfterWrite() {
    await syncNow();
    return await refreshSyncStatus();
  }

  async function connectSupabase() {
    let lastError = null;
    for (const key of supabaseKeyCandidates) {
      activeSupabaseKey = key;
      client = window.supabase.createClient(cfg.supabaseUrl, key);
      await syncNow();
      if (!lastSyncStatus.lastError) {
        return true;
      }
      lastError = lastSyncStatus.lastError;
    }
    await refreshSyncStatus({ lastError });
    return false;
  }

  window.stockStore = {
    async init(dataHandler) {
      try {
        handler = dataHandler;
        db = await openDb();
        emitDataChanged(await readLocalRecords());
        await refreshSyncStatus();

        if (isSupabaseReady) {
          window.addEventListener('online', syncNow);
          window.addEventListener('focus', syncNow);
          document.addEventListener('visibilitychange', () => {
            if (!document.hidden) syncNow();
          });
          setInterval(syncNow, 30000);
          await connectSupabase();
        } else {
          console.warn('[SYNC] Supabase key belum diisi. Aplikasi berjalan offline-only sampai konfigurasi lengkap.');
        }
        return ok();
      } catch (error) {
        return fail(error);
      }
    },

    async create(record) {
      try {
        const next = {
          ...record,
          __backendId: record.__backendId || genId(),
          updated_at: new Date().toISOString()
        };
        await putLocal(next);
        await enqueue('upsert', next);
        emitDataChanged(await readLocalRecords());
        await refreshSyncStatus();
        syncNow();
        return ok(next);
      } catch (error) {
        return fail(error);
      }
    },

    async update(record) {
      try {
        const next = { ...record, updated_at: new Date().toISOString() };
        await putLocal(next);
        await enqueue('upsert', next);
        emitDataChanged(await readLocalRecords());
        await refreshSyncStatus();
        syncNow();
        return ok(next);
      } catch (error) {
        return fail(error);
      }
    },

    async delete(record) {
      try {
        const deleted = { ...record, _deleted: true, updated_at: new Date().toISOString() };
        await putLocal(deleted);
        await enqueue('delete', deleted);
        emitDataChanged(await readLocalRecords());
        await refreshSyncStatus();
        syncNow();
        return ok(deleted);
      } catch (error) {
        return fail(error);
      }
    },

    syncNow,
    syncAfterWrite,

    async getSyncStatus() {
      try {
        return ok(await refreshSyncStatus());
      } catch (error) {
        return fail(error);
      }
    }
  };
})();

window.allData = [];
window.currentUser = null;
window.currentView = 'dashboard';
window.searchTerm = '';
window.txFilter = 'all';
window.categoryFilter = 'all';
window.showMobileMenu = false; // CRITICAL: Proper state tracking
window.showStockModal = null;
window.reportDateStart = '';
window.reportDateEnd = '';
window.reportSelectedUser = null;
window.dashboardTodayModal = false;
window.showTopItemsModal = false;
window.topItemsFullList = [];
window.isLoading = false;
window.showItemForm = false;
window.editingItem = null;
window.txType = 'OUT';
window.txCart = [];
window.txItemSearch = '';
window.txCategoryFilter = 'all';
window.showTxCartDetail = false;
window.historyDateStart = '';
window.historyDateEnd = '';
window.showCreateUser = false;
window.editingUserPin = null;
window.confirmMsg = '';
window.confirmCallback = null;
window.pinModal = { show:false, title:'', cb:null, error:'' };
window.changePinModal = { show:false, userId:'', newPin:'', error:'' };

// State tracking untuk prevent double binding
let mobileMenuOverlayBound = false;
let eventsBound = false;
let appInitialized = false;

const defaultConfig = {
  app_title: 'Seblak Nyakcut',
  company_name: 'Management Stock',
  background_color: '#fff1f2',
  surface_color: '#ffffff',
  text_color: '#1f2937',
  primary_color: '#dc2626',
  secondary_color: '#6b7280',
  font_family: 'Plus Jakarta Sans',
  font_size: 16
};

function isValidViewForCurrentUser(view) {
  const adminViews = ['reports', 'users'];
  const knownViews = ['dashboard', 'items', 'transaction', 'history', ...adminViews];
  if (!knownViews.includes(view)) return false;
  if (adminViews.includes(view) && window.currentUser?.role !== 'admin') return false;
  return true;
}

function syncBrowserHistory(view, mode = 'push') {
  if (!window.currentUser || !window.history?.pushState) return;
  const nextView = isValidViewForCurrentUser(view) ? view : 'dashboard';
  const state = { stockAppView: nextView };
  const hash = nextView === 'dashboard' ? '#dashboard' : `#${nextView}`;
  if (mode === 'replace' || nextView === 'dashboard') {
    history.replaceState(state, '', hash);
  } else {
    history.replaceState({ stockAppView: 'dashboard' }, '', '#dashboard');
    history.pushState(state, '', hash);
  }
}

function goToView(view, options = {}) {
  const nextView = isValidViewForCurrentUser(view) ? view : 'dashboard';
  window.currentView = nextView;
  window.searchTerm = '';
  window.txFilter = 'all';
  window.categoryFilter = 'all';
  window.showMobileMenu = false;
  if (options.resetTxSearch) {
    window.txItemSearch = '';
    window.txCategoryFilter = 'all';
  }
  if (options.history !== false) {
    syncBrowserHistory(nextView, options.replaceHistory ? 'replace' : 'push');
  }
  render();
}

function safeCreateIcons() {
  if (window.lucide && typeof window.lucide.createIcons === 'function') {
    try {
      window.lucide.createIcons();
    } catch (error) {
      console.warn('[UI] Lucide icons gagal dibuat, aplikasi tetap dilanjutkan:', error?.message || error);
    }
  }
}
window.safeCreateIcons = safeCreateIcons;

// ── Data Handler ──
let dataRenderVersion = 0;
let lastDataSignature = '';

const dataHandler = {
  onDataChanged(data) {
    const nextData = data || [];
    const nextSignature = JSON.stringify(nextData.map(record => [
      record.__backendId,
      record.type,
      record.updated_at || '',
      record.stock ?? '',
      record.name || '',
      record._deleted ? 1 : 0
    ]));

    if (nextSignature === lastDataSignature) return;
    lastDataSignature = nextSignature;
    dataRenderVersion++;
    window.allData = nextData;
    render();
  }
};

// ── Helpers ──
function getItems() { return window.allData.filter(d => d.type === 'item'); }
function getUsers() { return window.allData.filter(d => d.type === 'user'); }
function getTxs() { return window.allData.filter(d => d.type === 'tx').sort((a,b) => new Date(b.timestamp) - new Date(a.timestamp)); }
function genId() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }

function itemUsesStock(item) {
  return item?.tracks_stock !== false;
}

function getStockStatus(item) {
  if (!itemUsesStock(item)) return { label:'Non-stok', cls:'bg-blue-100 text-blue-700', icon:'minus-circle' };
  if (item.stock <= 0) return { label:'Habis', cls:'bg-red-100 text-red-700', icon:'alert-circle' };
  if (item.stock <= item.min_stock) return { label:'Menipis', cls:'bg-amber-100 text-amber-700', icon:'alert-triangle' };
  return { label:'Aman', cls:'bg-emerald-100 text-emerald-700', icon:'check-circle' };
}

function formatCurrency(n) { return 'Rp ' + (n||0).toLocaleString('id-ID'); }
function getTxUnitPrice(tx, items = getItems()) {
  if (tx && tx.price !== undefined && tx.price !== null) {
    const snapshotPrice = Number(tx.price);
    return Number.isFinite(snapshotPrice) ? snapshotPrice : 0;
  }
  const item = items.find(i => i.__backendId === tx?.item_id) || items.find(i => i.name === tx?.name);
  return Number(item?.price) || 0;
}
function getTxValue(tx, items = getItems()) {
  return getTxUnitPrice(tx, items) * (Number(tx?.qty) || 0);
}
function formatDate(ts) {
  if (!ts) return '-';
  const d = new Date(ts);
  return d.toLocaleDateString('id-ID',{day:'2-digit',month:'short',year:'numeric'}) + ' ' + d.toLocaleTimeString('id-ID',{hour:'2-digit',minute:'2-digit'});
}
function getCategories() {
  const cats = new Set(getItems().map(i=>i.category).filter(Boolean));
  return [...cats].sort();
}

// ── Toast ──
let toastTimer = null;
function showToast(msg, type='success') {
  let t = document.getElementById('toast-container');
  if (!t) { t = document.createElement('div'); t.id='toast-container'; t.className='fixed top-4 right-4 z-50'; document.body.appendChild(t); }
  const colors = type==='success'?'bg-emerald-600':'bg-red-600';
  const icon = type==='success'?'check-circle':'alert-circle';
  t.innerHTML = `<div class="toast-enter ${colors} text-white px-5 py-3 rounded-xl shadow-lg flex items-center gap-3 text-sm font-medium"><i data-lucide="${icon}" style="width:18px;height:18px"></i>${msg}</div>`;
  safeCreateIcons();
  clearTimeout(toastTimer);
  toastTimer = setTimeout(()=>{ const el=t.firstChild; if(el){el.classList.remove('toast-enter');el.classList.add('toast-exit');setTimeout(()=>{t.innerHTML='';},300);} },2500);
}

function showSyncResult(status, successMsg) {
  lastRenderKey = '';
  render();
  if (status?.lastError) {
    showToast('Tersimpan lokal, sync gagal: ' + status.lastError, 'error');
    return;
  }
  if (status?.pendingCount > 0) {
    showToast(`Tersimpan lokal, ${status.pendingCount} data menunggu sync`, 'error');
    return;
  }
  showToast(successMsg);
}

// ── Inline Confirm ──
let confirmCallback = null;
let confirmMsg = '';
function showConfirm(msg, cb) { window.confirmMsg=msg; window.confirmCallback=cb; render(); }
function hideConfirm() { window.confirmMsg=''; window.confirmCallback=null; render(); }

// ── PIN Modal ──
let pinModal = window.pinModal;
function showPinModal(title, cb) { window.pinModal={show:true,title,cb,error:''}; pinModal = window.pinModal; render(); }
function hidePinModal() { window.pinModal={show:false,title:'',cb:null,error:''}; pinModal = window.pinModal; render(); }

function isBlockingModalOpen() {
  return Boolean(
    window.showCreateUser ||
    window.showItemForm ||
    window.pinModal?.show ||
    window.changePinModal?.show ||
    window.confirmMsg ||
    window.showTxCartDetail ||
    window.showStockModal ||
    window.showTopItemsModal
  );
}

// ── Render ──
let lastRenderKey = '';
function render() {
  const app = document.getElementById('app');
  const cfg = window.elementSdk?.config || defaultConfig;

  // Generate render key - PENTING: include mobileMenu state
  const renderKey = JSON.stringify({
    user: window.currentUser?.__backendId,
    view: window.currentView,
    showForm: window.showItemForm,
    editItem: window.editingItem?.__backendId,
    showCreateUser: window.showCreateUser,
    txCart: window.txCart.map(item => `${item.itemId}:${item.qty}`).join('|'),
    showTxDetail: window.showTxCartDetail,
    confirmMsg: window.confirmMsg,
    pinShow: window.pinModal.show,
    changePinShow: window.changePinModal.show,
    stockModal: window.showStockModal,
    topItems: window.showTopItemsModal,
    todayModal: window.dashboardTodayModal,
    mobileMenu: window.showMobileMenu, // CRITICAL: track mobile menu state
    search: window.searchTerm,
    txFilter: window.txFilter,
    categoryFilter: window.categoryFilter,
    txType: window.txType,
    txItemSearch: window.txItemSearch,
    txCategoryFilter: window.txCategoryFilter,
    reportDateStart: window.reportDateStart,
    reportDateEnd: window.reportDateEnd,
    reportSelectedUser: window.reportSelectedUser?.name || '',
    historyDateStart: window.historyDateStart,
    historyDateEnd: window.historyDateEnd,
    dataVersion: dataRenderVersion,
  });

  if (renderKey === lastRenderKey) return;
  lastRenderKey = renderKey;

  if (!window.currentUser) {
    app.innerHTML = renderLogin(cfg);
  } else {
    app.innerHTML = renderMain(cfg);
  }
  safeCreateIcons();
  
  // Re-bind form/input handlers after DOM replacement; keep delegated click handler single.
  bindEvents();
  restorePendingFocus();
}

function restorePendingFocus() {
  const pending = window.pendingFocus;
  if (!pending?.id) return;
  window.pendingFocus = null;
  requestAnimationFrame(() => {
    const el = document.getElementById(pending.id);
    if (!el) return;
    el.focus();
    if (typeof el.setSelectionRange === 'function') {
      const pos = Math.min(pending.cursor ?? el.value.length, el.value.length);
      el.setSelectionRange(pos, pos);
    }
  });
}

function renderLogin(cfg) {
  const users = getUsers();
  const hasAdmin = users.some(user => user.role === 'admin');
  const title = cfg.app_title || defaultConfig.app_title;
  const company = 'Management Stock';

  return `
  <div class="login-screen h-full w-full flex items-center justify-center p-4 bg-gradient-to-br from-red-50 via-white to-red-50 relative overflow-hidden">
    <!-- Background Elements -->
    <div class="absolute top-0 right-0 w-96 h-96 bg-red-200/20 rounded-full blur-3xl -z-10" style="animation: float 6s ease-in-out infinite"></div>
    <div class="absolute -bottom-32 -left-32 w-80 h-80 bg-red-100/30 rounded-full blur-3xl -z-10" style="animation: float 8s ease-in-out infinite reverse"></div>
    
    <div class="login-card w-full max-w-xs fade-in relative z-10">
      <!-- Header Section with Premium Feel -->
      <div class="text-center mb-10">
        <!-- Icon Badge -->
        <div class="inline-flex items-center justify-center mb-4 mt-3 relative">
          <div class="absolute inset-0 bg-red-600/20 blur-2xl rounded-full w-40 h-40 -z-10"></div>
          <img src="./seblak-nyakcut-logo.png" alt="Seblak Nyakcut" class="login-app-icon w-36 h-36 rounded-3xl shadow-2xl transform hover:scale-105 transition-transform duration-300 object-cover">
        </div>
        
        <!-- Title & Subtitle -->
        <h1 class="app-page-title text-5xl font-bold text-red-950 mb-2 leading-tight" id="login-title">${title}</h1>
        <p class="text-red-700 text-xl font-900 font-black" id="login-company">${company}</p>
        <div class="h-1 w-20 bg-gradient-to-r from-red-600 to-orange-400 rounded-full mx-auto my-4"></div>
        <p class="text-gray-500 text-sm mt-2">Kelola stok dan transaksi dalam satu tempat</p>
      </div>

      <!-- Users Container -->
      <div class="mb-8">
        ${users.length > 0 ? `
          <div class="space-y-3" style="animation:slideUp 0.6s ease-out">
            ${users.map((u, idx) => `
              <button data-login-user="${u.__backendId}" class="w-full group relative overflow-hidden rounded-2xl transition-all duration-300 transform hover:-translate-y-1" style="animation: slideInUp ${0.2 + idx * 0.1}s cubic-bezier(0.34, 1.56, 0.64, 1)">
                <div class="absolute inset-0 bg-gradient-to-r ${u.role==='admin'?'from-red-600 to-red-500':'from-white to-red-50'} opacity-0 group-hover:opacity-100 transition-opacity duration-300 -z-10"></div>
                <div class="relative px-6 py-4 flex items-center gap-4 text-left backdrop-blur-sm border ${u.role==='admin'?'border-red-400/30 bg-gradient-to-r from-red-600/90 to-red-500/90 text-white shadow-xl':'border-gray-200/60 bg-white/80 text-gray-800 group-hover:border-red-300/50 group-hover:shadow-lg'}">
                  <!-- Avatar -->
                  <div class="w-14 h-14 rounded-2xl flex items-center justify-center text-sm font-900 shrink-0 transform group-hover:scale-110 transition-transform duration-300 ${u.role==='admin'?'bg-white/20 text-white':'bg-gradient-to-br from-red-100 to-red-50 text-red-600 border border-red-200/50'}">
                    ${(u.name||'?')[0].toUpperCase()}
                  </div>
                  
                  <!-- Info -->
                  <div class="flex-1 min-w-0">
                    <div class="font-700 text-base">${u.name}</div>
                    <div class="text-xs ${u.role==='admin'?'text-white/80':'text-gray-500'} font-600 mt-1">${u.role==='admin'?'👑 Administrator':'👤 Staff'}</div>
                  </div>
                  
                  <!-- Arrow Icon -->
                  <div class="w-9 h-9 rounded-full flex items-center justify-center ${u.role==='admin'?'bg-white/20':'bg-red-100/60'} group-hover:translate-x-1 transition-transform duration-300 shrink-0">
                    <i data-lucide="arrow-right" style="width:18px;height:18px;color:${u.role==='admin'?'white':'#dc2626'}"></i>
                  </div>
                </div>
              </button>
            `).join('')}
          </div>
        ` : `
          <div class="text-center py-12 rounded-2xl border-2 border-dashed border-red-200 bg-red-50/50 backdrop-blur-sm">
            <div class="inline-flex items-center justify-center w-16 h-16 bg-red-100 rounded-2xl mb-4 border-2 border-red-300/50">
              <i data-lucide="user-plus" style="width:28px;height:28px;color:#dc2626"></i>
            </div>
            <p class="text-gray-700 text-sm font-600">Belum ada akun<br><span class="text-gray-500 text-xs mt-1 block">Buat Admin untuk memulai</span></p>
          </div>
        `}
      </div>

      ${!hasAdmin ? `<!-- Primary Button -->
      <button id="btn-create-user" class="w-full py-4 rounded-2xl font-700 text-base transition-all duration-300 transform active:scale-95 flex items-center justify-center gap-2 backdrop-blur-sm shadow-lg hover:shadow-2xl ${users.length===0?'bg-gradient-to-r from-red-600 to-red-500 text-white hover:from-red-700 hover:to-red-600':'bg-gradient-to-r from-gray-200 to-gray-100 text-gray-800 hover:from-gray-300 hover:to-gray-200 border border-gray-300/50'}" style="animation: slideInUp 0.4s cubic-bezier(0.34, 1.56, 0.64, 1)">
        <i data-lucide="plus" style="width:20px;height:20px"></i>
        <span>${users.length===0?'Buat Akun Admin':'Tambah User'}</span>
      </button>` : ''}

      <!-- Footer -->
      <p class="text-center text-xs text-red-700/60 mt-6 font-700">Sistem manajemen stok terintegrasi</p>
    </div>
    
    <style>
      @keyframes glow { 0%, 100% { box-shadow: 0 0 30px rgba(220,38,38,0.4), 0 0 60px rgba(220,38,38,0.2); } 50% { box-shadow: 0 0 40px rgba(220,38,38,0.6), 0 0 80px rgba(220,38,38,0.3); } }
      @keyframes slideUp { from { opacity:0; transform:translateY(20px); } to { opacity:1; transform:translateY(0); } }
      @keyframes slideInUp { from { opacity:0; transform:translateY(30px); } to { opacity:1; transform:translateY(0); } }
      @keyframes float { 0%, 100% { transform:translateY(0px); } 50% { transform:translateY(-30px); } }
    </style>
    
    ${renderCreateUserModal(cfg)}
    ${renderPinModalHTML()}
  </div>`;
}

function renderCreateUserModal(cfg) {
  if (!window.showCreateUser) return '';
  const isFirst = getUsers().length === 0;
  return `
  <div class="modal-overlay fade-in">
    <div class="modal-content p-6">
      <h3 class="font-700 text-lg text-gray-800 mb-4">${isFirst?'Buat Akun Admin':'Tambah User Baru'}</h3>
      <form id="form-create-user" class="space-y-4">
        <div>
          <label class="block text-sm font-600 text-gray-700 mb-1">Nama</label>
          <input id="cu-name" type="text" class="w-full border border-gray-300 rounded-xl px-4 py-2.5 text-sm" placeholder="Nama lengkap" required>
        </div>
        ${!isFirst ? `
        <div>
          <label class="block text-sm font-600 text-gray-700 mb-1">Role</label>
          <select id="cu-role" class="w-full border border-gray-300 rounded-xl px-4 py-2.5 text-sm">
            <option value="staff">Staff</option>
            <option value="admin">Admin</option>
          </select>
        </div>` : ''}
        <div>
          <label class="block text-sm font-600 text-gray-700 mb-1">PIN (4 digit)</label>
          <input id="cu-pin" type="password" maxlength="4" pattern="\\d{4}" class="w-full border border-gray-300 rounded-xl px-4 py-2.5 text-sm tracking-widest" placeholder="••••" required>
        </div>
        <div class="flex gap-3 pt-2">
          <button type="button" id="btn-cancel-cu" class="flex-1 py-2.5 rounded-xl border border-gray-300 text-sm font-600 text-gray-600 hover:bg-gray-50">Batal</button>
          <button type="submit" class="flex-1 py-2.5 rounded-xl bg-red-600 text-white text-sm font-600 btn-primary hover:bg-red-700">Buat</button>
        </div>
      </form>
    </div>
  </div>`;
}

function renderChangePinModal() {
  if (!window.changePinModal.show) return '';
  return `
  <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 fade-in">
    <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-xs p-6 text-center">
      <div class="inline-flex items-center justify-center w-14 h-14 bg-blue-100 rounded-full mb-4">
        <i data-lucide="key" style="width:24px;height:24px;color:#3b82f6"></i>
      </div>
      <h3 class="font-700 text-lg text-gray-800 mb-1">Ubah PIN</h3>
      <p class="text-gray-500 text-sm mb-4">Masukkan PIN baru (4 digit)</p>
      <input id="new-pin-input" type="password" maxlength="4" pattern="\\d{4}" class="w-full border border-gray-300 rounded-xl px-4 py-3 text-center text-2xl tracking-[0.5em] font-700 mb-2" placeholder="••••" autofocus>
      ${window.changePinModal.error?`<p class="text-red-600 text-xs mb-3">${window.changePinModal.error}</p>`:''}
      <div class="flex gap-3">
        <button id="btn-change-pin-cancel" class="flex-1 py-2.5 rounded-xl border border-gray-300 text-sm font-600 text-gray-600 hover:bg-gray-50">Batal</button>
        <button id="btn-change-pin-submit" class="flex-1 py-2.5 rounded-xl bg-blue-600 text-white text-sm font-600 btn-primary">Ubah</button>
      </div>
    </div>
  </div>`;
}

function renderPinModalHTML() {
  if (!window.pinModal.show) return '';
  return `
  <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 fade-in">
    <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-xs p-6 text-center">
      <div class="inline-flex items-center justify-center w-14 h-14 bg-red-100 rounded-full mb-4">
        <i data-lucide="lock" style="width:24px;height:24px;color:#dc2626"></i>
      </div>
      <h3 class="font-700 text-lg text-gray-800 mb-1">${window.pinModal.title}</h3>
      <p class="text-gray-500 text-sm mb-4">Masukkan PIN 4 digit</p>
      <input id="pin-input" type="password" maxlength="4" class="w-full border border-gray-300 rounded-xl px-4 py-3 text-center text-2xl tracking-[0.5em] font-700" placeholder="••••" autofocus>
      ${window.pinModal.error?`<p class="text-red-600 text-xs mt-2">${window.pinModal.error}</p>`:''}
      <div class="flex gap-3 mt-5">
        <button id="btn-pin-cancel" class="flex-1 py-2.5 rounded-xl border border-gray-300 text-sm font-600 text-gray-600 hover:bg-gray-50">Batal</button>
        <button id="btn-pin-submit" class="flex-1 py-2.5 rounded-xl bg-red-600 text-white text-sm font-600 btn-primary">Verifikasi</button>
      </div>
    </div>
  </div>`;
}

// ── Main Layout ──
function renderMain(cfg) {
  const title = cfg.app_title || defaultConfig.app_title;
  const isAdmin = window.currentUser.role === 'admin';
  const menuItems = [
    { id:'dashboard', icon:'layout-dashboard', label:'Dashboard' },
    { id:'items', icon:'package', label:'Barang' },
    { id:'transaction', icon:'arrow-left-right', label:'Transaksi' },
    { id:'history', icon:'history', label:'Riwayat' },
  ];
  if (isAdmin) {
    menuItems.push({ id:'reports', icon:'bar-chart-3', label:'Laporan' });
    menuItems.push({ id:'users', icon:'users', label:'Users' });
  }

  // Filter visible menu items
  const visibleMenuItems = menuItems.filter(m => !m.adminOnly || isAdmin);

  return `
  <div class="app-shell h-full w-full flex flex-col" style="background:linear-gradient(135deg, ${cfg.background_color||defaultConfig.background_color} 0%, #fee2e2 100%)">
    <!-- Top Bar -->
    <header class="app-header bg-gradient-to-r from-red-600 via-red-500 to-red-600 px-4 lg:px-6 py-4 flex items-center justify-between shrink-0 shadow-lg z-50">
      <div class="flex items-center gap-4">
        <button id="btn-mobile-menu" class="lg:hidden p-2 rounded-lg hover:bg-red-700 text-white transition">
          <i data-lucide="${window.showMobileMenu?'x':'menu'}" style="width:22px;height:22px"></i>
        </button>
        <div class="flex items-center gap-3">
          <img src="./seblak-nyakcut-logo.png" alt="Seblak Nyakcut" class="w-10 h-10 rounded-xl object-cover shadow-md hover:shadow-lg transition transform hover:scale-105">
          <div class="hidden sm:block">
            <div class="font-800 text-white text-base leading-tight" id="main-title">${title}</div>
            <div class="text-white/90 text-xs font-800 mt-0.5">Management Stock</div>
          </div>
        </div>
      </div>
      <div class="flex items-center gap-3">
        ${renderSyncBadge()}
        ${renderNotifBadge()}
        <div class="hidden sm:flex items-center gap-3 pl-3 border-l border-white/30">
          <div class="w-10 h-10 rounded-full flex items-center justify-center text-sm font-800 bg-white/20 text-white border-2 border-white/40 shadow-md">
            ${(window.currentUser.name||'?')[0].toUpperCase()}
          </div>
          <div class="hidden lg:block">
            <div class="text-sm font-700 text-white">${window.currentUser.name}</div>
            <div class="text-xs text-white/80 font-600 mt-0.5">${window.currentUser.role==='admin'?'👑 Admin':'👤 Staff'}</div>
          </div>
        </div>
        <button id="btn-logout" class="p-2 rounded-lg hover:bg-red-700 text-white/80 hover:text-white transition duration-200">
          <i data-lucide="log-out" style="width:18px;height:18px"></i>
        </button>
      </div>
    </header>
    <div class="flex flex-1 min-h-0">
      <!-- Desktop Sidebar -->
      <nav class="app-sidebar hidden lg:flex flex-col w-56 bg-white border-r border-gray-200 py-4 shrink-0">
        ${visibleMenuItems.map(m => `
          <button data-nav="${m.id}" class="sidebar-item mx-2 px-4 py-2.5 rounded-l-xl flex items-center gap-3 text-sm font-500 ${window.currentView===m.id?'active text-red-700 font-600 bg-red-50':'text-gray-600 hover:bg-gray-50'}">
            <i data-lucide="${m.icon}" style="width:18px;height:18px"></i>${m.label}
          </button>
        `).join('')}
      </nav>
      <!-- Mobile Menu -->
      ${window.showMobileMenu ? `
        <div class="fixed inset-0 z-30 lg:hidden fade-in" id="mobile-overlay" role="button" style="background:rgba(0,0,0,0.4); top: 4rem;"></div>
        <nav class="fixed left-0 top-16 w-64 bg-white shadow-2xl overflow-y-auto z-40 slide-in border-r border-gray-200" style="height: calc(100% - 4rem);">
          <div class="px-4 py-4 border-b border-gray-100 bg-gradient-to-r from-red-50 to-white">
            <div class="font-700 text-gray-800 text-base">${title}</div>
            <div class="text-xs text-gray-500 mt-1">Menu Navigasi</div>
          </div>
          <div class="divide-y divide-gray-100">
            ${visibleMenuItems.map(m => `
              <button data-nav="${m.id}" class="sidebar-item w-full px-4 py-3 flex items-center gap-3 text-sm font-500 text-left hover:bg-red-50 transition ${window.currentView===m.id?'active text-red-700 font-600 bg-red-50 border-r-3 border-r-red-600':'text-gray-600'}">
                <i data-lucide="${m.icon}" style="width:18px;height:18px"></i><span>${m.label}</span>
              </button>
            `).join('')}
          </div>
        </nav>
      ` : ''}
      <!-- Content -->
      <main class="app-main flex-1 overflow-auto p-4 lg:p-6">
        ${renderContent(cfg)}
      </main>
    </div>
    ${window.confirmMsg ? renderConfirmModal() : ''}
    ${window.pinModal.show ? renderPinModalHTML() : ''}
    ${window.showStockModal ? renderStockModal() : ''}
    ${window.showTopItemsModal ? renderTopItemsModal() : ''}
    ${renderCreateUserModal(cfg)}
    ${renderChangePinModal()}
  </div>`;
}

function renderNotifBadge() {
  const items = getItems();
  const stockItems = items.filter(itemUsesStock);
  const low = stockItems.filter(i => i.stock > 0 && i.stock <= i.min_stock).length;
  const out = stockItems.filter(i => i.stock <= 0).length;
  const total = low + out;
  if (total === 0) return '';
  return `<button id="btn-notif-bell" class="relative hover:opacity-90 transition transform hover:scale-110"><i data-lucide="bell" style="width:20px;height:20px;color:white"></i><span class="absolute -top-2 -right-2 w-5 h-5 bg-yellow-300 text-red-700 text-[11px] font-900 rounded-full flex items-center justify-center shadow-lg">${total}</span></button>`;
}

function renderSyncBadge() {
  const status = window.stockAppSyncStatus;
  if (!status) return '';
  if (!status.configured) {
    return `<span class="hidden sm:inline-flex items-center px-2 py-1 rounded-lg bg-white/15 text-white/80 text-[11px] font-800">Offline</span>`;
  }
  if (status.lastError) {
    return `<button id="btn-sync-now" class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-yellow-300 text-red-800 text-[11px] font-900" title="${status.lastError}">Sync Error</button>`;
  }
  if (status.pendingCount > 0 || status.syncing) {
    return `<button id="btn-sync-now" class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-white/20 text-white text-[11px] font-800">${status.syncing ? 'Sync...' : `${status.pendingCount} Pending`}</button>`;
  }
  return `<button id="btn-sync-now" class="hidden sm:inline-flex items-center px-2 py-1 rounded-lg bg-white/15 text-white text-[11px] font-800">Synced</button>`;
}

function renderConfirmModal() {
  return `
  <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 fade-in">
    <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-sm p-6 text-center">
      <div class="inline-flex items-center justify-center w-14 h-14 bg-red-100 rounded-full mb-4">
        <i data-lucide="alert-triangle" style="width:24px;height:24px;color:#dc2626"></i>
      </div>
      <h3 class="font-700 text-lg text-gray-800 mb-2">Konfirmasi</h3>
      <p class="text-gray-500 text-sm mb-5">${window.confirmMsg}</p>
      <div class="flex gap-3">
        <button id="btn-confirm-no" class="flex-1 py-2.5 rounded-xl border border-gray-300 text-sm font-600 text-gray-600 hover:bg-gray-50">Batal</button>
        <button id="btn-confirm-yes" class="flex-1 py-2.5 rounded-xl bg-red-600 text-white text-sm font-600 btn-primary">Ya, Lanjut</button>
      </div>
    </div>
  </div>`;
}

function renderContent(cfg) {
  switch(window.currentView) {
    case 'dashboard': return renderDashboard(cfg);
    case 'items': return renderItems(cfg);
    case 'transaction': return renderTransaction(cfg);
    case 'history': return renderHistory(cfg);
    case 'reports': return renderReports(cfg);
    case 'users': return renderUsers(cfg);
    default: return renderDashboard(cfg);
  }
}

// ── Dashboard ──
function renderDashboard(cfg) {
  const items = getItems();
  const txs = getTxs();
  const stockItems = items.filter(itemUsesStock);
  const safe = stockItems.filter(i=>i.stock>i.min_stock).length;
  const low = stockItems.filter(i=>i.stock>0&&i.stock<=i.min_stock).length;
  const out = stockItems.filter(i=>i.stock<=0).length;
  const todayTxs = txs.filter(t=>{const d=new Date(t.timestamp);const n=new Date();return d.toDateString()===n.toDateString();});
  const todayOut = todayTxs.filter(t=>t.tx_type==='OUT').reduce((s,t)=>s+(t.qty||0),0);
  const todayIn = todayTxs.filter(t=>t.tx_type==='IN').reduce((s,t)=>s+(t.qty||0),0);
  const totalValue = stockItems.reduce((s,i)=>s+(i.price||0)*(i.stock||0),0);

  const alertItems = stockItems.filter(i=>i.stock<=i.min_stock).slice(0,5);
  const lowItems = stockItems.filter(i=>i.stock>0&&i.stock<=i.min_stock);
  const outItems = stockItems.filter(i=>i.stock<=0);

  return `
  <div class="fade-in space-y-6">
    <div>
      <h2 class="text-2xl font-800 text-gray-800">Dashboard</h2>
      <p class="text-gray-500 text-sm mt-1">Ringkasan kondisi stok hari ini</p>
    </div>
    <!-- Stats -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 lg:gap-4">
      <div class="stat-card bg-white rounded-2xl p-4 border border-gray-200">
        <div class="flex items-center gap-3 mb-3"><div class="w-10 h-10 bg-blue-600 rounded-xl flex items-center justify-center"><i data-lucide="package" style="width:20px;height:20px;color:white"></i></div></div>
        <div class="text-2xl font-800 text-gray-800">${items.length}</div>
        <div class="text-xs text-gray-500 font-500 mt-0.5">Total Barang</div>
      </div>
      <div class="stat-card bg-emerald-600 rounded-2xl p-4 border border-emerald-700 text-white">
        <div class="flex items-center gap-3 mb-3"><div class="w-10 h-10 bg-white/20 rounded-xl flex items-center justify-center"><i data-lucide="check-circle" style="width:20px;height:20px;color:white"></i></div></div>
        <div class="text-2xl font-800">${safe}</div>
        <div class="text-xs text-white/80 font-500 mt-0.5">Stok Aman</div>
      </div>
      <button id="btn-dash-low" class="stat-card bg-amber-500 rounded-2xl p-4 border border-amber-600 text-white hover:shadow-lg">
        <div class="flex items-center gap-3 mb-3"><div class="w-10 h-10 bg-white/20 rounded-xl flex items-center justify-center"><i data-lucide="alert-triangle" style="width:20px;height:20px;color:white"></i></div></div>
        <div class="text-2xl font-800">${low}</div>
        <div class="text-xs text-white/80 font-500 mt-0.5">Menipis</div>
      </button>
      <button id="btn-dash-out" class="stat-card bg-red-600 rounded-2xl p-4 border border-red-700 text-white hover:shadow-lg">
        <div class="flex items-center gap-3 mb-3"><div class="w-10 h-10 bg-white/20 rounded-xl flex items-center justify-center"><i data-lucide="alert-circle" style="width:20px;height:20px;color:white"></i></div></div>
        <div class="text-2xl font-800">${out}</div>
        <div class="text-xs text-white/80 font-500 mt-0.5">Habis</div>
      </button>
    </div>
    <!-- Row 2 -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="bg-red-600 rounded-2xl p-5 border border-red-700 text-white">
        <button id="btn-dash-today" class="w-full text-left hover:opacity-90">
          <div class="text-sm font-600 text-white/80 mb-1">Transaksi Hari Ini</div>
          <div class="text-3xl font-800">${todayTxs.length}</div>
          <div class="flex gap-4 mt-3">
            <span class="text-xs font-600 text-white/90 flex items-center gap-1"><i data-lucide="arrow-down" style="width:12px;height:12px"></i>IN: ${todayIn}</span>
            <span class="text-xs font-600 text-white/90 flex items-center gap-1"><i data-lucide="arrow-up" style="width:12px;height:12px"></i>OUT: ${todayOut}</span>
          </div>
        </button>
      </div>
      <div class="bg-white rounded-2xl p-5 border border-gray-200">
        <div class="text-sm font-600 text-gray-500 mb-1">Nilai Stok</div>
        <div class="text-2xl font-800 text-gray-800">${formatCurrency(totalValue)}</div>
        <div class="text-xs text-gray-400 mt-2">Total nilai seluruh barang</div>
      </div>
      <div class="bg-orange-600 rounded-2xl p-5 border border-orange-700 text-white">
        <div class="text-sm font-600 text-white/80 mb-1">Pengeluaran Hari Ini</div>
        <div class="text-2xl font-800">${formatCurrency(todayTxs.filter(t=>t.tx_type==='OUT').reduce((s,t)=>s+getTxValue(t, items),0))}</div>
        <div class="text-xs text-white/80 mt-2">Total nilai barang keluar</div>
      </div>
    </div>
    <!-- Alerts -->
    ${alertItems.length > 0 ? `
    <div class="bg-red-600 rounded-2xl border border-red-700 text-white overflow-hidden">
      <div class="px-5 py-4 border-b border-red-700 bg-red-700 flex items-center gap-2 font-700 text-sm">
        <i data-lucide="alert-triangle" style="width:18px;height:18px;color:white"></i>
        Peringatan Stok (${alertItems.length})
      </div>
      <div class="divide-y divide-red-500">
        ${alertItems.map((i, idx) => {
          const st = getStockStatus(i);
          return `<div class="${idx % 2 === 0 ? 'bg-red-600' : 'bg-red-500'} px-5 py-3 flex items-center justify-between">
            <div class="flex items-center gap-3">
              <span class="px-2.5 py-1 rounded-lg text-xs font-600 ${st.cls}">${st.label}</span>
              <span class="text-sm font-600">${i.name}</span>
              <span class="text-xs text-white/70">${i.category||''}</span>
            </div>
            <div class="text-sm font-700 text-white/90">${i.stock} / min ${i.min_stock}</div>
          </div>`;
        }).join('')}
      </div>
    </div>` : ''}
    <!-- Recent Transactions -->
    <div class="bg-white rounded-2xl border border-gray-100 overflow-hidden">
      <div class="px-5 py-4 border-b border-gray-100 flex items-center justify-between bg-red-600 text-white">
        <span class="font-700 text-sm">Transaksi Terbaru</span>
        <button data-nav="history" class="text-white text-xs font-600 hover:underline">Lihat Semua</button>
      </div>
      ${txs.length === 0 ? `<div class="px-5 py-8 text-center text-gray-400 text-sm">Belum ada transaksi</div>` : `
      <div class="divide-y divide-gray-100">
        ${txs.slice(0,5).map((tx) => `
          <div class="${tx.tx_type==='IN'?'bg-white border-l-4 border-l-emerald-500':'bg-red-600 border-l-4 border-l-red-700'} px-5 py-3 flex items-center justify-between">
            <div class="flex items-center gap-3">
              <div class="w-8 h-8 rounded-lg flex items-center justify-center ${tx.tx_type==='IN'?'bg-emerald-200':'bg-red-200'}">
                <i data-lucide="${tx.tx_type==='IN'?'arrow-down':'arrow-up'}" style="width:14px;height:14px;color:${tx.tx_type==='IN'?'#059669':'#dc2626'}"></i>
              </div>
              <div>
                <div class="text-sm font-600 text-${tx.tx_type==='IN'?'gray-800':'white'}">${tx.name||'-'}</div>
                <div class="text-xs text-${tx.tx_type==='IN'?'gray-500':'white/70'}">${tx.user_name} · ${formatDate(tx.timestamp)}</div>
              </div>
            </div>
            <span class="text-sm font-700 ${tx.tx_type==='IN'?'text-emerald-600':'text-white'}">${tx.tx_type==='IN'?'+':'-'}${tx.qty}</span>
          </div>
        `).join('')}
      </div>`}
    </div>
  </div>`;
}

// ── Items ──
function renderItems(cfg) {
  const items = getItems();
  const cats = getCategories();
  let filtered = items;
  if (searchTerm) filtered = filtered.filter(i=>i.name.toLowerCase().includes(searchTerm.toLowerCase())||i.category?.toLowerCase().includes(searchTerm.toLowerCase()));
  if (categoryFilter !== 'all') filtered = filtered.filter(i=>i.category===categoryFilter);

  return `
  <div class="fade-in space-y-4">
    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
      <div>
        <h2 class="text-2xl font-800 text-gray-800">Barang</h2>
        <p class="text-gray-500 text-sm mt-0.5">${items.length} barang terdaftar</p>
      </div>
      ${currentUser.role==='admin'?`<button id="btn-add-item" class="px-5 py-2.5 bg-red-600 text-white rounded-xl text-sm font-600 btn-primary flex items-center gap-2 self-start"><i data-lucide="plus" style="width:16px;height:16px"></i>Tambah Barang</button>`:''}
    </div>
    <!-- Filters -->
    <div class="flex flex-col sm:flex-row gap-3">
      <div class="relative flex-1">
        <i data-lucide="search" style="width:16px;height:16px;color:#9ca3af" class="absolute left-3 top-1/2 -translate-y-1/2"></i>
        <input id="item-search" type="text" value="${searchTerm}" placeholder="Cari barang..." class="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-xl text-sm">
      </div>
      <select id="cat-filter" class="border border-gray-200 rounded-xl px-4 py-2.5 text-sm bg-white">
        <option value="all">Semua Kategori</option>
        ${cats.map(c=>`<option value="${c}" ${categoryFilter===c?'selected':''}>${c}</option>`).join('')}
      </select>
    </div>
    <!-- Item List Compact -->
    ${filtered.length === 0 ? `
      <div class="bg-white rounded-2xl border border-gray-100 p-12 text-center">
        <div class="inline-flex items-center justify-center w-16 h-16 bg-gray-100 rounded-full mb-4"><i data-lucide="package-x" style="width:28px;height:28px;color:#9ca3af"></i></div>
        <p class="text-gray-500 text-sm">Tidak ada barang ditemukan</p>
      </div>
    ` : `
      <div class="bg-white rounded-2xl border border-gray-100 overflow-hidden">
        <div class="divide-y divide-gray-100">
          ${filtered.map((item, idx) => {
            const st = getStockStatus(item);
            const bgColor = idx % 2 === 0 ? 'bg-red-50 border-l-4 border-l-red-600' : 'bg-white border-l-4 border-l-gray-200';
            return `
            <div class="${bgColor} px-4 py-3 flex items-center gap-3 hover:bg-red-100 transition">
              <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2 gap-y-1 flex-wrap">
                  <span class="font-600 text-gray-800 text-sm">${item.name}</span>
                  ${item.category?`<span class="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-200 text-gray-700">${item.category}</span>`:''}
                  <span class="text-[10px] font-600 px-2 py-0.5 rounded-lg ${st.cls}">${st.label}</span>
                </div>
                <div class="text-xs text-gray-500 mt-1">${formatCurrency(item.price)} · ${itemUsesStock(item) ? `min: ${item.min_stock}` : 'tanpa kontrol stok'}</div>
              </div>
              <div class="text-right shrink-0">
                <div class="text-lg font-800 ${!itemUsesStock(item)?'text-blue-600':item.stock<=0?'text-red-600':item.stock<=item.min_stock?'text-amber-600':'text-emerald-600'}">${itemUsesStock(item) ? item.stock : '∞'}</div>
              </div>
              ${currentUser.role==='admin'?`
              <div class="flex gap-1 ml-2 shrink-0">
                <button data-edit-item="${item.__backendId}" class="p-1.5 rounded-lg hover:bg-blue-100 text-gray-400 hover:text-blue-600 transition"><i data-lucide="edit-2" style="width:14px;height:14px"></i></button>
                <button data-del-item="${item.__backendId}" class="p-1.5 rounded-lg hover:bg-red-100 text-gray-400 hover:text-red-600 transition"><i data-lucide="trash-2" style="width:14px;height:14px"></i></button>
              </div>`:''}
            </div>`;
          }).join('')}
        </div>
      </div>
    `}
    ${window.showItemForm ? renderItemForm() : ''}
  </div>`;
}

function renderItemForm() {
  const isEdit = !!editingItem;
  const usesStock = isEdit ? itemUsesStock(editingItem) : true;
  const categories = getCategories();
  const currentCategory = isEdit ? (editingItem.category || '') : '';
  const categoryOptions = currentCategory && !categories.includes(currentCategory)
    ? [currentCategory, ...categories]
    : categories;
  return `
  <div class="modal-overlay">
    <div class="modal-content compact-modal p-4 sm:p-5">
      <!-- Header dengan Close Button -->
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-700 text-base text-gray-800">${isEdit?'Edit Barang':'Tambah Barang'}</h3>
        <button type="button" id="btn-close-item-modal" class="p-1.5 rounded-lg hover:bg-gray-100 text-gray-400 transition">
          <i data-lucide="x" style="width:18px;height:18px"></i>
        </button>
      </div>

      <form id="form-item" class="space-y-3">
        <!-- Nama Barang -->
        <div>
          <label class="block text-xs font-700 text-gray-700 mb-1.5">Nama Barang <span class="text-red-600">*</span></label>
          <input 
            id="fi-name" 
            type="text" 
            value="${isEdit?editingItem.name:''}" 
            class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:border-red-400 focus:ring-0 transition" 
            placeholder="Contoh: Pulpen Biru"
            required>
        </div>

        <!-- Kategori & Harga (2 kolom) -->
        <div class="grid grid-cols-2 gap-3">
          <!-- Kategori -->
          <div>
            <label class="block text-xs font-700 text-gray-700 mb-1.5">Kategori</label>
            <div class="relative">
              <input
                id="fi-cat"
                type="text"
                value="${currentCategory}"
                class="w-full border border-gray-300 rounded-lg pl-3 pr-9 py-2 text-sm focus:border-red-400 focus:ring-0 transition"
                placeholder="Pilih atau tulis kategori"
                autocomplete="off">
              <button type="button" id="btn-cat-toggle" class="absolute right-1 top-1 h-8 w-8 rounded-md text-gray-500 hover:bg-gray-100" title="Lihat kategori">
                <i data-lucide="chevron-down" style="width:16px;height:16px;margin:auto"></i>
              </button>
              <div id="cat-options" class="hidden absolute z-50 mt-1 w-full max-h-36 overflow-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                ${categoryOptions.length ? categoryOptions.map(c=>`<button type="button" data-cat-option="${c}" class="w-full text-left px-3 py-2 text-sm hover:bg-red-50">${c}</button>`).join('') : '<div class="px-3 py-2 text-xs text-gray-400">Belum ada kategori</div>'}
              </div>
            </div>
          </div>

          <!-- Harga -->
          <div>
            <label class="block text-xs font-700 text-gray-700 mb-1.5">Harga <span class="text-red-600">*</span></label>
            <div class="relative">
              <span class="absolute left-3 top-2 text-gray-500 font-500 text-sm">Rp</span>
              <input 
                id="fi-price" 
                type="number" 
                min="0"
                step="100"
                value="${isEdit?editingItem.price:''}" 
                class="w-full border border-gray-300 rounded-lg pl-9 pr-3 py-2 text-sm focus:border-red-400 focus:ring-0 transition" 
                placeholder="0"
                required>
            </div>
          </div>
        </div>

        <label class="flex items-center justify-between gap-3 rounded-xl border border-gray-200 bg-red-50/60 px-3 py-2">
          <span>
            <span class="block text-xs font-800 text-gray-800">Pakai stok</span>
            <span class="block text-[11px] text-gray-500 mt-0.5">Matikan untuk item yang selalu bisa checkout tanpa batas stok.</span>
          </span>
          <input id="fi-tracks-stock" type="checkbox" class="w-5 h-5 accent-red-600" ${usesStock?'checked':''}>
        </label>

        <!-- Stok Awal & Stok Minimum (2 kolom) -->
        <div id="stock-fields" class="grid grid-cols-2 gap-3 ${usesStock?'':'opacity-50'}">
          <!-- Stok -->
          <div>
            <label class="block text-xs font-700 text-gray-700 mb-1.5">${isEdit?'Stok':'Stok Awal'} <span class="text-red-600">*</span></label>
            <input 
              id="fi-stock" 
              type="number" 
              min="0"
              value="${usesStock ? (isEdit?editingItem.stock:'0') : '0'}" 
              class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:border-red-400 focus:ring-0 transition" 
              placeholder="0"
              ${usesStock?'required':'disabled'}>
          </div>

          <!-- Stok Minimum -->
          <div>
            <label class="block text-xs font-700 text-gray-700 mb-1.5">Stok Minimum <span class="text-red-600">*</span></label>
            <input 
              id="fi-min" 
              type="number" 
              min="0"
              value="${usesStock ? (isEdit?editingItem.min_stock:'5') : '0'}" 
              class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:border-red-400 focus:ring-0 transition" 
              placeholder="5"
              ${usesStock?'required':'disabled'}>
          </div>
        </div>

        <!-- Action Buttons -->
        <div class="flex gap-3 pt-3 border-t border-gray-200">
          <button type="button" id="btn-cancel-item" class="flex-1 py-2.5 rounded-lg border border-gray-300 text-sm font-600 text-gray-700 hover:bg-gray-50 transition duration-200">Batal</button>
          <button type="submit" id="btn-save-item" class="flex-1 py-2.5 rounded-lg bg-red-600 text-white text-sm font-600 btn-primary hover:bg-red-700 transition duration-200 flex items-center justify-center gap-2">
            <i data-lucide="${isEdit?'check':'plus'}" style="width:16px;height:16px"></i>
            <span>${isEdit?'Simpan Perubahan':'Tambah Barang'}</span>
          </button>
        </div>
      </form>
    </div>
  </div>`;
}

function renderTransaction(cfg) {
  const items = getItems();
  const availableItems = txType==='OUT'?items.filter(i=>i.stock>0):items;
  const cats = getCategories();
  
  // Filter by category
  const catFiltered = txCategoryFilter==='all' ? availableItems : availableItems.filter(i => i.category === txCategoryFilter);
  
  // Group items by category
  const grouped = {};
  catFiltered.forEach(i => {
    const cat = i.category || 'Lainnya';
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(i);
  });
  
  // Filter by search
  let filteredCats = cats.length > 0 ? cats : ['Lainnya'];
  if (txItemSearch) {
    const search = txItemSearch.toLowerCase();
    filteredCats = filteredCats.filter(cat =>
      (grouped[cat] || []).some(i => i.name.toLowerCase().includes(search))
    );
  }
  // Only show selected category or all if 'all' is selected
  if (txCategoryFilter !== 'all') {
    filteredCats = filteredCats.filter(c => c === txCategoryFilter);
  }

  return `
  <div class="fade-in space-y-4">
    <div>
      <h2 class="text-2xl font-800 text-gray-800">Transaksi</h2>
      <p class="text-gray-500 text-sm mt-0.5">Klik barang untuk tambah ke keranjang</p>
    </div>
    <!-- Type Toggle -->
    <div class="flex gap-2">
      <button id="btn-tx-in" data-tx-type="IN" class="flex-1 sm:flex-none px-6 py-3 rounded-xl text-sm font-600 flex items-center justify-center gap-2 btn-primary transition ${txType==='IN'?'bg-emerald-600 text-white shadow-lg':'bg-white border border-gray-200 text-gray-600 hover:border-emerald-300'}">
        <i data-lucide="arrow-down" style="width:16px;height:16px"></i>Stok Masuk (IN)
      </button>
      <button id="btn-tx-out" data-tx-type="OUT" class="flex-1 sm:flex-none px-6 py-3 rounded-xl text-sm font-600 flex items-center justify-center gap-2 btn-primary transition ${txType==='OUT'?'bg-red-600 text-white shadow-lg':'bg-white border border-gray-200 text-gray-600 hover:border-red-300'}">
        <i data-lucide="arrow-up" style="width:16px;height:16px"></i>Stok Keluar (OUT)
      </button>
    </div>
    
    <!-- Search -->
    <div class="relative">
      <i data-lucide="search" style="width:16px;height:16px;color:#9ca3af" class="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none"></i>
      <input id="tx-search" type="text" placeholder="Cari barang..." value="${txItemSearch}" class="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-xl text-sm">
    </div>
    
    <!-- Category Filter -->
    <select id="tx-cat-filter" class="border border-gray-200 rounded-xl px-4 py-2.5 text-sm bg-white">
      <option value="all">Semua Kategori</option>
      ${cats.map(c=>`<option value="${c}" ${txCategoryFilter===c?'selected':''}>${c}</option>`).join('')}
    </select>
    
    <!-- Items by Category -->
    <div class="space-y-4 pb-32">
      ${filteredCats.length === 0 ? `
        <div class="bg-white rounded-2xl border border-gray-100 p-12 text-center">
          <div class="inline-flex items-center justify-center w-16 h-16 bg-gray-100 rounded-full mb-4"><i data-lucide="package-x" style="width:28px;height:28px;color:#9ca3af"></i></div>
          <p class="text-gray-500 text-sm">Tidak ada barang ditemukan</p>
        </div>
      ` : filteredCats.map(cat => {
        const catItems = (grouped[cat] || []).filter(i => {
          if (!txItemSearch) return true;
          const search = txItemSearch.toLowerCase();
          return i.name.toLowerCase().includes(search);
        });
        
        if (catItems.length === 0) return '';
        
        return `
        <div class="bg-white rounded-2xl border border-gray-100 overflow-hidden">
          <div class="px-4 py-3 bg-red-50 border-b border-red-200 font-600 text-sm text-gray-800">
            ${cat}
          </div>
          <div class="divide-y divide-gray-100">
            ${catItems.map((item, idx) => {
              const cartItem = txCart.find(c => c.itemId === item.__backendId);
              const qty = cartItem ? cartItem.qty : 0;
              return `
              <div class="w-full p-4 hover:bg-red-50 transition flex items-center justify-between group">
                <div class="flex-1 min-w-0">
                  <div class="font-600 text-gray-800 text-sm">${item.name}</div>
                  <div class="text-xs text-gray-500 mt-1">${formatCurrency(item.price)} · ${itemUsesStock(item) ? `Stok: ${item.stock}` : 'Tanpa stok'}</div>
                </div>
                <div class="flex items-center gap-2 ml-3 shrink-0">
                  ${qty > 0 ? `<button data-quick-minus="${item.__backendId}" class="w-9 h-9 rounded-lg bg-red-100 text-red-700 flex items-center justify-center font-900 hover:bg-red-200 transition">-</button><span class="min-w-8 text-center px-2 py-1 rounded-full bg-red-600 text-white text-sm font-700">${qty}</span>` : ''}
                  <button data-add-tx-item="${item.__backendId}" class="w-10 h-10 rounded-lg bg-red-600 text-white flex items-center justify-center font-900 group-hover:shadow-md hover:bg-red-700 transition" title="Tambah ${item.name}">+</button>
                </div>
              </div>`;
            }).join('')}
          </div>
        </div>`;
      }).join('')}
    </div>
    
    <!-- Cart Bar Fixed Bottom -->
    ${txCart.length > 0 ? `
    <div class="fixed bottom-0 left-0 right-0 bg-gradient-to-t from-red-600 to-red-500 border-t-2 border-red-700 px-4 py-3 shadow-2xl z-30">
      <div class="max-w-7xl mx-auto flex items-center justify-between gap-3">
        <div class="text-white">
          <div class="text-2xl font-800">${txCart.length}</div>
          <div class="text-xs text-white/70">item${txCart.reduce((s, c) => s + c.qty, 0) > 1 ? 's' : ''} (${txCart.reduce((s, c) => s + c.qty, 0)} qty)</div>
        </div>
        <div class="flex gap-2 flex-1 sm:flex-none">
          <button id="btn-submit-tx-from-bar" class="flex-1 sm:flex-auto px-6 py-2.5 rounded-xl ${txType==='IN'?'bg-emerald-600 hover:bg-emerald-700':'bg-red-700 hover:bg-red-800'} text-white text-sm font-600 transition btn-primary">
            <i data-lucide="${txType==='IN'?'arrow-down':'arrow-up'}" style="width:16px;height:16px;display:inline;margin-right:6px"></i>
            ${txType==='IN'?'Input Masuk':'Input Keluar'}
          </button>
          <button id="btn-tx-cancel" class="px-4 py-2.5 rounded-xl border border-white/30 text-white text-sm font-600 hover:bg-white/10 transition">
            <i data-lucide="x" style="width:16px;height:16px;display:inline"></i>
          </button>
        </div>
      </div>
    </div>
    ` : ''}
    
    <!-- Cart Detail Modal (Minimalis 1 Baris) -->
    ${showTxCartDetail ? `
    <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-40 p-4 fade-in">
      <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-4xl p-6 max-h-96 overflow-auto">
        <div class="flex items-center justify-between mb-4">
          <h3 class="font-700 text-lg text-gray-800">Keranjang (${txCart.length} item)</h3>
          <button id="btn-close-cart-detail" class="p-1 rounded-lg hover:bg-gray-100 text-gray-400">
            <i data-lucide="x" style="width:18px;height:18px"></i>
          </button>
        </div>
        <div class="space-y-2 mb-5 max-h-48 overflow-y-auto">
          <div class="${txType==='IN'?'bg-emerald-50 border-emerald-200 text-emerald-800':'bg-red-50 border-red-200 text-red-800'} border rounded-xl px-3 py-2 text-xs font-700">
            Mode: ${txType==='IN'?'STOK MASUK - stok akan bertambah':'STOK KELUAR - stok akan berkurang'}
          </div>
          ${txCart.map((cartItem, idx) => {
            const itemData = getItems().find(i => i.__backendId === cartItem.itemId);
            return `
            <div class="bg-red-50 rounded-xl p-2.5 border border-red-200 flex items-center gap-2 justify-between">
              <div class="flex-1 min-w-0">
                <div class="font-700 text-gray-800 text-sm truncate">${itemData?.name || '?'}</div>
              </div>
              <div class="flex items-center gap-1 shrink-0">
                <button data-qty-minus="${idx}" class="w-6 h-6 rounded bg-red-200 hover:bg-red-300 text-red-700 font-700 text-xs transition">−</button>
                <input data-edit-qty="${idx}" type="number" min="1" value="${cartItem.qty}" class="w-10 border border-red-300 rounded px-1 py-0.5 text-xs text-center font-600">
                <button data-qty-plus="${idx}" class="w-6 h-6 rounded bg-red-200 hover:bg-red-300 text-red-700 font-700 text-xs transition">+</button>
              </div>
              <button data-remove-tx-item="${idx}" class="p-1 rounded hover:bg-red-200 text-red-600 shrink-0">
                <i data-lucide="trash-2" style="width:13px;height:13px"></i>
              </button>
            </div>`;
          }).join('')}
        </div>
        <div class="flex gap-2 pt-3 border-t border-gray-200">
          <button id="btn-close-cart-detail-cancel" class="flex-1 py-2.5 rounded-xl border border-gray-300 text-sm font-600 text-gray-600 hover:bg-gray-50">Batal</button>
          <button id="btn-submit-tx-batch" class="flex-1 py-2.5 rounded-xl ${txType==='IN'?'bg-emerald-600 hover:bg-emerald-700':'bg-red-600 hover:bg-red-700'} text-white text-sm font-600 btn-primary">Konfirmasi ${txType==='IN'?'Stok Masuk':'Stok Keluar'}</button>
        </div>
      </div>
    </div>
    ` : ''}
  </div>`;
}

function renderHistory(cfg) {
  const txs = getTxs();
  let filtered = txs;
  
  // Filter by type
  if (txFilter !== 'all') filtered = filtered.filter(t=>t.tx_type===txFilter);
  
  // Filter by date range
  if (historyDateStart || historyDateEnd) {
    filtered = filtered.filter(t => {
      const txDate = new Date(t.timestamp);
      if (historyDateStart) {
        const start = new Date(historyDateStart);
        start.setHours(0,0,0,0);
        if (txDate < start) return false;
      }
      if (historyDateEnd) {
        const end = new Date(historyDateEnd);
        end.setHours(23,59,59,999);
        if (txDate > end) return false;
      }
      return true;
    });
  }
  
  // Filter by search term
  if (searchTerm) {
    const search = searchTerm.toLowerCase();
    filtered = filtered.filter(t=>
      (t.name && t.name.toLowerCase().includes(search)) ||
      (t.user_name && t.user_name.toLowerCase().includes(search))
    );
  }

  return `
  <div class="fade-in space-y-4">
    <div>
      <h2 class="text-2xl font-800 text-gray-800">Riwayat Transaksi</h2>
      <p class="text-gray-500 text-sm mt-0.5">${txs.length} transaksi tercatat</p>
    </div>
    <!-- Search & Filter Top -->
    <div class="flex flex-col sm:flex-row gap-3">
      <div class="relative flex-1">
        <i data-lucide="search" style="width:16px;height:16px;color:#9ca3af" class="absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none"></i>
        <input id="history-search" type="text" value="${searchTerm}" placeholder="Cari nama barang atau user..." class="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-xl text-sm">
      </div>
      <div class="flex gap-2">
        <button data-tx-filter="all" class="px-4 py-2.5 rounded-xl text-xs font-600 ${txFilter==='all'?'bg-gray-800 text-white':'bg-white border border-gray-200 text-gray-600'}">Semua</button>
        <button data-tx-filter="IN" class="px-4 py-2.5 rounded-xl text-xs font-600 ${txFilter==='IN'?'bg-emerald-600 text-white':'bg-white border border-gray-200 text-gray-600'}">IN</button>
        <button data-tx-filter="OUT" class="px-4 py-2.5 rounded-xl text-xs font-600 ${txFilter==='OUT'?'bg-red-600 text-white':'bg-white border border-gray-200 text-gray-600'}">OUT</button>
      </div>
    </div>
    <!-- Date Filter Minimalis 1 Baris -->
    <div class="bg-gradient-to-r from-red-50 to-white rounded-xl border border-red-200 p-2.5 flex items-center gap-2">
      <div class="flex-1 min-w-0">
        <input id="history-date-start" type="date" value="${historyDateStart}" class="w-full border border-gray-300 rounded-lg px-3 py-1.5 text-xs bg-white focus:border-red-400" title="Dari tanggal">
      </div>
      <span class="text-gray-400 text-xs font-500 px-1">–</span>
      <div class="flex-1 min-w-0">
        <input id="history-date-end" type="date" value="${historyDateEnd}" class="w-full border border-gray-300 rounded-lg px-3 py-1.5 text-xs bg-white focus:border-red-400" title="Sampai tanggal">
      </div>
      <button id="btn-history-clear-date" class="px-3 py-1.5 rounded-lg border border-red-300 text-red-600 text-xs font-600 hover:bg-red-100 transition whitespace-nowrap bg-white">Bersihkan</button>
    </div>
    ${filtered.length === 0 ? `
      <div class="bg-gradient-to-br from-red-50 to-white rounded-2xl border border-red-200 p-12 text-center">
        <div class="inline-flex items-center justify-center w-16 h-16 bg-red-100 rounded-full mb-4"><i data-lucide="inbox" style="width:28px;height:28px;color:#dc2626"></i></div>
        <p class="text-gray-500 text-sm">Tidak ada transaksi ditemukan</p>
      </div>
    ` : `
      <div class="space-y-2">
        ${filtered.slice(0,100).map((tx, idx) => {
          const st = tx.tx_type === 'IN' ? { color: 'emerald', icon: 'arrow-down', bgClass: 'bg-white border-l-emerald-500' } : { color: 'red', icon: 'arrow-up', bgClass: 'bg-red-600 border-l-red-700' };
          return `
          <div class="rounded-xl border transition hover:shadow-md ${st.bgClass} border-l-4">
            <div class="p-4 flex items-start gap-3">
              <div class="w-10 h-10 rounded-xl flex items-center justify-center shrink-0 ${st.color === 'emerald' ? 'bg-emerald-200' : 'bg-red-200'}">
                <i data-lucide="${st.icon}" style="width:18px;height:18px;color:${st.color === 'emerald' ? '#059669' : '#dc2626'}"></i>
              </div>
              <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2 flex-wrap">
                  <span class="font-700 text-sm ${st.color === 'emerald' ? 'text-gray-800' : 'text-white'}">${tx.name||'-'}</span>
                  <span class="px-2 py-0.5 rounded text-[10px] font-700 ${st.color === 'emerald' ? 'bg-emerald-100 text-emerald-700' : 'bg-red-200 text-red-800'}">${tx.tx_type}</span>
                </div>
                <div class="text-xs ${st.color === 'emerald' ? 'text-gray-500' : 'text-white/70'} mt-2 flex flex-wrap gap-4">
                  <span class="flex items-center gap-1"><i data-lucide="user" style="width:12px;height:12px"></i>${tx.user_name}</span>
                  <span class="flex items-center gap-1"><i data-lucide="clock" style="width:12px;height:12px"></i>${formatDate(tx.timestamp)}</span>
                </div>
              </div>
              <div class="text-right shrink-0">
                <div class="text-lg font-800 ${st.color === 'emerald' ? 'text-emerald-600' : 'text-white'}">${tx.tx_type==='IN'?'+':'-'}${tx.qty}</div>
                <div class="text-[10px] ${st.color === 'emerald' ? 'text-gray-400' : 'text-white/60'} mt-1">${tx.stock_before} → ${tx.stock_after}</div>
              </div>
            </div>
          </div>`;
        }).join('')}
      </div>
    `}
  </div>`;
}

// ── Stock Modal ──
function renderStockModal() {
  if (!showStockModal) return '';
  let items = [];
  let title = '';
  if (showStockModal === 'low') {
    items = getItems().filter(i=>i.stock>0&&i.stock<=i.min_stock);
    title = 'Barang Menipis';
  } else if (showStockModal === 'out') {
    items = getItems().filter(i=>i.stock<=0);
    title = 'Barang Habis';
  }
  
  return `
  <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-40 p-4 fade-in">
    <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-2xl p-6 max-h-96 overflow-auto">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-700 text-lg text-gray-800">${title} (${items.length})</h3>
        <button id="btn-close-stock" class="p-1 rounded-lg hover:bg-gray-100 text-gray-400">
          <i data-lucide="x" style="width:18px;height:18px"></i>
        </button>
      </div>
      <div class="space-y-2">
        ${items.map((item, idx) => {
          const st = getStockStatus(item);
          return `
          <div class="${idx % 2 === 0 ? 'bg-red-50 border-l-4 border-l-red-600' : 'bg-white border-l-4 border-l-gray-200'} p-4 rounded-lg flex items-center justify-between">
            <div>
              <div class="font-600 text-gray-800">${item.name}</div>
              <div class="text-xs text-gray-500 mt-1">${item.category||'Tanpa Kategori'} · ${formatCurrency(item.price)}</div>
            </div>
            <div class="text-right">
              <div class="text-lg font-800 ${showStockModal==='low'?'text-amber-600':'text-red-600'}">${item.stock}</div>
              <div class="text-xs text-gray-400">${itemUsesStock(item) ? `min: ${item.min_stock}` : 'tanpa stok'}</div>
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>
  </div>`;
}

// ── Top Items Modal ──
function renderTopItemsModal() {
  if (!window.showTopItemsModal || window.topItemsFullList.length === 0) return '';
  
  return `
  <div class="fixed inset-0 bg-black/50 flex items-center justify-center z-40 p-4 fade-in">
    <div class="modal-box bg-white rounded-2xl shadow-2xl w-full max-w-2xl p-6 max-h-96 overflow-auto">
      <div class="flex items-center justify-between mb-4">
        <h3 class="font-700 text-lg text-gray-800">Semua Barang Keluar (${window.topItemsFullList.length})</h3>
        <button id="btn-close-top-items" class="p-1 rounded-lg hover:bg-gray-100 text-gray-400">
          <i data-lucide="x" style="width:18px;height:18px"></i>
        </button>
      </div>
      <div class="space-y-2">
        ${window.topItemsFullList.map((item, idx) => {
          const maxVal = window.topItemsFullList[0][1];
          return `
          <div class="flex items-center gap-3 p-3 rounded-xl ${idx % 2 === 0 ? 'bg-red-50 border border-red-200' : 'bg-white border border-gray-100'}">
            <span class="w-8 text-center text-xs font-800 ${idx===0?'text-red-600':idx<10?'text-red-500':'text-gray-400'}">${idx+1}</span>
            <div class="flex-1">
              <div class="flex items-center justify-between mb-1">
                <span class="text-sm font-600 text-gray-800">${item[0]}</span>
                <span class="text-sm font-700 text-gray-600">${item[1]}</span>
              </div>
              <div class="w-full bg-gray-100 rounded-full h-2">
                <div class="h-2 rounded-full bg-red-500" style="width:${(item[1]/maxVal*100).toFixed(0)}%"></div>
              </div>
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>
  </div>`;
}

// ── Reports (Admin) ──
function renderReports(cfg) {
  const items = getItems();
  const txs = getTxs();
  
  let filteredTxs = txs;
  if (window.reportDateStart || window.reportDateEnd) {
    filteredTxs = txs.filter(t => {
      const txDate = new Date(t.timestamp);
      if (window.reportDateStart) {
        const start = new Date(window.reportDateStart);
        start.setHours(0,0,0,0);
        if (txDate < start) return false;
      }
      if (window.reportDateEnd) {
        const end = new Date(window.reportDateEnd);
        end.setHours(23,59,59,999);
        if (txDate > end) return false;
      }
      return true;
    });
  }

  const outTxs = filteredTxs.filter(t=>t.tx_type==='OUT');
  const itemOutMap = {};
  outTxs.forEach(t => { itemOutMap[t.name] = (itemOutMap[t.name]||0) + (t.qty||0); });
  const topItems = Object.entries(itemOutMap).sort((a,b)=>b[1]-a[1]).slice(0,5);
  const maxOut = topItems.length > 0 ? topItems[0][1] : 1;
  const allOutItems = Object.entries(itemOutMap).sort((a,b)=>b[1]-a[1]);

  const userRevenueMap = {};
  outTxs.forEach(t => {
    const revenue = getTxValue(t, items);
    if(!userRevenueMap[t.user_name]) userRevenueMap[t.user_name]={qty:0,revenue:0};
    userRevenueMap[t.user_name].qty += (t.qty||0);
    userRevenueMap[t.user_name].revenue += revenue;
  });
  const userStats = Object.entries(userRevenueMap).map(([name,data])=>({name,qty:data.qty,revenue:data.revenue})).sort((a,b)=>b.revenue-a.revenue);

  const catMap = {};
  items.forEach(i => { if(i.category){catMap[i.category]=(catMap[i.category]||0)+1;} });
  const catStats = Object.entries(catMap).sort((a,b)=>b[1]-a[1]);

  const monthMap = {};
  filteredTxs.forEach(t => {
    const d = new Date(t.timestamp);
    const key = d.getFullYear()+'-'+(d.getMonth()+1).toString().padStart(2,'0');
    if(!monthMap[key]) monthMap[key]={in_qty:0,out_qty:0};
    if(t.tx_type==='IN') monthMap[key].in_qty += (t.qty||0);
    else monthMap[key].out_qty += (t.qty||0);
  });
  const months = Object.keys(monthMap).sort().slice(-6);

  const estRevenue = outTxs.reduce((s,t) => {
    return s + getTxValue(t, items);
  }, 0);

  if (window.reportSelectedUser) {
    const user = window.reportSelectedUser;
    const userTxs = filteredTxs.filter(t => t.user_name === user.name);
    const userOutTxs = userTxs.filter(t => t.tx_type === 'OUT');
    const userInTxs = userTxs.filter(t => t.tx_type === 'IN');
    const userRevenue = userOutTxs.reduce((s,t) => {
      return s + getTxValue(t, items);
    }, 0);

    return `<div class="fade-in space-y-6"><div class="flex items-center gap-3 mb-6"><button id="btn-back-report" class="p-2 rounded-lg hover:bg-gray-100"><i data-lucide="arrow-left" style="width:20px;height:20px;color:#6b7280"></i></button><div><h2 class="text-2xl font-800 text-gray-800">Detail ${user.name}</h2><p class="text-gray-500 text-sm mt-0.5">Riwayat transaksi personal</p></div></div><div class="grid grid-cols-3 gap-3"><div class="bg-red-600 rounded-2xl p-4 border border-red-700 text-white text-center"><div class="text-xs font-500 mb-1 text-white/80">Total Keluar</div><div class="text-2xl font-800">${userOutTxs.reduce((s,t)=>s+(t.qty||0),0)}</div></div><div class="bg-emerald-600 rounded-2xl p-4 border border-emerald-700 text-white text-center"><div class="text-xs font-500 mb-1 text-white/80">Total Masuk</div><div class="text-2xl font-800">${userInTxs.reduce((s,t)=>s+(t.qty||0),0)}</div></div><div class="bg-white rounded-2xl p-4 border border-gray-200 text-center"><div class="text-xs text-gray-600 font-500 mb-1">Omzet</div><div class="text-lg font-800 text-gray-800">${formatCurrency(userRevenue)}</div></div></div><div class="bg-white rounded-2xl border border-gray-100 p-5"><h3 class="font-700 text-gray-800 text-sm mb-4">Riwayat Transaksi</h3><div class="space-y-2">${userTxs.length === 0 ? `<p class="text-gray-400 text-sm">Tidak ada transaksi</p>` : userTxs.slice(0, 50).map((tx, idx) => {
          const isIN = tx.tx_type === 'IN';
          return `<div class="${isIN ? 'bg-white border-l-4 border-l-emerald-500' : 'bg-red-600 border-l-4 border-l-red-700'} p-3 rounded-lg flex items-center gap-3 hover:shadow-md transition">
            <div class="w-8 h-8 rounded-lg flex items-center justify-center shrink-0 ${isIN ? 'bg-emerald-200' : 'bg-red-200'}">
              <i data-lucide="${isIN ? 'arrow-down' : 'arrow-up'}" style="width:16px;height:16px;color:${isIN ? '#059669' : '#dc2626'}"></i>
            </div>
            <div class="flex-1 min-w-0">
              <div class="font-600 text-sm ${isIN ? 'text-gray-800' : 'text-white'}">${tx.name}</div>
              <div class="text-xs ${isIN ? 'text-gray-500' : 'text-white/70'} mt-0.5">${formatDate(tx.timestamp)}</div>
            </div>
            <div class="text-right shrink-0">
              <div class="text-lg font-800 ${isIN ? 'text-emerald-600' : 'text-white'}">${isIN ? '+' : '-'}${tx.qty}</div>
            </div>
          </div>`;
        }).join('')}</div></div></div>`;
  }

  return `<div class="fade-in space-y-6"><div><h2 class="text-2xl font-800 text-gray-800">Laporan</h2><p class="text-gray-500 text-sm mt-0.5">Insight operasional berbasis data</p></div><!-- Date Filter Minimalis 1 Baris --><div class="bg-gradient-to-r from-red-50 to-white rounded-xl border border-red-200 p-2.5 flex items-center gap-2"><div class="flex-1 min-w-0"><input id="report-date-start" type="date" value="${window.reportDateStart||''}" class="w-full border border-gray-300 rounded-lg px-3 py-1.5 text-xs bg-white focus:border-red-400" title="Dari tanggal"></div><span class="text-gray-400 text-xs font-500 px-1">–</span><div class="flex-1 min-w-0"><input id="report-date-end" type="date" value="${window.reportDateEnd||''}" class="w-full border border-gray-300 rounded-lg px-3 py-1.5 text-xs bg-white focus:border-red-400" title="Sampai tanggal"></div><button id="btn-clear-date-filter" class="px-3 py-1.5 rounded-lg border border-red-300 text-red-600 text-xs font-600 hover:bg-red-100 transition whitespace-nowrap bg-white">Bersihkan</button></div><div class="grid grid-cols-2 lg:grid-cols-4 gap-3"><div class="bg-red-600 rounded-2xl p-4 border border-red-700 text-center text-white"><div class="text-xs font-500 mb-1 text-white/80">Total Keluar</div><div class="text-2xl font-800">${outTxs.reduce((s,t)=>s+(t.qty||0),0)}</div></div><div class="bg-emerald-600 rounded-2xl p-4 border border-emerald-700 text-center text-white"><div class="text-xs font-500 mb-1 text-white/80">Total Masuk</div><div class="text-2xl font-800">${filteredTxs.filter(t=>t.tx_type==='IN').reduce((s,t)=>s+(t.qty||0),0)}</div></div><div class="bg-white rounded-2xl p-4 border border-gray-200 text-center"><div class="text-xs text-gray-600 font-500 mb-1">Estimasi Pengeluaran</div><div class="text-lg font-800 text-gray-800">${formatCurrency(estRevenue)}</div></div><div class="bg-blue-600 rounded-2xl p-4 border border-blue-700 text-center text-white"><div class="text-xs font-500 mb-1 text-white/80">Total Transaksi</div><div class="text-2xl font-800">${filteredTxs.length}</div></div></div><div class="bg-white rounded-2xl border border-gray-100 p-5"><h3 class="font-700 text-gray-800 text-sm mb-4 flex items-center gap-2"><i data-lucide="trending-up" style="width:16px;height:16px;color:#dc2626"></i>Barang Terlaris (Keluar)</h3>${topItems.length===0?'<p class="text-gray-400 text-sm">Belum ada data</p>':`<div class="space-y-3">${topItems.map((ti,idx) => `<div class="flex items-center gap-3 p-3 rounded-xl ${idx % 2 === 0 ? 'bg-red-50 border border-red-200' : 'bg-white border border-gray-100'}"><span class="w-6 text-center text-xs font-800 ${idx===0?'text-red-600':'text-gray-400'}">${idx+1}</span><div class="flex-1"><div class="flex items-center justify-between mb-1"><span class="text-sm font-600 text-gray-800">${ti[0]}</span><span class="text-sm font-700 text-gray-600">${ti[1]}</span></div><div class="w-full bg-gray-100 rounded-full h-2"><div class="h-2 rounded-full ${idx===0?'bg-red-600':'bg-red-400'}" style="width:${(ti[1]/maxOut*100).toFixed(0)}%"></div></div></div></div>`).join('')}<button id="btn-show-all-items" class="w-full mt-3 py-2 rounded-lg border-2 border-red-300 text-red-600 text-xs font-700 hover:bg-red-50 transition">Lihat Semua</button></div>`}</div><div class="grid grid-cols-1 lg:grid-cols-2 gap-4"><div class="bg-red-600 rounded-2xl border border-red-700 text-white p-5"><h3 class="font-700 text-sm mb-4 flex items-center gap-2"><i data-lucide="users" style="width:16px;height:16px;color:white"></i>Omset per Staff</h3>${userStats.length===0?'<p class="text-white/70 text-sm">Belum ada data</p>':`<div class="space-y-2">${userStats.map((us, idx) => `<button data-select-report-user="${us.name}" class="w-full text-left flex items-center justify-between p-3 ${idx % 2 === 0 ? 'bg-red-700 hover:bg-red-800' : 'bg-red-500 hover:bg-red-600'} rounded-xl transition"><div class="flex items-center gap-2"><div class="w-8 h-8 rounded-full bg-white/20 flex items-center justify-center text-xs font-700 text-white">${us.name[0].toUpperCase()}</div><div><div class="text-sm font-600">${us.name}</div><div class="text-xs text-white/70">${us.qty} item</div></div></div><div class="text-right"><div class="text-sm font-800">${formatCurrency(us.revenue)}</div></div></button>`).join('')}</div>`}</div><div class="bg-white rounded-2xl border border-gray-100 p-5"><h3 class="font-700 text-gray-800 text-sm mb-4 flex items-center gap-2"><i data-lucide="pie-chart" style="width:16px;height:16px;color:#dc2626"></i>Distribusi Kategori</h3>${catStats.length===0?'<p class="text-gray-400 text-sm">Belum ada data</p>':`<div class="space-y-2">${catStats.map((cs,idx) => `<div class="flex items-center justify-between p-2.5 rounded-lg ${idx%2===0?'bg-red-50':'bg-white border border-gray-100'}"><span class="text-sm font-600 text-gray-700">${cs[0]}</span><span class="text-sm font-700 text-gray-800">${cs[1]} barang</span></div>`).join('')}</div>`}</div></div>${months.length>0?`<div class="bg-red-50 rounded-2xl border border-red-200 p-5"><h3 class="font-700 text-gray-800 text-sm mb-4 flex items-center gap-2"><i data-lucide="calendar" style="width:16px;height:16px;color:#dc2626"></i>Tren Bulanan</h3><div class="space-y-2">${months.map((m,idx) => {const d = monthMap[m];return `<div class="flex items-center gap-4 p-2.5 rounded-lg ${idx%2===0?'bg-red-100 border border-red-200':'bg-white border border-gray-100'}"><span class="text-sm font-600 text-gray-700 w-20">${m}</span><span class="text-xs font-600 text-emerald-600">IN: ${d.in_qty}</span><span class="text-xs font-600 text-red-600">OUT: ${d.out_qty}</span></div>`;}).join('')}</div></div>`:''}</div>`;
}

// ── Users (Admin) ──
function renderUsers(cfg) {
  const users = getUsers();
  return `<div class="fade-in space-y-4"><div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3"><div><h2 class="text-2xl font-800 text-gray-800">Manajemen User</h2><p class="text-gray-500 text-sm mt-0.5">${users.length} user terdaftar</p></div><button id="btn-add-user-page" class="px-5 py-2.5 bg-red-600 text-white rounded-xl text-sm font-600 btn-primary flex items-center gap-2 self-start"><i data-lucide="user-plus" style="width:16px;height:16px"></i>Tambah Staff</button></div><div class="grid gap-3">${users.map(u => {const userTxs = getTxs().filter(t=>t.user_name===u.name);return `<div class="bg-gradient-to-r from-red-50 to-white rounded-2xl border border-red-200 p-5 flex items-center gap-4 hover:shadow-md transition"><div class="w-12 h-12 rounded-full flex items-center justify-center text-lg font-800 ${u.role==='admin'?'bg-red-100 text-red-700':'bg-blue-100 text-blue-700'}">${(u.name||'?')[0].toUpperCase()}</div><div class="flex-1 min-w-0"><div class="font-700 text-gray-800">${u.name}</div><div class="flex items-center gap-3 mt-1"><span class="text-xs font-600 px-2 py-0.5 rounded-lg ${u.role==='admin'?'bg-red-100 text-red-700':'bg-blue-100 text-blue-700'}">${u.role==='admin'?'Admin':'Staff'}</span><span class="text-xs text-gray-400">${userTxs.length} transaksi</span>${u.__backendId===currentUser.__backendId?'<span class="text-xs text-gray-400">Login sekarang</span>':''}</div></div><div class="flex gap-2 shrink-0"><button data-change-pin="${u.__backendId}" class="p-2 rounded-lg hover:bg-blue-100 text-gray-400 hover:text-blue-600 transition" title="Ubah PIN"><i data-lucide="key" style="width:16px;height:16px"></i></button><button data-del-user="${u.__backendId}" class="p-2 rounded-lg hover:bg-red-100 text-gray-400 hover:text-red-600 transition" title="Hapus"><i data-lucide="trash-2" style="width:16px;height:16px"></i></button></div></div>`;}).join('')}</div></div>`;
}

// ── Shopping/Draft (Placeholder) ──
function renderShoppingFrame(title, pageHash) {
  const src = `apk%20belanja.html#${pageHash}`;
  return `
  <div class="shopping-page fade-in h-full min-h-[calc(100vh-4.5rem)] flex flex-col">
    <iframe
      class="shopping-frame flex-1 w-full bg-transparent"
      src="${src}"
      title="${title}"
      loading="eager"></iframe>
  </div>`;
}

function renderShopping(cfg) {
  return renderShoppingFrame('Belanja', 'form');
}
function renderShoppingReport(cfg) {
  return renderShoppingFrame('Belanja', 'admin');
}
function renderDraftShopping(cfg) {
  return renderShoppingFrame('Belanja', 'draft');
}


// ── Events ──
function bindEvents() {
  const app = document.getElementById('app');
  if (!app) return;

  // Delegated click handler cukup dipasang sekali di container #app.
  // Form/input handler tetap dicek ulang setiap render karena innerHTML diganti.
  if (!eventsBound) {
    eventsBound = true;
    app.addEventListener('click', handleMainClick, true);
  }

  bindFormHandlers();
  bindInputHandlers();
}

// ==================== MAIN CLICK HANDLER ====================
async function handleMainClick(e) {
  if (isBlockingModalOpen() && !e.target.closest('.modal-content, .modal-box')) {
    e.preventDefault();
    e.stopPropagation();
    return;
  }
  const btn = e.target.closest('button, [role="button"]');
  if (!btn) return;
    if (btn.dataset.loginUser) {
      e.preventDefault();
      e.stopPropagation();
      const uid = btn.dataset.loginUser;
      const user = getUsers().find(u => u.__backendId === uid);
      if (user) {
        window.pinModal = { show: true, title: 'Login sebagai ' + user.name, cb: (pin) => {
          if (pin === user.pin) {
            window.currentUser = user;
            window.currentView = 'dashboard';
            window.pinModal.show = false;
            window.showMobileMenu = false;
            syncBrowserHistory('dashboard', 'replace');
            lastRenderKey = '';
            render();
          }
          else {
            window.pinModal.error = 'PIN salah! Periksa kembali password/PIN.';
            showToast('Password/PIN salah', 'error');
            render();
          }
        }, error: '' };
        lastRenderKey = '';
        render();
      }
      return;
    }

    if (btn.id === 'btn-create-user' || btn.id === 'btn-add-user-page') {
      window.showCreateUser = true;
      render();
      return;
    }

    if (btn.id === 'btn-cancel-cu') {
      window.showCreateUser = false;
      render();
      return;
    }

    // ==================== PIN MODAL ====================
    if (btn.id === 'btn-pin-cancel') {
      window.pinModal = { show: false, title: '', cb: null, error: '' };
      render();
      return;
    }

    if (btn.id === 'btn-pin-submit') {
      const pin = document.getElementById('pin-input')?.value || '';
      if (window.pinModal.cb) window.pinModal.cb(pin);
      return;
    }

    // ==================== MAIN PAGE ====================
    if (btn.id === 'btn-mobile-menu') {
      e.preventDefault();
      e.stopPropagation();
      // Toggle menu state
      window.showMobileMenu = !window.showMobileMenu;
      // Force full render to update DOM
      lastRenderKey = '';
      render();
      return;
    }

    // CLOSE mobile menu when overlay is clicked
    if (btn.id === 'mobile-overlay') {
      e.preventDefault();
      e.stopPropagation();
      window.showMobileMenu = false;
      lastRenderKey = '';
      render();
      return;
    }

    // CLOSE mobile menu when any nav item is clicked
    if (btn.dataset.nav) {
      e.preventDefault();
      lastRenderKey = '';
      goToView(btn.dataset.nav);
      return;
    }

    if (btn.id === 'btn-logout') {
      window.currentUser = null;
      window.currentView = 'dashboard';
      window.showMobileMenu = false; // Reset menu on logout
      if (window.history?.replaceState) history.replaceState({ stockAppView: 'login' }, '', '#login');
      lastRenderKey = '';
      render();
      return;
    }

    if (btn.id === 'btn-sync-now') {
      const status = await window.stockStore.syncAfterWrite?.();
      lastRenderKey = '';
      render();
      if (status?.lastError) {
        showToast('Sync gagal: ' + status.lastError, 'error');
      } else {
        showToast('Data tersinkron ke Supabase');
      }
      return;
    }

    if (btn.id === 'btn-notif-bell') {
      window.showStockModal = null;
      lastRenderKey = '';
      goToView('history');
      return;
    }

    // ==================== DASHBOARD ====================
    if (btn.id === 'btn-dash-low') {
      window.showStockModal = 'low';
      lastRenderKey = '';
      render();
      return;
    }

    if (btn.id === 'btn-dash-out') {
      window.showStockModal = 'out';
      lastRenderKey = '';
      render();
      return;
    }

    if (btn.id === 'btn-close-stock') {
      window.showStockModal = null;
      lastRenderKey = '';
      render();
      return;
    }

    if (btn.id === 'btn-dash-today') {
      const today = new Date().toISOString().slice(0, 10);
      window.historyDateStart = today;
      window.historyDateEnd = today;
      goToView('history');
      return;
    }

    if (btn.id === 'btn-show-all-items') {
      const txs = getTxs();
      const itemOutMap = {};
      txs.filter(t => t.tx_type === 'OUT').forEach(t => { itemOutMap[t.name] = (itemOutMap[t.name] || 0) + (t.qty || 0); });
      window.topItemsFullList = Object.entries(itemOutMap).sort((a, b) => b[1] - a[1]);
      window.showTopItemsModal = true;
      render();
      return;
    }

    if (btn.id === 'btn-close-top-items') {
      window.showTopItemsModal = false;
      render();
      return;
    }

    // ==================== ITEMS ====================
    if (btn.id === 'btn-add-item') {
      window.editingItem = null;
      window.showItemForm = true;
      render();
      return;
    }

    if (btn.id === 'btn-cancel-item' || btn.id === 'btn-close-item-modal') {
      window.showItemForm = false;
      window.editingItem = null;
      render();
      return;
    }

    if (btn.id === 'btn-cat-toggle') {
      e.preventDefault();
      const input = document.getElementById('fi-cat');
      const options = document.getElementById('cat-options');
      if (options) options.classList.toggle('hidden');
      input?.focus();
      return;
    }

    if (btn.dataset.catOption !== undefined) {
      e.preventDefault();
      const input = document.getElementById('fi-cat');
      const options = document.getElementById('cat-options');
      if (input) input.value = btn.dataset.catOption;
      options?.classList.add('hidden');
      input?.focus();
      return;
    }

    if (btn.dataset.editItem) {
      e.preventDefault();
      const itemId = btn.dataset.editItem;
      const item = getItems().find(i => i.__backendId === itemId);
      if (item) { window.editingItem = item; window.showItemForm = true; render(); }
      return;
    }

    if (btn.dataset.delItem) {
      const itemId = btn.dataset.delItem;
      const item = getItems().find(i => i.__backendId === itemId);
      if (item) {
        showConfirm(`Hapus barang "${item.name}"?`, async () => {
          hideConfirm();
          const r = await window.stockStore.delete(item);
          if (r.isOk) { showToast('Barang dihapus'); } else showToast('Gagal menghapus', 'error');
        });
      }
      return;
    }

    // ==================== TRANSACTION ====================
    if (btn.id === 'btn-tx-in') {
      e.preventDefault();
      window.txType = 'IN';
      window.txCart = [];
      window.txItemSearch = '';
      window.txCategoryFilter = 'all';
      window.showTxCartDetail = false;
      render();
      return;
    }

    if (btn.id === 'btn-tx-out') {
      e.preventDefault();
      window.txType = 'OUT';
      window.txCart = [];
      window.txItemSearch = '';
      window.txCategoryFilter = 'all';
      window.showTxCartDetail = false;
      render();
      return;
    }

    if (btn.dataset.addTxItem) {
      e.preventDefault();
      const itemId = btn.dataset.addTxItem;
      const item = getItems().find(i => i.__backendId === itemId);
      if (!item) { showToast('Barang tidak ditemukan', 'error'); return; }
      if (window.txType === 'OUT' && itemUsesStock(item) && item.stock <= 0) { showToast('Stok tidak tersedia', 'error'); return; }

      const existing = window.txCart.findIndex(c => c.itemId === itemId);
      if (existing >= 0) {
        window.txCart[existing].qty += 1;
        if (window.txType === 'OUT' && itemUsesStock(item) && window.txCart[existing].qty > item.stock) {
          window.txCart[existing].qty -= 1;
          showToast('Stok tidak cukup! Tersisa: ' + item.stock, 'error');
          return;
        }
      } else {
        window.txCart.push({ itemId, qty: 1, note: '' });
      }
      render();
      return;
    }

    if (btn.dataset.quickMinus) {
      e.preventDefault();
      const itemId = btn.dataset.quickMinus;
      const existing = window.txCart.findIndex(c => c.itemId === itemId);
      if (existing >= 0) {
        if (window.txCart[existing].qty > 1) {
          window.txCart[existing].qty -= 1;
        } else {
          window.txCart.splice(existing, 1);
        }
        render();
      }
      return;
    }

    if (btn.id === 'btn-submit-tx-from-bar') {
      window.showTxCartDetail = true;
      render();
      return;
    }

    if (btn.id === 'btn-tx-cancel') {
      window.txCart = [];
      window.txItemSearch = '';
      window.txCategoryFilter = 'all';
      window.showTxCartDetail = false;
      render();
      return;
    }

    if (btn.id === 'btn-close-cart-detail' || btn.id === 'btn-close-cart-detail-cancel') {
      window.showTxCartDetail = false;
      render();
      return;
    }

    if (btn.dataset.removeTxItem) {
      e.preventDefault();
      const idx = parseInt(btn.dataset.removeTxItem);
      window.txCart.splice(idx, 1);
      if (window.txCart.length === 0) { window.showTxCartDetail = false; }
      render();
      return;
    }

    if (btn.dataset.qtyMinus) {
      e.preventDefault();
      const idx = parseInt(btn.dataset.qtyMinus);
      if (window.txCart[idx] && window.txCart[idx].qty > 1) {
        window.txCart[idx].qty -= 1;
        render();
      }
      return;
    }

    if (btn.dataset.qtyPlus) {
      e.preventDefault();
      const idx = parseInt(btn.dataset.qtyPlus);
      if (window.txCart[idx]) {
        const itemData = getItems().find(i => i.__backendId === window.txCart[idx].itemId);
        if (!itemData) { showToast('Barang tidak ditemukan', 'error'); return; }
        if (window.txType === 'OUT' && itemUsesStock(itemData) && window.txCart[idx].qty >= itemData.stock) {
          showToast('Stok tidak cukup! Tersisa: ' + itemData.stock, 'error');
          return;
        }
        window.txCart[idx].qty += 1;
        render();
      }
      return;
    }

    if (btn.id === 'btn-submit-tx-batch') {
      if (window.txCart.length === 0) {
        showToast('Keranjang kosong', 'error');
        return;
      }
      document.querySelectorAll('[data-edit-qty]').forEach(input => {
        const idx = parseInt(input.dataset.editQty);
        const qty = parseInt(input.value) || 0;
        if (qty > 0) { window.txCart[idx].qty = qty; }
      });
      const totalQty = window.txCart.reduce((sum, item) => sum + (item.qty || 0), 0);
      const modeText = window.txType === 'IN'
        ? `Konfirmasi stok masuk ${totalQty} qty? Stok barang akan bertambah.`
        : `Konfirmasi stok keluar ${totalQty} qty? Stok barang akan berkurang.`;
      showConfirm(modeText, async () => {
        hideConfirm();
        await processTransactionCart();
      });
      return;
    }

    async function processTransactionCart() {
        let successCount = 0;
        let errorCount = 0;
        const originalCart = [...window.txCart];

        for (const cartItem of originalCart) {
          const item = getItems().find(i => i.__backendId === cartItem.itemId);
          if (!item) continue;

          const usesStock = itemUsesStock(item);
          if (window.txType === 'OUT' && usesStock && cartItem.qty > item.stock) {
            errorCount++;
            continue;
          }

          const stockBefore = usesStock ? item.stock : 0;
          const stockAfter = usesStock
            ? (window.txType === 'IN' ? stockBefore + cartItem.qty : stockBefore - cartItem.qty)
            : 0;

          if (usesStock) {
            const updatedItem = { ...item, stock: stockAfter };
            const r1 = await window.stockStore.update(updatedItem);
            if (!r1.isOk) { errorCount++; continue; }
          }

          const r2 = await window.stockStore.create({
            type: 'tx', name: item.name, category: item.category || '', price: item.price || 0, stock: 0, min_stock: 0, pin: '', role: '',
            item_id: item.__backendId, user_name: window.currentUser.name, tx_type: window.txType, qty: cartItem.qty,
            stock_before: stockBefore, stock_after: stockAfter, note: cartItem.note, timestamp: new Date().toISOString()
          });
          if (!r2.isOk) { errorCount++; continue; }
          successCount++;
        }

        window.txCart = [];
        window.txItemSearch = '';
        window.txCategoryFilter = 'all';
        window.showTxCartDetail = false;
        window.showMobileMenu = false; // Close menu
        lastRenderKey = '';

        if (successCount > 0) { showToast(`${successCount} item ${window.txType === 'IN' ? 'masuk' : 'keluar'} berhasil`); }
        if (errorCount > 0) { showToast(`${errorCount} item gagal diproses`, 'error'); }

        render();
    }

    // ==================== HISTORY ====================
    if (btn.id === 'btn-history-clear-date') {
      window.historyDateStart = '';
      window.historyDateEnd = '';
      render();
      return;
    }

    if (btn.dataset.txFilter) {
      window.txFilter = btn.dataset.txFilter;
      render();
      return;
    }

    // ==================== REPORTS ====================
    if (btn.id === 'btn-clear-date-filter') {
      window.reportDateStart = '';
      window.reportDateEnd = '';
      render();
      return;
    }

    if (btn.dataset.selectReportUser) {
      const userName = btn.dataset.selectReportUser;
      window.reportSelectedUser = { name: userName };
      render();
      return;
    }

    if (btn.id === 'btn-back-report') {
      window.reportSelectedUser = null;
      render();
      return;
    }

    // ==================== CONFIRM MODAL ====================
    if (btn.id === 'btn-confirm-no') {
      hideConfirm();
      return;
    }

    if (btn.id === 'btn-confirm-yes') {
      if (window.confirmCallback) window.confirmCallback();
      return;
    }

    // ==================== USERS ====================
    if (btn.dataset.delUser) {
      const user = getUsers().find(u => u.__backendId === btn.dataset.delUser);
      if (user) {
        showConfirm(`Hapus user "${user.name}"?`, async () => {
          hideConfirm();
          const r = await window.stockStore.delete(user);
          if (r.isOk) {
            if (window.currentUser?.__backendId === user.__backendId) {
              window.currentUser = null;
              window.currentView = 'dashboard';
            }
            showToast('User dihapus');
            lastRenderKey = '';
            render();
          } else showToast('Gagal menghapus', 'error');
        });
      }
      return;
    }

    if (btn.dataset.changePin) {
      const userId = btn.dataset.changePin;
      window.changePinModal = { show: true, userId, newPin: '', error: '' };
      render();
      return;
    }

    if (btn.id === 'btn-change-pin-cancel') {
      window.changePinModal = { show: false, userId: '', newPin: '', error: '' };
      render();
      return;
    }

    if (btn.id === 'btn-change-pin-submit') {
      const newPin = document.getElementById('new-pin-input')?.value || '';
      if (newPin.length !== 4 || !/^\d{4}$/.test(newPin)) { window.changePinModal.error = 'PIN harus 4 digit angka'; render(); return; }
      const user = getUsers().find(u => u.__backendId === window.changePinModal.userId);
      if (!user) { showToast('User tidak ditemukan', 'error'); return; }
      const updated = { ...user, pin: newPin };
      window.stockStore.update(updated).then(r => {
        if (r.isOk) { window.changePinModal = { show: false, userId: '', newPin: '', error: '' }; showToast('PIN berhasil diubah'); }
        else { window.changePinModal.error = 'Gagal mengubah PIN'; }
        render();
      });
      return;
    }
}

// ==================== FORM HANDLERS (bound once) ====================
function bindFormHandlers() {
  const formCU = document.getElementById('form-create-user');
  if (formCU && !formCU.dataset.bound) {
    formCU.dataset.bound = 'true';
    formCU.onsubmit = async (e) => {
      e.preventDefault();
      const name = document.getElementById('cu-name').value.trim();
      const pin = document.getElementById('cu-pin').value.trim();
      const roleEl = document.getElementById('cu-role');
      const role = roleEl ? roleEl.value : 'admin';
      if (!name || pin.length !== 4 || !/^\d{4}$/.test(pin)) { showToast('Nama & PIN 4 digit wajib diisi', 'error'); return; }
      const btn = formCU.querySelector('button[type=submit]');
      btn.disabled = true;
      const originalText = btn.textContent;
      btn.textContent = 'Menyimpan...';
      const r = await window.stockStore.create({ type: 'user', name, pin, role });
      if (r.isOk) { window.showCreateUser = false; window.showMobileMenu = false; showToast('User berhasil dibuat'); lastRenderKey = ''; render(); }
      else { showToast('Gagal membuat user', 'error'); btn.disabled = false; btn.textContent = originalText; }
    };
  }

  const formItem = document.getElementById('form-item');
  if (formItem && !formItem.dataset.bound) {
    formItem.dataset.bound = 'true';
    formItem.onsubmit = async (e) => {
      e.preventDefault();
      const name = document.getElementById('fi-name').value.trim();
      const category = document.getElementById('fi-cat')?.value.trim() || '';
      const price = parseInt(document.getElementById('fi-price').value) || 0;
      const tracks_stock = document.getElementById('fi-tracks-stock')?.checked !== false;
      const stock = tracks_stock ? (parseInt(document.getElementById('fi-stock').value) || 0) : 0;
      const min_stock = tracks_stock ? (parseInt(document.getElementById('fi-min').value) || 5) : 0;
      if (!name) { showToast('Nama barang wajib', 'error'); return; }
      const btn = document.getElementById('btn-save-item');
      btn.disabled = true;
      const originalText = btn.textContent;
      btn.textContent = 'Menyimpan...';
      if (window.editingItem) {
        const updated = { ...window.editingItem, name, category, price, tracks_stock, stock, min_stock };
        const r = await window.stockStore.update(updated);
        if (r.isOk) {
          const status = await window.stockStore.syncAfterWrite();
          window.showItemForm = false;
          window.editingItem = null;
          window.showMobileMenu = false;
          showSyncResult(status, 'Barang diperbarui dan tersinkron');
        }
        else { showToast('Gagal memperbarui', 'error'); btn.disabled = false; btn.textContent = originalText; }
      } else {
        const r = await window.stockStore.create({ type: 'item', name, category, price, tracks_stock, stock, min_stock });
        if (r.isOk) {
          const status = await window.stockStore.syncAfterWrite();
          window.showItemForm = false;
          window.showMobileMenu = false;
          showSyncResult(status, 'Barang ditambahkan dan tersinkron');
        }
        else { showToast('Gagal menambahkan', 'error'); btn.disabled = false; btn.textContent = originalText; }
      }
    };
  }
}

// ==================== INPUT HANDLERS (bound once) ====================
function bindInputHandlers() {
  const itemSearch = document.getElementById('item-search');
  if (itemSearch && !itemSearch.dataset.bound) {
    itemSearch.dataset.bound = 'true';
    let searchTimer = null;
    itemSearch.oninput = (e) => {
      clearTimeout(searchTimer);
      window.searchTerm = e.target.value;
      window.pendingFocus = { id: 'item-search', cursor: e.target.selectionStart ?? e.target.value.length };
      searchTimer = setTimeout(() => { render(); }, 300);
    };
  }

  const catFilter = document.getElementById('cat-filter');
  if (catFilter && !catFilter.dataset.bound) {
    catFilter.dataset.bound = 'true';
    catFilter.onchange = (e) => { window.categoryFilter = e.target.value; render(); };
  }

  const txSearchInput = document.getElementById('tx-search');
  if (txSearchInput && !txSearchInput.dataset.bound) {
    txSearchInput.dataset.bound = 'true';
    let searchTimer = null;
    txSearchInput.oninput = (e) => {
      clearTimeout(searchTimer);
      window.txItemSearch = e.target.value;
      window.pendingFocus = { id: 'tx-search', cursor: e.target.selectionStart ?? e.target.value.length };
      searchTimer = setTimeout(() => { render(); }, 300);
    };
  }

  const txCatFilterSelect = document.getElementById('tx-cat-filter');
  if (txCatFilterSelect && !txCatFilterSelect.dataset.bound) {
    txCatFilterSelect.dataset.bound = 'true';
    txCatFilterSelect.onchange = (e) => { window.txCategoryFilter = e.target.value; render(); };
  }

  const histSearch = document.getElementById('history-search');
  if (histSearch && !histSearch.dataset.bound) {
    histSearch.dataset.bound = 'true';
    let searchTimer = null;
    histSearch.oninput = (e) => {
      clearTimeout(searchTimer);
      window.searchTerm = e.target.value;
      window.pendingFocus = { id: 'history-search', cursor: e.target.selectionStart ?? e.target.value.length };
      searchTimer = setTimeout(() => { render(); }, 300);
    };
  }

  const histDateStart = document.getElementById('history-date-start');
  if (histDateStart && !histDateStart.dataset.bound) {
    histDateStart.dataset.bound = 'true';
    histDateStart.onchange = (e) => { window.historyDateStart = e.target.value; render(); };
  }

  const histDateEnd = document.getElementById('history-date-end');
  if (histDateEnd && !histDateEnd.dataset.bound) {
    histDateEnd.dataset.bound = 'true';
    histDateEnd.onchange = (e) => { window.historyDateEnd = e.target.value; render(); };
  }

  const reportDateStart = document.getElementById('report-date-start');
  if (reportDateStart && !reportDateStart.dataset.bound) {
    reportDateStart.dataset.bound = 'true';
    reportDateStart.onchange = (e) => { window.reportDateStart = e.target.value; render(); };
  }

  const reportDateEnd = document.getElementById('report-date-end');
  if (reportDateEnd && !reportDateEnd.dataset.bound) {
    reportDateEnd.dataset.bound = 'true';
    reportDateEnd.onchange = (e) => { window.reportDateEnd = e.target.value; render(); };
  }

  const pinInput = document.getElementById('pin-input');
  if (pinInput && !pinInput.dataset.bound) {
    pinInput.dataset.bound = 'true';
    pinInput.onkeydown = (e) => { if (e.key === 'Enter' && window.pinModal.cb) { window.pinModal.cb(pinInput.value); } };
  }

  const itemCategoryInput = document.getElementById('fi-cat');
  if (itemCategoryInput && !itemCategoryInput.dataset.bound) {
    itemCategoryInput.dataset.bound = 'true';
    itemCategoryInput.onfocus = () => {
      document.getElementById('cat-options')?.classList.remove('hidden');
    };
    itemCategoryInput.oninput = (e) => {
      const term = e.target.value.toLowerCase();
      const options = document.getElementById('cat-options');
      if (!options) return;
      options.classList.remove('hidden');
      options.querySelectorAll('[data-cat-option]').forEach(option => {
        const value = option.dataset.catOption.toLowerCase();
        option.classList.toggle('hidden', Boolean(term) && !value.includes(term));
      });
    };
  }

  const tracksStockInput = document.getElementById('fi-tracks-stock');
  if (tracksStockInput && !tracksStockInput.dataset.bound) {
    tracksStockInput.dataset.bound = 'true';
    tracksStockInput.onchange = (e) => {
      const enabled = e.target.checked;
      document.getElementById('stock-fields')?.classList.toggle('opacity-50', !enabled);
      ['fi-stock', 'fi-min'].forEach(id => {
        const input = document.getElementById(id);
        if (!input) return;
        input.disabled = !enabled;
        input.required = enabled;
        if (!enabled) input.value = '0';
        if (enabled && id === 'fi-min' && input.value === '0') input.value = '5';
      });
    };
  }
}

// ── Storage Init ──
async function initializeApp() {
  if (appInitialized) return;
  appInitialized = true;

  console.log('[APP] Initializing stock store first...');
  
  // Initialize IndexedDB storage FIRST before any render
  try {
    const result = await window.stockStore.init(dataHandler);
    if (!result.isOk) {
      console.error('[APP] Failed to init stock store:', result.error);
      // Still render even if SDK fails
      lastRenderKey = '';
      render();
      return;
    }
    console.log('[APP] Stock store initialized successfully');
  } catch(e) {
    console.error('[APP] Stock store initialization error:', e);
    // Still render even if SDK fails
    lastRenderKey = '';
    render();
    return;
  }
  
  // Now render UI after SDK is ready
  console.log('[APP] Rendering UI...');
  lastRenderKey = '';
  render();
}

window.addEventListener('popstate', (event) => {
  if (!window.currentUser) return;
  const requestedView = event.state?.stockAppView || 'dashboard';
  window.currentView = isValidViewForCurrentUser(requestedView) ? requestedView : 'dashboard';
  window.searchTerm = '';
  window.txFilter = 'all';
  window.categoryFilter = 'all';
  window.showMobileMenu = false;
  lastRenderKey = '';
  render();
});

// Initialize Element SDK if available
if (window.elementSdk) {
  console.log('[APP] Initializing Element SDK...');
  try {
    window.elementSdk.init({
      defaultConfig,
      onConfigChange: async (config) => {
        const bg = config.background_color || defaultConfig.background_color;
        const surface = config.surface_color || defaultConfig.surface_color;
        const text = config.text_color || defaultConfig.text_color;
        const primary = config.primary_color || defaultConfig.primary_color;
        const secondary = config.secondary_color || defaultConfig.secondary_color;
        const font = config.font_family || 'Plus Jakarta Sans';
        const fontSize = config.font_size || 16;

        document.documentElement.style.fontFamily = `${font}, Plus Jakarta Sans, sans-serif`;

        const loginTitle = document.getElementById('login-title');
        if (loginTitle) loginTitle.textContent = config.app_title || defaultConfig.app_title;
        const loginCompany = document.getElementById('login-company');
        if (loginCompany) loginCompany.textContent = 'Management Stock';
        const mainTitle = document.getElementById('main-title');
        if (mainTitle) mainTitle.textContent = config.app_title || defaultConfig.app_title;

        document.body.style.backgroundColor = bg;
        document.querySelectorAll('.bg-white').forEach(el => el.style.backgroundColor = surface);
        document.querySelectorAll('h2, .font-700, .font-800').forEach(el => { if(!el.classList.contains('text-white')&&!el.classList.contains('text-red-600')&&!el.classList.contains('text-emerald-600')&&!el.classList.contains('text-amber-600')) el.style.color = text; });
        document.querySelectorAll('.bg-red-600').forEach(el => { el.style.backgroundColor = primary; });
        document.querySelectorAll('.text-red-600').forEach(el => { if(!el.closest('.bg-red-600')) el.style.color = primary; });
        document.querySelectorAll('.text-gray-500, .text-gray-400').forEach(el => el.style.color = secondary);

        document.body.style.fontSize = `${fontSize}px`;
      },
      mapToCapabilities: (config) => ({
        recolorables: [
          { get:()=>config.background_color||defaultConfig.background_color, set:(v)=>{config.background_color=v;window.elementSdk.setConfig({background_color:v});} },
          { get:()=>config.surface_color||defaultConfig.surface_color, set:(v)=>{config.surface_color=v;window.elementSdk.setConfig({surface_color:v});} },
          { get:()=>config.text_color||defaultConfig.text_color, set:(v)=>{config.text_color=v;window.elementSdk.setConfig({text_color:v});} },
          { get:()=>config.primary_color||defaultConfig.primary_color, set:(v)=>{config.primary_color=v;window.elementSdk.setConfig({primary_color:v});} },
          { get:()=>config.secondary_color||defaultConfig.secondary_color, set:(v)=>{config.secondary_color=v;window.elementSdk.setConfig({secondary_color:v});} },
        ],
        borderables: [],
        fontEditable: { get:()=>config.font_family||'Plus Jakarta Sans', set:(v)=>{config.font_family=v;window.elementSdk.setConfig({font_family:v});} },
        fontSizeable: { get:()=>config.font_size||16, set:(v)=>{config.font_size=v;window.elementSdk.setConfig({font_size:v});} },
      }),
      mapToEditPanelValues: (config) => new Map([
        ['app_title', config.app_title || defaultConfig.app_title],
        ['company_name', config.company_name || defaultConfig.company_name],
      ])
    });
    console.log('[APP] Element SDK initialized');
  } catch(e) {
    console.error('[APP] Element SDK error:', e);
  }
}

// Start app
console.log('[APP] Starting initialization...');
initializeApp();

