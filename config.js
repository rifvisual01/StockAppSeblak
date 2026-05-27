// Konfigurasi aplikasi.
// App tetap bisa jalan offline-only walau Supabase key belum diisi.
// Simpan hanya public/anon key di frontend. Service role tidak boleh dipakai di browser.
window.STOCK_APP_CONFIG = {
  supabaseUrl: "https://sydttmngaithmbdxqlwh.supabase.co",
  supabasePublishableKey: "sb_publishable_BGVB-bqBPbAPRW9lypWqHg_QvFOPtIJ",
  supabaseAnonKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJIUzI1NiIsInJlZiI6InN5ZHR0bW5nYWl0aG1iZHhxbHdoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzk3NzQyNzUsImV4cCI6MjA5NTM1MDI3NX0.pAcaYF4j-9UWT8LcK_8ocNnLXZsLiEiw4vufmSHRVJE",
  supabaseTables: {
    users: "users",
    items: "items",
    stockLogs: "stock_logs"
  }
};
