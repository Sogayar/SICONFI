import os
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from worker import Worker
from utils import garantir_dir, parse_anos


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SICONFI Coletor (MSC/DCA)")
        self.geometry("980x700")
        self.resizable(True, True)

        self.log_queue = queue.Queue()
        self.job = None

        # ---- Lista fixa de anexos DCA (pré-carregada) ----
        self.DCA_ANEXOS = [
            "DCA-Anexo I-AB",
            "DCA-Anexo I-C",
            "DCA-Anexo I-D",
            "DCA-Anexo I-E",
            "DCA-Anexo I-F",
            "DCA-Anexo I-G",
            "DCA-Anexo I-HI",
        ]

        # Vars de controle
        self.modo = tk.StringVar(value="MSC")  # MSC | DCA | ENTES
        self.csv_path = tk.StringVar()
        self.out_dir = tk.StringVar(value=os.path.abspath("saida"))
        self.anos = tk.StringVar(value="2022,2023,2024")

        # MSC: meses
        self.month_vars = [tk.BooleanVar(value=False) for _ in range(12)]
        self.month_vars[11].set(True)  # Dezembro padrão

        # Parâmetros gerais
        self.threads = tk.IntVar(value=8)
        self.timeout = tk.IntVar(value=15)
        self.overwrite = tk.BooleanVar(value=False)
        self.consolidar = tk.BooleanVar(value=True)
        self.keep_partials = tk.BooleanVar(value=True)

        # DCA
        self.anexos_manual = tk.BooleanVar(value=False)
        self.anexos = tk.StringVar(value="")  # será preenchido pela UI

        self._build_ui()
        self.after(100, self._poll_log)

        # Fechamento seguro
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def selected_months(self):
        return [i + 1 for i, var in enumerate(self.month_vars) if var.get()]

    # ---------- UI ----------
    def _build_ui(self):
        frm_top = ttk.Frame(self)
        frm_top.pack(fill="x", padx=10, pady=10)
        ttk.Label(frm_top, text="Modo:").pack(side="left")
        ttk.Radiobutton(frm_top, text="MSC Orçamentária", variable=self.modo, value="MSC").pack(side="left", padx=5)
        ttk.Radiobutton(frm_top, text="DCA", variable=self.modo, value="DCA").pack(side="left", padx=5)
        ttk.Radiobutton(frm_top, text="Entes", variable=self.modo, value="ENTES").pack(side="left", padx=5)

        frm_csv = ttk.Frame(self)
        frm_csv.pack(fill="x", padx=10, pady=5)
        ttk.Label(frm_csv, text="CSV de cidades (ente,cod_ibge):").pack(side="left")
        ent_csv = ttk.Entry(frm_csv, textvariable=self.csv_path)
        ent_csv.pack(side="left", fill="x", expand=True, padx=6)
        ttk.Button(frm_csv, text="Procurar...", command=self._pick_csv).pack(side="left")

        frm_out = ttk.Frame(self)
        frm_out.pack(fill="x", padx=10, pady=5)
        ttk.Label(frm_out, text="Pasta de saída:").pack(side="left")
        ent_out = ttk.Entry(frm_out, textvariable=self.out_dir)
        ent_out.pack(side="left", fill="x", expand=True, padx=6)
        ttk.Button(frm_out, text="Procurar...", command=self._pick_dir).pack(side="left")

        frm_params = ttk.LabelFrame(self, text="Parâmetros")
        frm_params.pack(fill="x", padx=10, pady=10)

        ttk.Label(frm_params, text="Anos (ex: 2022,2023,2024):").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(frm_params, textvariable=self.anos, width=28).grid(row=0, column=1, sticky="w", padx=6, pady=4)

        ttk.Label(frm_params, text="Trabalhadores (threads):").grid(row=0, column=2, sticky="w", padx=6, pady=4)
        tk.Spinbox(frm_params, from_=1, to=64, textvariable=self.threads, width=6).grid(row=0, column=3, sticky="w", padx=6, pady=4)

        ttk.Label(frm_params, text="Timeout (s):").grid(row=0, column=4, sticky="w", padx=6, pady=4)
        tk.Spinbox(frm_params, from_=5, to=120, textvariable=self.timeout, width=6).grid(row=0, column=5, sticky="w", padx=6, pady=4)

        ttk.Checkbutton(frm_params, text="Sobrescrever arquivos existentes", variable=self.overwrite).grid(
            row=1, column=0, columnspan=3, sticky="w", padx=6, pady=4)
        ttk.Checkbutton(frm_params, text="Gerar consolidado", variable=self.consolidar, command=self._toggle_keep).grid(
            row=1, column=3, sticky="w", padx=6, pady=4)
        self.chk_keep = ttk.Checkbutton(frm_params, text="Manter parciais (ao consolidar)",
                                        variable=self.keep_partials)
        self.chk_keep.grid(row=1, column=4, columnspan=2, sticky="w", padx=6, pady=4)
        self._toggle_keep()

        frm_months = ttk.LabelFrame(self, text="Meses (MSC)")
        frm_months.pack(fill="x", padx=10, pady=5)
        nomes = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
        for i, nome in enumerate(nomes):
            r, c = divmod(i, 6)
            ttk.Checkbutton(frm_months, text=nome, variable=self.month_vars[i]).grid(row=r, column=c, sticky="w", padx=6, pady=3)

        # ---- DCA – Anexos com seleção pronta ----
        frm_dca = ttk.LabelFrame(self, text="DCA – Anexos")
        frm_dca.pack(fill="x", padx=10, pady=5)

        ttk.Checkbutton(
            frm_dca,
            text="Especificar anexos",
            variable=self.anexos_manual,
            command=self._toggle_anexos
        ).grid(row=0, column=0, sticky="w", padx=6, pady=4)

        ttk.Label(frm_dca, text="Selecionar anexos ▾").grid(row=0, column=1, sticky="w", padx=6, pady=4)
        self.cbo_anexo = ttk.Combobox(frm_dca, state="disabled", width=28, values=self.DCA_ANEXOS)
        self.cbo_anexo.grid(row=0, column=2, sticky="w", padx=6, pady=4)

        self.btn_add_anexo = ttk.Button(frm_dca, text="Adicionar", state="disabled", command=self._add_anexo_from_combo)
        self.btn_add_anexo.grid(row=0, column=3, sticky="w", padx=4)

        self.btn_clear_anexo = ttk.Button(frm_dca, text="Limpar", state="disabled", command=self._clear_anexos)
        self.btn_clear_anexo.grid(row=0, column=4, sticky="w", padx=4)

        ttk.Label(frm_dca, text="Escolhidos:").grid(row=1, column=0, sticky="e", padx=6, pady=4)
        self.anexos_entry = ttk.Entry(frm_dca, textvariable=self.anexos, width=60, state="disabled")
        self.anexos_entry.grid(row=1, column=1, columnspan=4, sticky="w", padx=6, pady=4)

        frm_actions = ttk.Frame(self)
        frm_actions.pack(fill="x", padx=10, pady=10)
        self.btn_start = ttk.Button(frm_actions, text="▶️ Iniciar", command=self._start_job)
        self.btn_start.pack(side="left")
        self.btn_stop = ttk.Button(frm_actions, text="⏹️ Parar", command=self._stop_job, state="disabled")
        self.btn_stop.pack(side="left", padx=6)

        frm_prog = ttk.Frame(self)
        frm_prog.pack(fill="x", padx=10, pady=5)
        self.prog = ttk.Progressbar(frm_prog, length=420, mode="determinate")
        self.prog.pack(side="left", padx=6)
        self.lbl_prog = ttk.Label(frm_prog, text="0%")
        self.lbl_prog.pack(side="left")

        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=True, padx=10, pady=10)
        self.txt = tk.Text(frm_log, height=18, wrap="word")
        self.txt.pack(fill="both", expand=True)
        self.txt.configure(state="disabled")

    # ----- helpers DCA anexos -----
    def _toggle_keep(self):
        state = "normal" if self.consolidar.get() else "disabled"
        self.chk_keep.configure(state=state)

    def _toggle_anexos(self):
        enabled = self.anexos_manual.get()
        state = "normal" if enabled else "disabled"
        self.anexos_entry.config(state=state)
        self.cbo_anexo.config(state=state)
        self.btn_add_anexo.config(state=state)
        self.btn_clear_anexo.config(state=state)

    def _append_anexo(self, value: str):
        value = (value or "").strip()
        if not value:
            return
        itens = [a.strip() for a in self.anexos.get().split(",") if a.strip()]
        if value not in itens:
            itens.append(value)
            self.anexos.set(", ".join(itens))

    def _add_anexo_from_combo(self):
        self._append_anexo(self.cbo_anexo.get())

    def _clear_anexos(self):
        self.anexos.set("")

    # ----- resto da UI -----
    def _pick_csv(self):
        path = filedialog.askopenfilename(title="Selecione CSV de cidades",
                                          filetypes=[("CSV", "*.csv"), ("Todos", "*.*")])
        if path:
            self.csv_path.set(path)

    def _pick_dir(self):
        path = filedialog.askdirectory(title="Selecione a pasta de saída")
        if path:
            self.out_dir.set(path)

    def set_progress(self, val):
        val = max(0, min(100, int(val)))
        self.prog["value"] = val
        self.lbl_prog.config(text=f"{val}%")
        self.update_idletasks()

    def log(self, msg):
        self.txt.configure(state="normal")
        self.txt.insert("end", msg + "\n")
        self.txt.see("end")
        self.txt.configure(state="disabled")

    def _poll_log(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log(msg)
        except queue.Empty:
            pass
        self.after(100, self._poll_log)

    def _validate_inputs(self):
        if self.modo.get() in ("MSC", "DCA"):
            if not self.csv_path.get():
                raise ValueError("Selecione o CSV de cidades.")
            if not os.path.exists(self.csv_path.get()):
                raise ValueError("CSV de cidades não encontrado.")
        if not self.out_dir.get():
            raise ValueError("Selecione a pasta de saída.")
        garantir_dir(self.out_dir.get())
        _ = parse_anos(self.anos.get())
        if self.modo.get() == "MSC" and not self.selected_months():
            raise ValueError("Selecione ao menos um mês em 'Meses (MSC)'.")
        if int(self.threads.get()) < 1:
            self.threads.set(1)
        if int(self.timeout.get()) < 1:
            self.timeout.set(15)
        # Se anexos manuais estiver ligado e vazio, sugira pelo menos 1
        if self.modo.get() == "DCA" and self.anexos_manual.get():
            if not [a.strip() for a in self.anexos.get().split(",") if a.strip()]:
                raise ValueError("Selecione ao menos um anexo em 'DCA – Anexos'.")

    def _start_job(self):
        try:
            self._validate_inputs()
        except Exception as e:
            messagebox.showerror("Validação", str(e))
            return
        self.set_progress(0)
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.txt.configure(state="normal")
        self.txt.delete("1.0", "end")
        self.txt.configure(state="disabled")

        self.job = Worker(self)
        self.job.start()

    def _stop_job(self):
        if self.job:
            self.job.stop()
            self.log_queue.put("[INFO] Cancelando tarefas pendentes…")

    def _on_close(self):
        try:
            if self.job and self.job.is_alive():
                self.job.stop()
        except Exception:
            pass
        self.after(100, self.destroy)

    def on_job_finished(self):
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.set_progress(100)
