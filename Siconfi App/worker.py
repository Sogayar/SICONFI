import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import pandas as pd

import api
from governo import Governo
from utils import garantir_dir, garantir_pasta_arquivo, read_cidades_csv, slugify, parse_anos


class Worker(threading.Thread):
    def __init__(self, ui):
        super().__init__(daemon=True)
        self.ui = ui
        self.stop_event = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._futs: list[Future] = []
        self._start_ts: float | None = None

    def stop(self):
        """Sinaliza cancelamento e encerra o pool cancelando futures pendentes."""
        self.stop_event.set()
        ex = self._executor
        if ex is not None:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    def log(self, msg):
        self.ui.log_queue.put(msg)

    def _set_progress(self, done, total):
        pct = int(done / total * 100) if total > 0 else 0
        self.ui.set_progress(pct)

    def _eta_tick(self, done, total):
        if not self._start_ts or done <= 0:
            return
        elapsed = time.time() - self._start_ts
        rate = elapsed / done
        remain = rate * max(0, total - done)
        if hasattr(self.ui, "set_eta"):
            try:
                self.ui.set_eta(remain)
            except Exception:
                pass

    def run(self):
        try:
            if hasattr(self.ui, "timeout") and hasattr(self.ui.timeout, "get"):
                try:
                    api.set_timeout(int(self.ui.timeout.get()))
                except Exception:
                    pass

            self._start_ts = time.time()
            modo = self.ui.modo.get()
            if modo == "MSC":
                self._run_msc()
            elif modo == "DCA":
                self._run_dca()
            else:
                self._run_entes()
        except Exception as e:
            self.log(f"[ERRO] {e}")
        finally:
            # encerra seguro
            if self._executor is not None:
                try:
                    self._executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                self._executor = None
            self._futs.clear()
            self.ui.on_job_finished()

    # ----------------- ENTES -----------------
    def _run_entes(self):
        out_dir = self.ui.out_dir.get().strip() or "saida"
        garantir_dir(out_dir)
        self.log("Baixando entes...")
        itens = Governo.Anexo()
        path = os.path.join(out_dir, "entes.csv")
        pd.DataFrame(itens).to_csv(path, index=False, encoding="utf-8-sig")
        self.log(f"[OK] Entes salvos em: {path}")
        self.ui.set_progress(100)

    # ----------------- MSC -------------------
    def _run_msc(self):
        df = read_cidades_csv(self.ui.csv_path.get())
        anos = parse_anos(self.ui.anos.get())
        meses = self.ui.selected_months()
        if not meses:
            raise ValueError("Selecione ao menos um mês para MSC.")
        out_dir = self.ui.out_dir.get().strip() or "saida"
        garantir_dir(out_dir)

        overwrite = bool(self.ui.overwrite.get())
        consolidar = bool(self.ui.consolidar.get())
        keep_partials = bool(getattr(self.ui, "keep_partials", None).get() if hasattr(self.ui, "keep_partials") else True)
        max_workers = max(1, int(self.ui.threads.get()))

        total = len(df) * len(anos) * len(meses)
        done = 0
        all_rows = []
        generated_paths = []

        def msc_task(row, ano, mes):
            if self.stop_event.is_set():
                return {"status": "cancel", "msg": "[CANCELADO]"}
            nome = str(row["ente"])
            id_ente = int(row["cod_ibge"])
            fname = f"{slugify(nome)}_{ano}_mes{mes}_msc.csv"
            path = os.path.join(out_dir, fname)

            if os.path.exists(path) and not overwrite:
                return {"status": "skip", "msg": f"[PULAR] já existe: {path}", "path": path}

            try:
                dados = Governo.MSCOrcamentaria(id_ente, ano, mes)
                if self.stop_event.is_set():
                    return {"status": "cancel", "msg": "[CANCELADO]"}
                df_city = pd.DataFrame(dados)
                garantir_pasta_arquivo(path)
                df_city.to_csv(path, index=False, encoding="utf-8-sig")
                res = {
                    "status": "ok",
                    "msg": f"[OK] {nome} ({ano}/{mes:02d}) -> {os.path.basename(path)} [{len(df_city)} linhas]",
                    "path": path,
                }
                if consolidar and not df_city.empty:
                    res["df"] = df_city.assign(_ente=nome, _ano=ano, _mes=mes)
                return res
            except Exception as e:
                return {"status": "err", "msg": f"[ERRO] {nome} ({ano}/{mes:02d}): {e}"}

        self._futs = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            self._executor = ex
            for ano in anos:
                if self.stop_event.is_set():
                    break
                self.log(f"=== MSC {ano} ===")
                for _, row in df.iterrows():
                    if self.stop_event.is_set():
                        break
                    for mes in meses:
                        if self.stop_event.is_set():
                            break
                        self._futs.append(ex.submit(msc_task, row, ano, mes))

            for fut in as_completed(self._futs):
                if self.stop_event.is_set():
                    break
                if fut.cancelled():
                    continue
                try:
                    res = fut.result()
                except Exception as e:
                    self.log(f"[ERRO-TAREFA] {e}")
                    done += 1
                    self._set_progress(done, total)
                    self._eta_tick(done, total)
                    continue

                if res["status"] != "cancel":
                    self.log(res["msg"])
                if "df" in res:
                    all_rows.append(res["df"])
                if res.get("path"):
                    generated_paths.append(res["path"])

                done += 1
                self._set_progress(done, total)
                self._eta_tick(done, total)

        # Consolidado + limpeza
        if consolidar and all_rows:
            dfc = pd.concat(all_rows, ignore_index=True)
            pathc = os.path.join(out_dir, "msc_consolidado.csv")
            dfc.to_csv(pathc, index=False, encoding="utf-8-sig")
            self.log(f"[OK] Consolidado: {pathc}")
            if not keep_partials:
                for p in generated_paths:
                    try:
                        os.remove(p)
                        self.log(f"[DEL] {p}")
                    except Exception as e:
                        self.log(f"[ERRO-DEL] {p}: {e}")

    def _run_dca(self):
        df = read_cidades_csv(self.ui.csv_path.get())
        anos = parse_anos(self.ui.anos.get())
        out_dir = self.ui.out_dir.get().strip() or "saida"
        garantir_dir(out_dir)

        overwrite = bool(self.ui.overwrite.get())
        consolidar = bool(self.ui.consolidar.get())
        keep_partials = bool(getattr(self.ui, "keep_partials", None).get() if hasattr(self.ui, "keep_partials") else True)
        anexos_manual = bool(self.ui.anexos_manual.get())
        anexos_text = self.ui.anexos.get().strip()
        max_workers = max(1, int(self.ui.threads.get()))

        total = len(df) * len(anos)
        done = 0
        all_rows = []
        generated_paths = []

        def dca_task(row, ano):
            if self.stop_event.is_set():
                return {"logs": ["[CANCELADO]"], "dfs": [], "paths": []}
            nome = str(row["ente"])
            id_ente = int(row["cod_ibge"])
            logs = []
            dfs = []
            paths = []

            if anexos_manual and anexos_text:
                anexos = [a.strip() for a in anexos_text.split(",") if a.strip()]
                itens_gerais = None
            else:
                try:
                    itens_gerais = Governo.DCA(ano, id_ente=id_ente, no_anexo=None)
                    anexos = sorted({i.get("no_anexo") for i in itens_gerais if i.get("no_anexo")})
                except Exception as e:
                    logs.append(f"[ERRO] Falha ao listar anexos de {nome} ({ano}): {e}")
                    anexos = []
                    itens_gerais = None

            if not anexos:
                fname = f"{slugify(nome)}_{ano}_dca.csv"
                path = os.path.join(out_dir, fname)
                if os.path.exists(path) and not overwrite:
                    logs.append(f"[PULAR] já existe: {path}")
                else:
                    df_city = pd.DataFrame(itens_gerais or [])
                    garantir_pasta_arquivo(path)
                    df_city.to_csv(path, index=False, encoding="utf-8-sig")
                    paths.append(path)
                    if consolidar and not df_city.empty:
                        dfs.append(df_city.assign(_ente=nome, _ano=ano))
                    logs.append(f"[OK] {nome} ({ano}) -> {os.path.basename(path)} [{len(df_city)} linhas]")
            else:
                for anexo in anexos:
                    if self.stop_event.is_set():
                        break
                    try:
                        dados = Governo.DCA(ano, id_ente=id_ente, no_anexo=anexo)
                        df_city = pd.DataFrame(dados)
                        fname = f"{slugify(nome)}_{ano}_dca_{slugify(anexo)}.csv"
                        path = os.path.join(out_dir, fname)
                        if os.path.exists(path) and not overwrite:
                            logs.append(f"[PULAR] já existe: {path}")
                        else:
                            garantir_pasta_arquivo(path)
                            df_city.to_csv(path, index=False, encoding="utf-8-sig")
                            paths.append(path)
                            if consolidar and not df_city.empty:
                                dfs.append(df_city.assign(_ente=nome, _ano=ano, _anexo=anexo))
                            logs.append(f"[OK] {nome} ({ano}) [{anexo}] -> {os.path.basename(path)} [{len(df_city)} linhas]")
                    except Exception as e:
                        logs.append(f"[ERRO] {nome} ({ano}) [{anexo}]: {e}")

            return {"logs": logs, "dfs": dfs, "paths": paths}

        self._futs = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            self._executor = ex
            for ano in anos:
                if self.stop_event.is_set():
                    break
                self.log(f"=== DCA {ano} ===")
                for _, row in df.iterrows():
                    if self.stop_event.is_set():
                        break
                    self._futs.append(ex.submit(dca_task, row, ano))

            for fut in as_completed(self._futs):
                if self.stop_event.is_set():
                    break
                if fut.cancelled():
                    continue
                try:
                    res = fut.result()
                except Exception as e:
                    self.log(f"[ERRO-TAREFA] {e}")
                    done += 1
                    self._set_progress(done, total)
                    self._eta_tick(done, total)
                    continue

                for m in res["logs"]:
                    self.log(m)
                if res["dfs"]:
                    all_rows.extend(res["dfs"])
                if res["paths"]:
                    generated_paths.extend(res["paths"])

                done += 1
                self._set_progress(done, total)
                self._eta_tick(done, total)

        if consolidar and all_rows:
            dfc = pd.concat(all_rows, ignore_index=True)
            pathc = os.path.join(out_dir, "dca_consolidado.csv")
            dfc.to_csv(pathc, index=False, encoding="utf-8-sig")
            self.log(f"[OK] Consolidado: {pathc}")
            if not keep_partials:
                for p in generated_paths:
                    try:
                        os.remove(p)
                        self.log(f"[DEL] {p}")
                    except Exception as e:
                        self.log(f"[ERRO-DEL] {p}: {e}")