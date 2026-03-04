from api import API_BASE, fetch_all_pages

class Governo:
    @staticmethod
    def Anexo():
        return fetch_all_pages(f"{API_BASE}/entes", {})

    @staticmethod
    def DCA(an_exercicio, no_anexo=None, id_ente=None):
        if id_ente is None and no_anexo is not None and str(no_anexo).isdigit():
            id_ente = int(no_anexo)
            no_anexo = None
        base = f"{API_BASE}/dca"
        params = {"an_exercicio": int(an_exercicio)}
        if id_ente is not None:
            params["id_ente"] = int(id_ente)
        if no_anexo:
            params["no_anexo"] = str(no_anexo)
        return fetch_all_pages(base, params)

    @staticmethod
    def MSCOrcamentaria(id_ente, an_referencia, mes_referencia):
        id_tv = ["beginning_balance", "ending_balance", "period_change"]
        classe_conta = [6]
        co_tipo_matriz = ["MSCC"]

        ResultMscOrc = []
        for matriz in co_tipo_matriz:
            for tv in id_tv:
                for classe in classe_conta:
                    url = f"{API_BASE}/msc_orcamentaria"
                    params = {
                        "id_ente": int(id_ente),
                        "an_referencia": int(an_referencia),
                        "me_referencia": int(mes_referencia), 
                        "co_tipo_matriz": matriz,
                        "id_tv": tv,
                        "classe_conta": int(classe),
                    }
                    ResultMscOrc.extend(fetch_all_pages(url, params))
        return ResultMscOrc
