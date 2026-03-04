<img width=100% src="https://capsule-render.vercel.app/api?type=waving&color=00bfbf&height=120&section=header"/>

# SICONFI Data Collector

Ferramenta em **Python** para coleta automatizada de dados fiscais municipais diretamente do **Data Lake do Tesouro Nacional (SICONFI)**.

O projeto permite baixar dados em **larga escala**, utilizando **execução paralela**, e salvar os resultados em **arquivos CSV organizados por ente, ano e mês**, ou em um **arquivo consolidado**.

A aplicação possui uma **interface gráfica simples (Tkinter)** que facilita o uso por usuários não técnicos.

---

## Objetivo do Projeto

Este projeto foi desenvolvido para **automatizar a coleta de dados fiscais públicos** disponibilizados pelo **Tesouro Nacional** por meio do Data Lake do SICONFI.

A ferramenta resolve três problemas comuns:

- Download massivo de dados fiscais municipais
- Execução paralela para acelerar a coleta
- Organização automática dos dados em arquivos estruturados

A aplicação pode ser útil para:

- pesquisadores
- auditores
- analistas de finanças públicas
- órgãos de controle
- estudos fiscais municipais

---

## Fonte dos Dados

Os dados são obtidos diretamente da **API pública do Data Lake do Tesouro Nacional**.

https://apidatalake.tesouro.gov.br/ords/siconfi/tt

---

## Dados Disponíveis

O sistema permite coletar três tipos de dados.

### MSC Orçamentária

Dados mensais da **Matriz de Saldos Contábeis**.

Permite selecionar:

- municípios
- anos
- meses

Endpoint utilizado:
  - `/msc_orcamentaria`


---

### DCA (Declaração de Contas Anuais)

Dados anuais de prestação de contas dos entes federativos.

Permite selecionar:

- municípios
- anos
- anexos específicos

Endpoint utilizado:
  - `/dca`


---

### Entes

Lista completa de entes federativos cadastrados no SICONFI.

Endpoint utilizado:
  - `/entes`


---


# Arquitetura do Projeto

O projeto está organizado em módulos com responsabilidades específicas.

```
.
├── main.py
├── gui.py
├── worker.py
├── api.py
├── governo.py
└── utils.py
```

## main.py

Ponto de entrada da aplicação.

Inicializa a interface gráfica e executa o loop principal do Tkinter.

---

## gui.py

Implementa a **interface gráfica** e o controle da execução.

Responsável por:

- capturar parâmetros do usuário
- validar entradas
- exibir logs
- mostrar progresso da execução
- iniciar e parar o processamento

## Interface da aplicação

<p align="center">
  <img src="src/Captura de tela 2025-09-16 094935.png" width="800">
</p>

---

## worker.py

Motor de execução da aplicação.

Responsável por:

- execução em segundo plano
- paralelização com `ThreadPoolExecutor`
- coordenação das tarefas de download
- escrita dos arquivos CSV
- consolidação opcional dos dados

---

## api.py

Cliente HTTP para comunicação com o Data Lake.

Implementa:

- sessão HTTP persistente
- retry automático
- tratamento de erros
- paginação automática da API

---

## governo.py

Camada de abstração dos endpoints do SICONFI.

Implementa métodos para:

- consulta de entes
- consulta DCA
- consulta MSC Orçamentária

---

## utils.py

Funções auxiliares:

- leitura de CSV de municípios
- criação de diretórios
- padronização de nomes de arquivos
- parsing de parâmetros

---

<details>
<summary><b>Fluxo do Projeto e Tecnologias Utilizadas</b></summary>

# Fluxo de Execução

1. O usuário configura os parâmetros na interface gráfica.
2. A aplicação cria um **Worker Thread**.
3. O Worker cria um **ThreadPoolExecutor**.
4. Cada tarefa consulta a API do SICONFI.
5. Os dados retornados são convertidos em **DataFrame (Pandas)**.
6. Os dados são salvos em **arquivos CSV**.


---

# Tecnologias Utilizadas

- Python 3
- Tkinter
- Pandas
- Requests
- ThreadPoolExecutor
- API pública do Tesouro Nacional (SICONFI)

 <img width=100% src="https://capsule-render.vercel.app/api?type=waving&color=00bfbf&height=120&section=footer"/>
