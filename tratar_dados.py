# tratar_dados_fix_delim_loop_caseinsensitive.py
import pandas as pd
from pathlib import Path
import os
import csv
import traceback

DEFAULT_MISSING = {"99999", "9999", "X", "x", ".", "*", "-", ""}

ESSENCIAIS = [
    "NU_ANO",
    "SG_UF_PROVA", "CO_MUNICIPIO_PROVA", "NO_MUNICIPIO_PROVA",
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO",
    "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT",
    "TP_STATUS_REDACAO",
]

OPCIONAIS_DEMOGRAFICOS = [
    "TP_FAIXA_ETARIA", "TP_SEXO", "TP_ESTADO_CIVIL", "TP_COR_RACA", "TP_NACIONALIDADE"
]

QUESTIONARIO_Q = [f"Q{str(i).zfill(3)}" for i in range(1, 28)]  # Q001..Q027

def detectar_separador_e_encoding(caminho):
    testes_encoding = ["latin1", "utf-8", "cp1252"]
    for enc in testes_encoding:
        try:
            with open(caminho, "r", encoding=enc, errors="replace") as f:
                sample = f.read(32 * 1024)  # 32KB
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
                sep = dialect.delimiter
                return sep, enc
        except Exception:
            continue
    return None, None

def ler_csv_auto(caminho):
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(caminho)

    sep, enc = detectar_separador_e_encoding(caminho)
    tried = []
    if sep and enc:
        try:
            df = pd.read_csv(caminho, sep=sep, encoding=enc, low_memory=False)
            print(f"Lido com sniff: sep='{sep}' encoding='{enc}'")
            return df
        except Exception as e:
            print(f"Falha ao ler com sniff (sep={sep}, enc={enc}): {e}")
        tried.append((sep, enc))

    opcoes = [
        (",", "latin1"), (";", "latin1"), ("\t", "latin1"),
        (",", "utf-8"), (";", "utf-8"), ("\t", "utf-8"),
        (",", "cp1252"), (";", "cp1252"), ("\t", "cp1252"),
    ]
    for s, e in opcoes:
        if (s, e) in tried:
            continue
        try:
            df = pd.read_csv(caminho, sep=s, encoding=e, low_memory=False)
            print(f"Lido com tentativa: sep='{s}' encoding='{e}'")
            return df
        except Exception:
            continue

    # fallback: ler por linhas (útil apenas para diagnóstico)
    df = pd.read_csv(caminho, sep="\n", header=None, encoding="latin1", engine="python", low_memory=False)
    print("Fallback: arquivo lido por linha (sep='\\n'). Verifique manualmente o separador.")
    return df

def find_file_case_insensitive(desired_path: Path, search_pattern_lower: str = None):
    """
    Tenta resolver desired_path de forma case-insensitive.
    1) se desired_path existe -> retorna
    2) se não, tenta match direto no parent (iterdir)
    3) se não, faz rglob recursivo em parent (limitado) procurando por filename lower == target lower
    4) se search_pattern_lower fornecido, procura por qualquer arquivo .csv sob parent cujo nome contenha esse pattern
    Retorna Path ou None.
    """
    p = Path(desired_path)
    # 1) existe?
    if p.exists():
        return p

    parent = p.parent
    name = p.name

    # 2) procura no diretório pai por nome case-insensitive
    if parent.exists():
        for f in parent.iterdir():
            if f.name.lower() == name.lower():
                return f

    # 3) busca recursiva limitada (3 níveis) por filename exato case-insensitive
    # expandendo para o parent e alguns pais
    search_roots = [parent] + list(parent.parents)[:2]
    for root in search_roots:
        if root.exists():
            for f in root.rglob('*'):
                if f.is_file() and f.name.lower() == name.lower():
                    return f

    # 4) se foi passado search_pattern_lower, busca qualquer arquivo que contenha pattern (útil para MICRODADOS_ENEM_{ano}.csv)
    if search_pattern_lower:
        for root in search_roots:
            if root.exists():
                for f in root.rglob('*.csv'):
                    if search_pattern_lower in f.name.lower():
                        return f

    # não encontrado
    return None

def selecionar_e_tratar(caminho_entrada, caminho_saida,
                        manter_demograficos=True,
                        manter_questionario=False,
                        missing_values=None,
                        salvar_agregados=True):
    if missing_values is None:
        missing_values = DEFAULT_MISSING

    p = Path(caminho_entrada)
    if p.is_file():
        df = ler_csv_auto(p)
    elif p.is_dir():
        arquivos = sorted([f for f in p.rglob("*") if f.suffix.lower() in {".csv", ".txt"}])
        if not arquivos:
            raise FileNotFoundError(f"Nenhum CSV/TXT em {p}")
        dfl = []
        for f in arquivos:
            try:
                dfl.append(ler_csv_auto(f))
            except Exception as e:
                print("Erro ao ler", f, ":", e)
        if not dfl:
            raise RuntimeError("Nenhum arquivo lido com sucesso.")
        df = pd.concat(dfl, ignore_index=True)
    else:
        raise FileNotFoundError(caminho_entrada)

    # DEBUG: mostrar primeiras colunas lidas
    print("\nColunas detectadas (primeiras 30):")
    print(list(df.columns[:30]))

    # Se o CSV foi lido como 1 coluna com ';' dentro, tenta forçar ';'
    if len(df.columns) == 1:
        first_col = df.columns[0]
        if isinstance(first_col, str) and ";" in first_col:
            print("Atenção: parece que o arquivo foi lido como 1 coluna com ';' dentro. Forçando sep=';'.")
            try:
                df = pd.read_csv(p, sep=";", encoding="latin1", low_memory=False)
                print("Re-lido com sep=';' encoding='latin1'.")
                print("Colunas agora:", list(df.columns[:30]))
            except Exception as e:
                print("Falha ao reler com sep=';':", e)

    # limpeza básica
    df = df.apply(lambda c: c.str.strip() if c.dtype == "object" else c)
    df.replace(list(missing_values), pd.NA, inplace=True)
    df.replace(r'^\.+$', pd.NA, regex=True, inplace=True)

    # monta colunas a manter
    keep = list(ESSENCIAIS)
    if manter_demograficos:
        keep += OPCIONAIS_DEMOGRAFICOS
    if manter_questionario:
        keep += QUESTIONARIO_Q

    keep_exist = [c for c in keep if c in df.columns]
    print(f"\nColunas da lista que realmente existem no CSV: {keep_exist}")

    if not keep_exist:
        print("Erro: nenhuma das colunas essenciais/opcionais foi encontrada. Vou listar as 100 primeiras colunas do CSV para inspeção:")
        print(list(df.columns[:100]))
        raise RuntimeError("Colunas essenciais não encontradas no CSV. Verifique separador/nome das colunas.")

    df_sel = df[keep_exist].copy()

    # converte notas para numérico
    for nota_col in ["NU_NOTA_CN","NU_NOTA_CH","NU_NOTA_LC","NU_NOTA_MT","NU_NOTA_REDACAO"]:
        if nota_col in df_sel.columns:
            df_sel[nota_col] = pd.to_numeric(df_sel[nota_col].astype(str).str.replace(r'[^\d\.\-]','',regex=True), errors='coerce')

    # cria MEDIA_NOTAS
    notas_cols = [c for c in ["NU_NOTA_CN","NU_NOTA_CH","NU_NOTA_LC","NU_NOTA_MT"] if c in df_sel.columns]
    if notas_cols:
        df_sel["MEDIA_NOTAS"] = df_sel[notas_cols].mean(axis=1, skipna=True)

    # remove identificador pessoal por segurança (caso exista)
    if "NU_INSCRICAO" in df_sel.columns:
        df_sel.drop(columns=["NU_INSCRICAO"], inplace=True, errors='ignore')

    # salva
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
    df_sel.to_csv(caminho_saida, sep="\t", index=False, encoding="latin1")
    print(f"\nArquivo tratado salvo: {caminho_saida}")
    print("Colunas finais:", list(df_sel.columns))
    print("Linhas:", len(df_sel))

    # agregados simples
    if salvar_agregados and "SG_UF_PROVA" in df_sel.columns:
        agg_uf = df_sel.groupby("SG_UF_PROVA").agg(
            N_ALUNOS=("SG_UF_PROVA","count"),
            MEDIA_GERAL=("MEDIA_NOTAS","mean")
        ).reset_index()
        p1 = Path(caminho_saida).with_name(Path(caminho_saida).stem + "_agregado_UF.csv")
        agg_uf.to_csv(p1, sep="\t", index=False, encoding="latin1")
        print("Agregado por UF salvo:", p1)

    if salvar_agregados and "CO_MUNICIPIO_PROVA" in df_sel.columns and "NO_MUNICIPIO_PROVA" in df_sel.columns:
        agg2 = df_sel.groupby(["CO_MUNICIPIO_PROVA","NO_MUNICIPIO_PROVA"]).agg(
            N_ALUNOS=("CO_MUNICIPIO_PROVA","count"),
            MEDIA_GERAL=("MEDIA_NOTAS","mean")
        ).reset_index()
        p2 = Path(caminho_saida).with_name(Path(caminho_saida).stem + "_agregado_mun.csv")
        agg2.to_csv(p2, sep="\t", index=False, encoding="latin1")
        print("Agregado por município salvo:", p2)

    return df_sel

# ------------------ LOOP POR ANO (case-insensitive resolve) ------------------
if __name__ == "__main__":
    ano_inicio, ano_fim = 2014, 2025  # fará 2014..2024
    raiz_base = Path(r"C:\Users\efg\Music\danylo\tratados")
    for ano in range(ano_inicio, ano_fim):
        try:
            # caminho "esperado" (padrão). Pode ter maiúsculas/minúsculas diferentes no disco
            entrada_esperada = raiz_base / f"microdados_enem_{ano}_extraido" / "DADOS" / f"MICRODADOS_ENEM_{ano}.csv"
            # tenta resolver caso-insensitive. se não achar, tenta procurar por qualquer CSV que contenha 'microdados_enem_{ano}' no nome.
            resolved = find_file_case_insensitive(entrada_esperada, search_pattern_lower=f"microdados_enem_{ano}".lower())
            if resolved is None:
                print("\n" + "="*70)
                print(f"Aviso: arquivo não encontrado (nem case-insensitive) para {ano} em {entrada_esperada}. Pulando ano.")
                continue
            entrada = str(resolved)
            saida   = rf"C:\Users\efg\Music\danylo\dados_finais\microdados_enem_{ano}_filtrado\microdados_enem_{ano}_filtrado.csv"
            print("\n" + "="*70)
            print(f"Processando ano {ano}: {entrada}")
            selecionar_e_tratar(entrada, saida, manter_demograficos=True, manter_questionario=False)
        except Exception as e:
            print(f"Erro processando {ano}: {e}")
            traceback.print_exc()
            continue

    print("\nLoop finalizado.")
