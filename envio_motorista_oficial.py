"""
Envio do link do CTE ao motorista por WhatsApp (Banco Dedicado / Supabase).

Fluxo (uma rodada por execução, pensado para o Agendador de Tarefas):
  1. Preenche o telefone vazio de "Shopee_viagens" buscando por CPF em motoristas (BancoDados_SLI)
  2. Busca em "Shopee_viagens" as linhas com:
       - emissao = SIM
       - enviado_motorista em branco
       - telefone preenchido
       - motorista já em rota de entrega:
           * 1 coleta  (facility_3 vazio):      ata_2 preenchido e ata_3/ata_4 vazios
           * 2 coletas (facility_3 preenchido): ata_2 e ata_5 preenchidos e ata_3/ata_4 vazios
  3. Para cada travel_id, procura no storage (bucket documentos_motoristas, pasta emissao/<travel_id>/)
  4. Se os arquivos existem -> monta os links públicos, envia no WhatsApp Web e marca 'Sim'
     (o Chrome só é aberto quando há algo para enviar)
  5. Número sem WhatsApp -> marca 'Sem WhatsApp'
  6. Sem arquivos ainda -> deixa em branco (tenta de novo na próxima execução)

.env (na raiz do projeto):
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SUPABASE_URL
  SLI_DB_HOST, SLI_DB_PORT, SLI_DB_NAME, SLI_DB_USER, SLI_DB_PASSWORD
  MON_DB_HOST, MON_DB_NAME, MON_DB_USER, MON_DB_PASS, MON_DB_PORT   (monitoramento: execucoes_robos)
"""
import os
import re
import time
import random
import socket
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv(Path(__file__).resolve().parent / ".env")
print("Iniciando Automação Envio Watsap Motorista")

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
SLI_CONFIG = {
    "host": os.getenv("SLI_DB_HOST"),
    "port": int(os.getenv("SLI_DB_PORT", 5432)),
    "dbname": os.getenv("SLI_DB_NAME"),
    "user": os.getenv("SLI_DB_USER"),
    "password": os.getenv("SLI_DB_PASSWORD"),
}

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
BUCKET = "documentos_motoristas"
PASTA = "emissao"
PERFIL_CHROME = r"C:\chrome_whatsapp_perfil"  # mantém o login do WhatsApp salvo
INTERVALO_RODADAS = 300  # (não usado no modo agendador)
DELAY_ENTRE_ENVIOS = (8, 20)  # aleatório, evita bloqueio

# Modo teste: True = abre a conversa e digita a mensagem, mas NÃO envia e NÃO marca 'Sim'
MODO_TESTE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("envio_motorista")


# ----------------------------------------------------------------------------
# BANCO DE MONITORAMENTO (execucoes_robos) - mesmo padrão dos outros robôs
# ----------------------------------------------------------------------------
NOME_ROBO = "EXP - Envio CTE Motorista"
CICLO_ROBO = "Agendador de Tarefas"
FINALIDADE_ROBO = "Envio do link do CTE ao motorista por WhatsApp"


def registrar_execucao_nuvem(nome_robo, status, detalhes="", ciclo="", finalidade=""):
    DB_HOST = os.getenv("MON_DB_HOST")
    DB_NAME = os.getenv("MON_DB_NAME")
    DB_USER = os.getenv("MON_DB_USER")
    DB_PASS = os.getenv("MON_DB_PASS")
    DB_PORT = os.getenv("MON_DB_PORT")


    nome_maquina = socket.gethostname()
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        conexao = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER,
            password=DB_PASS, port=DB_PORT
        )
        cursor = conexao.cursor()
        cursor.execute(
            "INSERT INTO execucoes_robos (data_hora, nome_robo, maquina, status, detalhes, ciclo, finalidade) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (agora, nome_robo, nome_maquina, status, detalhes, ciclo, finalidade)
        )
        conexao.commit()
        cursor.close()
        conexao.close()
        print(f"☁️ Log gravado no banco: {status}")
    except Exception as e:
        print(f"[AVISO] Não foi possível salvar o log no banco de dados: {e}")


# ----------------------------------------------------------------------------
# BANCO
# ----------------------------------------------------------------------------
def conectar():
    return psycopg2.connect(**DB_CONFIG)


def buscar_pendentes(conn):
    sql = """
        SELECT travel_id, motorista, telefone
        FROM "Shopee_viagens"
        WHERE UPPER(TRIM(emissao)) = 'SIM'
          AND (enviado_motorista IS NULL OR TRIM(enviado_motorista) = '')
          AND telefone IS NOT NULL AND TRIM(telefone) <> ''
          AND (
                -- 1 coleta: facility_3 vazio, saiu da coleta e ainda não chegou no destino
                (    COALESCE(TRIM(facility_3), '') = ''
                 AND COALESCE(TRIM(ata_2), '') <> ''
                 AND COALESCE(TRIM(ata_3), '') = ''
                 AND COALESCE(TRIM(ata_4), '') = '')
             OR
                -- 2 coletas: facility_3 preenchido, passou nas duas coletas e ainda não chegou no destino
                (    COALESCE(TRIM(facility_3), '') <> ''
                 AND COALESCE(TRIM(ata_2), '') <> ''
                 AND COALESCE(TRIM(ata_5), '') <> ''
                 AND COALESCE(TRIM(ata_3), '') = ''
                 AND COALESCE(TRIM(ata_4), '') = '')
              )
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def buscar_arquivos_emissao(conn, travel_id):
    """Retorna os paths dos arquivos de emissão no storage para o travel_id."""
    sql = """
        SELECT name
        FROM storage.objects
        WHERE bucket_id = %s
          AND name LIKE %s
        ORDER BY created_at
    """
    with conn.cursor() as cur:
        cur.execute(sql, (BUCKET, f"{PASTA}/{travel_id}/%"))
        return [r[0] for r in cur.fetchall()]


def marcar_enviado(conn, travel_id, status="Sim"):
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE "Shopee_viagens" SET enviado_motorista = %s WHERE travel_id = %s',
            (status, travel_id),
        )
    conn.commit()


def _montar_tel(ddd, num):
    d = re.sub(r"\D", "", ddd or "")
    n = re.sub(r"\D", "", num or "")
    if len(n) in (10, 11):  # número já veio com DDD
        return n
    if len(n) in (8, 9) and len(d) == 2:  # número sem DDD + DDD separado
        return d + n
    return None


def completar_telefones(conn):
    """Preenche telefone vazio na Shopee_viagens buscando por CPF em motoristas (BancoDados_SLI)."""
    with conn.cursor() as cur:
        cur.execute(r'''
            SELECT DISTINCT regexp_replace(cpf, '\D', '', 'g')
            FROM "Shopee_viagens"
            WHERE (telefone IS NULL OR TRIM(telefone) = '')
              AND cpf IS NOT NULL AND TRIM(cpf) <> ''
              AND (enviado_motorista IS NULL OR TRIM(enviado_motorista) = '')
        ''')
        cpfs = [r[0] for r in cur.fetchall() if r[0]]
    if not cpfs:
        return

    sli = psycopg2.connect(**SLI_CONFIG)
    try:
        with sli.cursor() as cur:
            cur.execute(r'''
                SELECT regexp_replace(cpf, '\D', '', 'g'),
                       ddd_celular, celular,
                       ddd_fone_pessoal_1, fone_pessoal_1,
                       ddd_fone_pessoal_2, fone_pessoal_2
                FROM motoristas
                WHERE regexp_replace(cpf, '\D', '', 'g') = ANY(%s)
            ''', (cpfs,))
            linhas = cur.fetchall()
    finally:
        sli.close()

    achados = {}
    for cpf_d, *campos in linhas:
        if cpf_d in achados:
            continue
        for ddd, num in zip(campos[0::2], campos[1::2]):
            tel = _montar_tel(ddd, num)
            if tel:
                achados[cpf_d] = tel
                break

    with conn.cursor() as cur:
        for cpf_d, tel in achados.items():
            cur.execute(r'''
                UPDATE "Shopee_viagens" SET telefone = %s
                WHERE regexp_replace(cpf, '\D', '', 'g') = %s
                  AND (telefone IS NULL OR TRIM(telefone) = '')
            ''', (tel, cpf_d))
    conn.commit()
    log.info("Telefones preenchidos para %d CPF(s) de %d consultados", len(achados), len(cpfs))


# ----------------------------------------------------------------------------
# LINKS / MENSAGEM
# ----------------------------------------------------------------------------
def link_publico(path):
    # bucket é PUBLIC -> URL direta, sem token
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{quote(path)}"


def rotulo_arquivo(nome):
    """Classifica o documento pelo nome do arquivo; se não reconhecer, não inventa rótulo."""
    n = nome.lower()
    if "damdfe" in n or "mdfe" in n or "manifesto" in n:
        return "Manifesto (MDF-e)"
    if "ctrb" in n:
        return "CTRB"
    if "danfe" in n:
        return "DANFE"
    if "dacte" in n or "cte" in n:
        return "CTE"
    return "Documento"


def montar_mensagem(motorista, travel_id, paths):
    primeiro_nome = (motorista or "").split(" ")[0].title() or "Motorista"
    linhas = [
        "*Transking* 🚚",
        f"Olá, {primeiro_nome}! Seguem os documentos da sua viagem *{travel_id}*:",
        "",
    ]
    for p in paths:
        rotulo = rotulo_arquivo(p.split("/")[-1])
        linhas.append(f"*{rotulo}*")
        linhas.append(link_publico(p))
        linhas.append("")
    linhas.append("Qualquer dúvida, é só chamar. _Equipe Transking_")
    return "\n".join(linhas)


def normalizar_telefone(tel):
    d = re.sub(r"\D", "", str(tel))
    if len(d) in (10, 11):  # sem DDI
        d = "55" + d
    return d if len(d) in (12, 13) else None


# ----------------------------------------------------------------------------
# WHATSAPP (Selenium + WhatsApp Web)
# ----------------------------------------------------------------------------
def _logado(d):
    return (
        d.find_elements(By.ID, "pane-side")
        or d.find_elements(By.XPATH, '//*[@role="textbox"]')
        or d.find_elements(By.XPATH, '//*[@aria-label="Lista de conversas"]')
    )


def abrir_whatsapp():
    opts = webdriver.ChromeOptions()
    opts.add_argument(f"--user-data-dir={PERFIL_CHROME}")
    driver = webdriver.Chrome(options=opts)
    driver.get("https://web.whatsapp.com")
    log.info("Iniciando login do WhatsApp Web")
    try:
        WebDriverWait(driver, 60).until(_logado)
    except TimeoutException:
        log.warning("Não reconheci a tela inicial; seguindo mesmo assim (confira se está logado).")
    time.sleep(3)
    return driver


class NumeroSemWhatsapp(Exception):
    pass


def _chat_ou_popup(d):
    caixa = d.find_elements(By.XPATH, '//footer//*[@role="textbox"]')
    if caixa:
        return caixa[0]
    botoes = d.find_elements(By.XPATH, '//div[@role="dialog"]//button')
    if botoes:
        d.execute_script("arguments[0].click();", botoes[-1])  # clica no OK
        raise NumeroSemWhatsapp()
    return False


def enviar_whatsapp(driver, telefone, mensagem, enviar=True):
    # quebras de linha viram %0A pelo quote()
    driver.get(f"https://web.whatsapp.com/send?phone={telefone}&text={quote(mensagem)}")
    caixa = WebDriverWait(driver, 40).until(_chat_ou_popup)
    time.sleep(2)
    if not enviar:
        time.sleep(8)  # tempo para você conferir a mensagem na tela
        return
    caixa.send_keys(Keys.ENTER)
    time.sleep(3)  # dá tempo de sair antes de navegar para o próximo


# ----------------------------------------------------------------------------
# LOOP PRINCIPAL
# ----------------------------------------------------------------------------
def processar_rodada(conn, obter_driver):
    enviados = 0
    falhas = []

    try:
        completar_telefones(conn)
    except Exception as e:
        conn.rollback()
        log.exception("Falha ao completar telefones: %s", e)
        falhas.append(f"completar telefones: {e}")
    pendentes = buscar_pendentes(conn)
    log.info("Pendentes: %d", len(pendentes))

    for row in pendentes:
        travel_id = row["travel_id"]
        telefone = None
        try:
            paths = buscar_arquivos_emissao(conn, travel_id)
            if not paths:
                log.info("%s: emissão ainda não está no storage", travel_id)
                continue

            telefone = normalizar_telefone(row["telefone"])
            if not telefone:
                log.warning("%s: telefone inválido (%s)", travel_id, row["telefone"])
                continue

            msg = montar_mensagem(row["motorista"], travel_id, paths)

            if MODO_TESTE:
                log.info("%s: [TESTE] enviaria para %s:\n%s", travel_id, telefone, msg)
                enviar_whatsapp(obter_driver(), telefone, msg, enviar=False)
                continue

            enviar_whatsapp(obter_driver(), telefone, msg)
            marcar_enviado(conn, travel_id)
            enviados += 1
            log.info("%s: enviado para %s", travel_id, telefone)

            time.sleep(random.uniform(*DELAY_ENTRE_ENVIOS))
        except NumeroSemWhatsapp:
            marcar_enviado(conn, travel_id, "Sem WhatsApp")
            log.warning("%s: %s não está no WhatsApp; marcado 'Sem WhatsApp'", travel_id, telefone)
            continue

        except Exception as e:
            conn.rollback()
            log.exception("%s: falha no envio: %s", travel_id, e)
            falhas.append(f"{travel_id}: {str(e).split('Stacktrace:')[0].strip()}")

    return enviados, falhas


# Execução única, pensada para o Agendador de Tarefas.
# O Chrome só abre se houver algo para enviar e é fechado ao final.
def main():
    estado = {"driver": None}

    def obter_driver():
        if estado["driver"] is None:  # só abre o Chrome na primeira necessidade
            estado["driver"] = abrir_whatsapp()
        return estado["driver"]

    try:
        enviados, falhas = 0, []
        conn = conectar()
        try:
            enviados, falhas = processar_rodada(conn, obter_driver)
        finally:
            conn.close()
            if estado["driver"] is not None:
                estado["driver"].quit()  # fecha o Chrome se foi aberto
        log.info("Rodada concluída: %d enviado(s), %d falha(s).", enviados, len(falhas))

        if falhas:
            detalhes = f"{len(falhas)} falha(s), {enviados} enviado(s). 1ª: {falhas[0]}"
            registrar_execucao_nuvem(NOME_ROBO, "ERRO DE EXECUÇÃO", detalhes[:210], CICLO_ROBO, FINALIDADE_ROBO)
        else:
            registrar_execucao_nuvem(NOME_ROBO, "SUCESSO", f"Rodada concluída. Enviados: {enviados}",
                                     CICLO_ROBO, FINALIDADE_ROBO)

    except Exception as erro:
        erro_limpo = str(erro).split("Stacktrace:")[0].strip()
        log.exception("Erro geral na rodada: %s", erro_limpo)
        registrar_execucao_nuvem(NOME_ROBO, "ERRO DE EXECUÇÃO", erro_limpo[:210], CICLO_ROBO, FINALIDADE_ROBO)


if __name__ == "__main__":
    main()