import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
import schedule
import time
import locale
import os

# Definir o locale para o Brasil para a formatação de moeda
locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")

# Configurações de e-mail
SMTP_SERVER = "smtpout.secureserver.net"  # Servidor SMTP
SMTP_PORT = 465  # Porta SMTP
EMAIL_USER = "hallisson@usinadoseguro.com"  # Seu e-mail
EMAIL_PASS = "Fome!1739"  # Senha ou app password do e-mail

# E-mail do diretor para envio do relatório consolidado semanal
EMAIL_DIRETOR = "hallisson@agenciaduma.com"  # E-mail do diretor

# Conectar ao banco de dados
def conectar_bd():
    connection = psycopg2.connect(
        host="24.199.120.210",
        database="usina",
        user="halisson",
        password="Fome!1739",
        port="5432",
        connect_timeout="20"
    )
    return connection

# Função para associar o supervisor ao seu setor
def obter_setor_por_supervisor(supervisor):
    setores = {
        "MARIA JOSE DE ANDRADE": "Individual", 
        "JOSE ANDRE DE A BARBOSA": "Empresarial",
        "ADEILDA JULIA DA SILVA ARAUJO": "Petrolina"
    }
    return setores.get(supervisor, "Setor Desconhecido")

# Função para associar o supervisor ao seu e-mail
def obter_email_por_supervisor(supervisor):
    emails = {
        "MARIA JOSE DE ANDRADE": "alessandro.hallisson@gmail.com", 
        "JOSE ANDRE DE A BARBOSA": "hallisson@usinadoseguro.com",
        "ADEILDA JULIA DA SILVA ARAUJO": "alessandro.hallisson@yahoo.com.br"
    }
    return emails.get(supervisor, "")

# Função para executar a consulta SQL para um supervisor
def executar_consulta(supervisor_selecionado, data_inicio, data_fim):
    connection = conectar_bd()
    cursor = connection.cursor()

    query = """
    SELECT
        operators.name AS operadora,
        plans.description AS plano,
        COUNT(proposals.id) AS quantidade_propostas,
        SUM(proposals.life_quantity) AS quantidade_vidas,
        SUM(proposals.proposal_value) AS valor_total
    FROM proposals
    JOIN plans ON proposals.plan_id = plans.id
    JOIN supervisors ON proposals.supervisor_id = supervisors.id
    JOIN operators ON plans.operator_id = operators.id
    WHERE supervisors.name = %s
    AND proposals.created_at BETWEEN %s AND %s
    GROUP BY operators.name, plans.description
    ORDER BY quantidade_propostas DESC;
    """
    
    cursor.execute(query, (supervisor_selecionado, data_inicio, data_fim))
    resultados = cursor.fetchall()
    
    df = pd.DataFrame(resultados, columns=["Operadora", "Plano", "Quantidade de Propostas", "Quantidade de Vidas", "Valor Total"])
    cursor.close()
    connection.close()
    
    return df

# Função para gerar o relatório em PDF
def gerar_relatorio_pdf(supervisor, setor, df, data_inicio_str, data_fim_str, file_path):
    c = canvas.Canvas(file_path, pagesize=A4)
    width, height = A4
    c.setFont("Helvetica", 10)

    y = height - 2 * cm
    y_offset = 0.8 * cm

    # Adicionar título do supervisor ao PDF
    titulo = f"Relatório de Produção - {supervisor} - Setor: {setor} - {data_inicio_str} a {data_fim_str}"
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(width / 2, y, titulo)
    y -= 1.5 * cm

    # Cabeçalhos
    c.setFont("Helvetica-Bold", 10)
    c.drawString(1 * cm, y, "Operadora")
    c.drawString(6 * cm, y, "Plano")
    c.drawString(12 * cm, y, "Qtd. Propostas")
    c.drawString(15 * cm, y, "Qtd. Vidas")
    c.drawString(18 * cm, y, "Valor Total")
    y -= y_offset
    c.line(1 * cm, y + y_offset / 2, width - 1 * cm, y + y_offset / 2)
    y -= y_offset

    c.setFont("Helvetica", 9)

    for index, row in df.iterrows():
        valor_total_formatado = locale.currency(row["Valor Total"], grouping=True)
        c.drawString(1 * cm, y, str(row["Operadora"])[:20])
        c.drawString(6 * cm, y, str(row["Plano"])[:25])
        c.drawRightString(14 * cm, y, str(row["Quantidade de Propostas"]))
        c.drawRightString(17 * cm, y, str(row["Quantidade de Vidas"]))
        c.drawRightString(20 * cm, y, valor_total_formatado)

        y -= y_offset

        if y < 2 * cm:
            c.showPage()
            y = height - 2 * cm
            c.setFont("Helvetica-Bold", 12)
            c.drawCentredString(width / 2, y, titulo)
            y -= 1.5 * cm
            c.setFont("Helvetica-Bold", 10)
            c.drawString(1 * cm, y, "Operadora")
            c.drawString(6 * cm, y, "Plano")
            c.drawString(12 * cm, y, "Qtd. Propostas")
            c.drawString(15 * cm, y, "Qtd. Vidas")
            c.drawString(18 * cm, y, "Valor Total")
            y -= y_offset
            c.line(1 * cm, y + y_offset / 2, width - 1 * cm, y + y_offset / 2)
            y -= y_offset
            c.setFont("Helvetica", 9)

    # Linha de total
    total_propostas = df["Quantidade de Propostas"].sum()
    total_vidas = df["Quantidade de Vidas"].sum()
    total_valor = df["Valor Total"].sum()
    total_valor_formatado = locale.currency(total_valor, grouping=True)

    c.setFont("Helvetica-Bold", 10)
    y -= y_offset
    c.drawString(1 * cm, y, "TOTAL")
    c.drawRightString(14 * cm, y, str(total_propostas))
    c.drawRightString(17 * cm, y, str(total_vidas))
    c.drawRightString(20 * cm, y, total_valor_formatado)

    c.save()

# Função para enviar o e-mail com o relatório em anexo
def enviar_email(supervisor_email, subject, body, file_path):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = supervisor_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with open(file_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
    msg.attach(part)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, supervisor_email, msg.as_string())

# Função para gerar e enviar relatórios diários
def gerar_enviar_relatorios_diarios():
    ontem = datetime.now() - timedelta(days=1)
    data_inicio_str = data_fim_str = ontem.strftime('%Y-%m-%d')
    supervisores = ["MARIA JOSE DE ANDRADE", "JOSE ANDRE DE A BARBOSA", "ADEILDA JULIA DA SILVA ARAUJO"]

    for supervisor in supervisores:
        setor = obter_setor_por_supervisor(supervisor)
        email = obter_email_por_supervisor(supervisor)
        df = executar_consulta(supervisor, data_inicio_str, data_fim_str)
        if not df.empty:
            file_path = f"relatorio_{supervisor}_{data_inicio_str}.pdf"
            gerar_relatorio_pdf(supervisor, setor, df, data_inicio_str, data_fim_str, file_path)
            subject = f"Relatório Diário - {supervisor} - {data_inicio_str}"
            body = f"Prezada(o) {supervisor},\n\nSegue o relatório diário referente ao dia {data_inicio_str}.\n\nAtenciosamente,"
            enviar_email(email, subject, body, file_path)

# Função para gerar e enviar o relatório consolidado semanal
def gerar_enviar_relatorio_semanal():
    hoje = datetime.now()
    segunda_passada = hoje - timedelta(days=hoje.weekday() + 7)
    domingo_passado = segunda_passada + timedelta(days=6)
    data_inicio_str = segunda_passada.strftime('%Y-%m-%d')
    data_fim_str = domingo_passado.strftime('%Y-%m-%d')
    supervisores = ["MARIA JOSE DE ANDRADE", "JOSE ANDRE DE A BARBOSA", "ADEILDA JULIA DA SILVA ARAUJO"]

    for supervisor in supervisores:
        setor = obter_setor_por_supervisor(supervisor)
        df = executar_consulta(supervisor, data_inicio_str, data_fim_str)
        if not df.empty:
            file_path = f"relatorio_consolidado_semanal_{data_inicio_str}_a_{data_fim_str}.pdf"
            gerar_relatorio_pdf(supervisor, setor, df, data_inicio_str, data_fim_str, file_path)
            subject = f"Relatório Consolidado Semanal - {data_inicio_str} a {data_fim_str}"
            body = f"Prezada Direção,\n\nSegue o relatório consolidado semanal referente ao período {data_inicio_str} a {data_fim_str}.\n\nAtenciosamente,"
            enviar_email(EMAIL_DIRETOR, subject, body, file_path)

# Agendamento
schedule.every().day.at("07:00").do(gerar_enviar_relatorios_diarios)
schedule.every().monday.at("07:10").do(gerar_enviar_relatorio_semanal)

# Loop para manter o agendamento
while True:
    schedule.run_pending()
    time.sleep(2)