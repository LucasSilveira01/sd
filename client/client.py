import socket
import ssl
import os
import platform
import subprocess
import json
from Bio.PDB import PDBParser, PDBIO, Superimposer
import mysql.connector
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import re

subprocess.call(
    ['mkdir', 'arquivos'], shell=True)
subprocess.call(
    ['mkdir', 'referencias'], shell=True)
subprocess.call(
    ['mkdir', 'relatorios'], shell=True)
# Configuracões do cliente
HOST = 'localhost'
PORT = 12345
HOST_REPL = 'localhost'
PORT_REPL = PORT + 1
ca_cert = 'client-cert.pem'
state_file = 'state.json'
# Configurar as informacões de conexao
config = {
    'user': 'root',
    'password': '',
    'host': 'localhost'
}
connection = mysql.connector.connect(**config)
cursor = connection.cursor()
if connection.is_connected():
    print("Conexao ao banco de dados bem-sucedida.")
    nome_banco_dados = "sd_repl"
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(nome_banco_dados))
    try:
        cursor.execute("USE {}".format(nome_banco_dados))
        print("Banco de dados alterado com sucesso.")
        print("Após a mudanca para o banco de dados:", connection.database)
    except Exception as e:
        print(f"Falha ao mudar para o banco de dados: {e}") 
if os.path.exists(state_file):
    with open(state_file, 'r') as f:
        state = json.load(f)
        last_processed_index = state.get('last_processed_index', None)
else:
    last_processed_index = None

pdb_directory = "arquivos"
start_index = last_processed_index if last_processed_index is not None else 0
pdb_directory = "arquivos"
# Funcao para escrever no arquivo de log
def escrever_no_log(mensagem, nome_arquivo='logfile.txt'):
    hora_atual = time.strftime('%Y-%m-%d %H:%M:%S')
    mensagem_com_hora = f'{hora_atual} - {mensagem}'
    with open(nome_arquivo, 'a') as arquivo:
        arquivo.write(f'{mensagem_com_hora}\n')
def replicate_mysql_to_client(client):
    import_command = "mysql -u root sd_repl < received_dump.sql"
    # Execute os comandos no cliente
    escrever_no_log('Replicando banco de dados MySQL...')
    print("Replicando banco de dados MySQL...")
    
    os.system(import_command)  # Execute o comando de importacao
    print('Replicacao bem sucedida!')
    escrever_no_log('Replicacao bem sucedida!')
def receive_mysql_dump_from_server(client):
    dump_file = "received_dump.sql"
    try:
        with open(dump_file, "wb") as file:
            while True:
                data = client.recv(1024)
                if b"Finalizado" in data:
                    file.write(data.replace(b"Finalizado", b""))
                    print("[*] Arquivo MySQL dump recebido do servidor")
                    escrever_no_log('[*] Arquivo MySQL dump recebido do servidor')
                    break  # Saia do loop quando receber o marcador "Finalizado"
                file.write(data)
    except Exception as e:
        print(f"[*] Erro ao receber arquivo: {e}")
    finally:
        file.close()  # Certifique-se de fechar o arquivo após a escrita
# Funcao para buscar IDs de proteínas no banco de dados
def get_protein_ids_from_db():
    try:
        cursor.execute("SELECT cod_id FROM sd_repl.proteins limit 2")
        protein_ids = [str(row[0]) for row in cursor.fetchall()]
        return protein_ids
    except Exception as e:
        print(f"Erro ao buscar IDs de proteínas no banco de dados: {e}")
        return []

def superimposer(protein):
    global start_index
    parser = PDBParser(QUIET=True)

    if(not os.path.exists(f'referencias/{protein}.pdb')):
        url = f"https://files.rcsb.org/download/{protein}.pdb"
        local_filename = f"referencias/{protein}.pdb"
        print(f'Starting Reference Download from {url}')
        escrever_no_log(f'Starting Reference Download from {url}')
        response = requests.get(url)
        if response.status_code == 200:
            # Abra o arquivo local em modo de escrita binária
            with open(local_filename, "wb") as file:
                # Escreva o conteúdo do arquivo baixado no arquivo local
                file.write(response.content)
            print(f"Arquivo {local_filename} baixado com sucesso.")
            escrever_no_log(f"Arquivo {local_filename} baixado com sucesso.")
        else:
            print(
                f"Falha ao baixar o arquivo. Código de status: {response.status_code}")
    reference_structure = parser.get_structure(protein, f"referencias/{protein}.pdb")
    pdb_files = os.listdir(pdb_directory)
    for pdb_filename in pdb_files:
        # Atualize o estado com o índice do último arquivo processado
        last_processed_index = pdb_files.index(pdb_filename)
        state = {'last_processed_index': last_processed_index}
        if(os.path.exists(f'relatorios/superposed_{protein}+{pdb_filename}')):
            with open(state_file, 'w') as f:
                json.dump(state, f)
            continue
        if pdb_filename.endswith(".pdb"):
            # Carrega a estrutura da proteína a ser sobreposta
            target_pdb_parser = PDBParser()
            target_id = pdb_filename.split('.')[0]
            target_structure = parser.get_structure(target_id, os.path.join(pdb_directory, pdb_filename))

            # Extraia os átomos das estruturas e armazene-os em listas
            reference_atoms = [atom for atom in reference_structure.get_atoms()]
            target_atoms = [atom for atom in target_structure.get_atoms()]

            # Inicialize o Superimposer
            superimposer = Superimposer()

            # Defina os átomos da proteína de referência e da proteína a ser sobreposta
            reference_atoms = []
            target_atoms = []

            for ref_model, target_model in zip(reference_structure, target_structure):
                for ref_chain, target_chain in zip(ref_model, target_model):
                    for ref_residue, target_residue in zip(ref_chain, target_chain):
                        for ref_atom, target_atom in zip(ref_residue, target_residue):
                            reference_atoms.append(ref_atom)
                            target_atoms.append(target_atom)
            # Configure os átomos no Superimposer
            superimposer.set_atoms(reference_atoms, target_atoms)

            # Realize a sobreposicao
            superimposer.apply(target_structure)
            output_pdb_filename = f"relatorios/superposed_{protein}+{pdb_filename}"
            io = PDBIO()
            io.set_structure(target_structure)
            io.save(output_pdb_filename)

            print(f"Proteínas superpostas e salvas em {output_pdb_filename}")
            escrever_no_log(f"Proteínas superpostas e salvas em {output_pdb_filename}")
           
        with open(state_file, 'w') as f:
            print(state)
            json.dump(state, f)
        escrever_no_log('State File atualizado com sucesso!')
    start_index = 0
def send_files():
     # Criacao do soquete
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Configuracao do contexto SSL
    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    # Configuracao do contexto SSL com verificacao de certificado desativada
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    # Conectar-se ao servidor
    ssl_socket = ssl_context.wrap_socket(client_socket)
    try:
        ssl_socket.connect((HOST, PORT))
        print('Conexão com servidor principal estabelecida!')
    except ConnectionRefusedError:
        print(f"Não foi possível conectar ao servidor principal. Tentando o servidor secundário na porta {PORT + 1}")
        ssl_socket.connect((HOST_REPL, PORT_REPL))
        print('Conexão com servidor secundário estabelecida!')
    escrever_no_log('Conectado ao servidor para enviar arquivo')

    # Envie a flag indicando que um arquivo será enviado
    ssl_socket.sendall(b'file')
    escrever_no_log('Flag File enviada com sucesso!')
    diretorio = 'relatorios'
    arquivos = os.listdir('relatorios')
    arquivos_str = "\n".join(arquivos)
    ssl_socket.send(arquivos_str.encode())
    for arquivo in arquivos:
        with open(os.path.join(diretorio, arquivo), 'rb') as file:
            dados = file.read(1024)
            while dados:
                ssl_socket.send(dados)
                dados = file.read(1024)
    ssl_socket.close()
def extract_last_range_from_log(logfile):
    with open(logfile, 'r') as f:
        log_content = f.read()

    # Use regular expression to find all occurrences of the range pattern in the log
    range_pattern = re.compile(r'Range atual: (\d+)-(\d+)')
    matches = range_pattern.findall(log_content)

    if matches:
        # Retrieve the last occurrence of the range
        last_range = matches[-1]
        start_range, end_range = last_range
        return start_range, end_range
    else:
        return None
def extract_range_done_from_log(logfile):
    with open(logfile, 'r') as f:
        log_content = f.read()

    # Use regular expression to find all occurrences of the target message
    done_pattern = re.compile(r'Range (\d+) - (\d+) - Done')
    matches = done_pattern.findall(log_content)

    if matches:
        # Retrieve the last occurrence of the target message
        last_done = matches[-1]
        start_range, end_range = last_done
        return start_range, end_range
    else:
        return None
def connect_to_server(i):   
    escrever_no_log('Cliente Iniciado')
    if(last_processed_index == None):
        current_range = extract_last_range_from_log('logfile.txt')
        range_done = extract_range_done_from_log('logfile.txt')
        print(current_range, range_done)
        # Criacao do soquete
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Configuracao do contexto SSL
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
        # Configuracao do contexto SSL com verificacao de certificado desativada
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        # Conectar-se ao servidor
        ssl_socket = ssl_context.wrap_socket(client_socket)
        try:
            ssl_socket.connect((HOST, PORT))
            print('Conexão com servidor principal estabelecida!')
        except ConnectionRefusedError:
            print(f"Não foi possível conectar ao servidor principal. Tentando o servidor secundário na porta {PORT + 1}")
            ssl_socket.connect((HOST_REPL, PORT_REPL))
            print('Conexão com servidor secundário estabelecida!')
        escrever_no_log('Conectado ao servidor')
        if(current_range is None):
            ssl_socket.send(f"Initiating".encode())
            escrever_no_log('Flag Init enviado!')

            response = ssl_socket.recv(1024)
            range = (response.decode())
            split = range.split('-')
            print(split[0], split[1])
            escrever_no_log(f'Range atual: {range}')
            sql = f'SELECT * FROM sd_repl.proteins where id between {split[0]} and {split[1]}'
        else:
            if range_done is not None:
                if current_range[0] == range_done[0] and current_range[1] == range_done[1]:
                
                    ssl_socket.send(f"Initiating".encode())
                    escrever_no_log('Flag Init enviado!')

                    response = ssl_socket.recv(1024)
                    range = (response.decode())
                    split = range.split('-')
                    print(split[0], split[1])
                    escrever_no_log(f'Range atual: {range}')
                    sql = f'SELECT * FROM sd_repl.proteins where id between {split[0]} and {split[1]}'
                else:
                    print(f'Range Atual recuperado: {current_range[0]} - {current_range[1]}')
                    sql = f'SELECT * FROM sd_repl.proteins where id between {current_range[0]} and {current_range[1]}'
                    ssl_socket.send(f"FaTo".encode())
            else:
                print(f'Range Atual recuperado: {current_range[0]} - {current_range[1]}')
                sql = f'SELECT * FROM sd_repl.proteins where id between {current_range[0]} and {current_range[1]}'
                ssl_socket.send(f"FaTo".encode())

        receive_mysql_dump_from_server(ssl_socket)
        # Réplica do banco de dados MySQL no cliente
        if(i==0):
            replicate_mysql_to_client(ssl_socket)
        ssl_socket.close()
        escrever_no_log('Conexao com o servidor fechada!')

        cursor.execute(sql)

        # Recuperar todos os resultados da consulta
        results = cursor.fetchall()

        # Iterar pelos resultados e fazer algo com eles
        for row in results:
            if(os.path.exists(f"arquivos/{row[2]}.pdb")):
                print(f"Arquivo {row[2]}.pdb existente")
                continue
            url = f"https://files.rcsb.org/download/{row[2]}.pdb"
            local_filename = f"arquivos/{row[2]}.pdb"
            print(f'Starting Download from {url}')
            escrever_no_log(f'Starting Download from {url}')
            response = requests.get(url)
            if response.status_code == 200:
                # Abra o arquivo local em modo de escrita binária
                with open(local_filename, "wb") as file:
                    # Escreva o conteúdo do arquivo baixado no arquivo local
                    file.write(response.content)
                print(f"Arquivo {local_filename} baixado com sucesso.")
                escrever_no_log(f"Arquivo {local_filename} baixado com sucesso.")
            else:
                print(
                    f"Falha ao baixar o arquivo. Código de status: {response.status_code}")
            
    proteins_ids = get_protein_ids_from_db()
    """ threads = []
    for protein in proteins_ids:
        superimposer_handler = threading.Thread(target=superimposer, args=(protein,))
        threads.append(superimposer_handler)
        #superimposer_handler.start()
        
        #superimposer(protein)
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join() """
    """ with ThreadPoolExecutor() as executor:
        executor.map(superimposer, proteins_ids) """
    for protein in proteins_ids:
        superimposer(protein)
    send_files()
    lista_arquivos = os.listdir('relatorios')
    quantidade_arquivos = len(lista_arquivos)
    if(quantidade_arquivos % 20 == 0):
        os.remove(state_file)
        escrever_no_log('State File removido com sucesso!')

i = 0
# Conecte-se ao servidor
while True:
    connect_to_server(i)
    diretorio = 'arquivos'
    arquivos = os.listdir(diretorio)
    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        os.remove(caminho_arquivo)
    print('Diretório Arquivos Limpo!')
    escrever_no_log('Diretório Arquivos Limpo!')
    diretorio = 'relatorios'
    arquivos = os.listdir(diretorio)
    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        os.remove(caminho_arquivo)
    print('Diretório relatórios Limpo!')
    escrever_no_log('Diretório Relatórios Limpo!')
    range_atual = extract_last_range_from_log('logfile.txt')
    escrever_no_log(f'Range {range_atual[0]} - {range_atual[1]} - Done')
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
            last_processed_index = state.get('last_processed_index', None)
    else:
        last_processed_index = None

    start_index = last_processed_index if last_processed_index is not None else 0
    i+=1




