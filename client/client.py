import socket
import ssl
import os
import platform
import subprocess
import json
from Bio.PDB import PDBParser, PDBIO, Superimposer
import mysql.connector
import requests
subprocess.call(
    ['mkdir', 'arquivos'], shell=True)
subprocess.call(
    ['mkdir', 'referencias'], shell=True)
subprocess.call(
    ['mkdir', 'relatorios'], shell=True)

# Configurações do cliente
HOST = '172.16.124.240'
PORT = 12345
ca_cert = 'client-cert.pem'
state_file = 'state.json'
# Configurar as informações de conexão
config = {
    'user': 'root',
    'password': '',
    'host': 'localhost'
}
connection = mysql.connector.connect(**config)
cursor = connection.cursor()
if connection.is_connected():
    print("Conexão ao banco de dados bem-sucedida.")
    nome_banco_dados = "sd_repl"
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(nome_banco_dados))
    try:
        cursor.execute("USE {}".format(nome_banco_dados))
        print("Banco de dados alterado com sucesso.")
        print("Após a mudança para o banco de dados:", connection.database)
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
parser = PDBParser(QUIET=True)
pdb_directory = "arquivos"

def replicate_mysql_to_client(client):
    import_command = "mysql -u root sd_repl < received_dump.sql"
    # Execute os comandos no cliente
    print("Replicando banco de dados MySQL...")
    
    os.system(import_command)  # Execute o comando de importação
    print('Replicação bem sucedida!')

def receive_mysql_dump_from_server(client):
    dump_file = "received_dump.sql"
    try:
        with open(dump_file, "wb") as file:
            while True:
                data = client.recv(1024)
                if b"Finalizado" in data:
                    file.write(data.replace(b"Finalizado", b""))
                    print("[*] Arquivo MySQL dump recebido do servidor")
                    break  # Saia do loop quando receber o marcador "Finalizado"
                file.write(data)
    except Exception as e:
        print(f"[*] Erro ao receber arquivo: {e}")
    finally:
        file.close()  # Certifique-se de fechar o arquivo após a escrita
# Função para buscar IDs de proteínas no banco de dados
def get_protein_ids_from_db():
    try:
        cursor.execute("SELECT cod_id FROM sd_repl.proteins limit 2")
        protein_ids = [str(row[0]) for row in cursor.fetchall()]
        return protein_ids
    except Exception as e:
        print(f"Erro ao buscar IDs de proteínas no banco de dados: {e}")
        return []

def superimposer(protein):
    url = f"https://files.rcsb.org/download/{protein}.pdb"
    local_filename = f"referencias/{protein}.pdb"
    print(f'Starting Reference Download from {url}')
    response = requests.get(url)
    if response.status_code == 200:
        # Abra o arquivo local em modo de escrita binária
        with open(local_filename, "wb") as file:
            # Escreva o conteúdo do arquivo baixado no arquivo local
            file.write(response.content)
        print(f"Arquivo {local_filename} baixado com sucesso.")
    else:
        print(
            f"Falha ao baixar o arquivo. Código de status: {response.status_code}")
    reference_structure = parser.get_structure(protein, f"referencias/{protein}.pdb")
    pdb_files = os.listdir(pdb_directory)
    for pdb_filename in pdb_files[start_index:]:
        # Atualize o estado com o índice do último arquivo processado
        last_processed_index = pdb_files.index(pdb_filename)
        state = {'last_processed_index': last_processed_index}
        print(state)
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

            # Realize a sobreposição
            superimposer.apply(target_structure)
            output_pdb_filename = f"relatorios/superposed_{protein}+{pdb_filename}"
            io = PDBIO()
            io.set_structure(target_structure)
            io.save(output_pdb_filename)

            print(f"Proteínas superpostas e salvas em {output_pdb_filename}")

            # Criação do soquete
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Configuração do contexto SSL
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
            # Configuração do contexto SSL com verificação de certificado desativada
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            # Conectar-se ao servidor
            ssl_socket = ssl_context.wrap_socket(client_socket, server_hostname=HOST)
            ssl_socket.connect((HOST, PORT))





            
            # Envie a flag indicando que um arquivo será enviado
            ssl_socket.sendall(b'file')
            # Envie o nome do arquivo
            ssl_socket.sendall(f"{output_pdb_filename}\n".encode())

            # Envie os dados do arquivo
            with open(output_pdb_filename, 'rb') as file:
                while True:
                    data = file.read(1024)
                    if not data:
                        break
                    ssl_socket.sendall(data)
            print(f"Arquivo {output_pdb_filename} enviado com sucesso.")
        with open(state_file, 'w') as f:
            json.dump(state, f)


def connect_to_server(i):   
    # Criação do soquete
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Configuração do contexto SSL
    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    # Configuração do contexto SSL com verificação de certificado desativada
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    # Conectar-se ao servidor
    ssl_socket = ssl_context.wrap_socket(client_socket, server_hostname=HOST)
    ssl_socket.connect((HOST, PORT))
    if(last_processed_index == None):
        ssl_socket.send(f"Initiating".encode())
        response = ssl_socket.recv(1024)
        range = (response.decode())
        split = range.split('-')
        print(split[0], split[1])
        # Réplica do banco de dados MySQL no cliente
        if(i==0):
            receive_mysql_dump_from_server(ssl_socket)
            replicate_mysql_to_client(ssl_socket)
        ssl_socket.close()
   
        sql = f'SELECT * FROM sd_repl.proteins where id between {split[0]} and {split[1]}'

        cursor.execute(sql)

        # Recuperar todos os resultados da consulta
        results = cursor.fetchall()

        # Iterar pelos resultados e fazer algo com eles
        for row in results:
            url = f"https://files.rcsb.org/download/{row[2]}.pdb"
            local_filename = f"arquivos/{row[2]}.pdb"
            print(f'Starting Download from {url}')
            response = requests.get(url)
            if response.status_code == 200:
                # Abra o arquivo local em modo de escrita binária
                with open(local_filename, "wb") as file:
                    # Escreva o conteúdo do arquivo baixado no arquivo local
                    file.write(response.content)
                print(f"Arquivo {local_filename} baixado com sucesso.")
            else:
                print(
                    f"Falha ao baixar o arquivo. Código de status: {response.status_code}")
            
    proteins_ids = get_protein_ids_from_db()

    for protein in proteins_ids:
        superimposer(protein)
    lista_arquivos = os.listdir('relatorios')
    quantidade_arquivos = len(lista_arquivos)
    print(quantidade_arquivos)
    if(quantidade_arquivos % 20 == 0):
        os.remove(state_file)

i = 0
# Conecte-se ao servidor
while True:
    connect_to_server(i)
    i+=1




