
import socket
import os
import platform
import subprocess
import winreg as reg
import shutil
from concurrent.futures import ThreadPoolExecutor
import json


subprocess.call(
    ['mkdir', 'arquivos'], shell=True)
subprocess.call(
    ['mkdir', 'referencias'], shell=True)
subprocess.call(
    ['mkdir', 'relatorios'], shell=True)
subprocess.call(
    ['pip', 'install', 'biopython'], shell=True)

subprocess.call(
    ['pip', 'install', 'mysql-connector-python'], shell=True)

subprocess.call(
    ['pip', 'install', 'requests'], shell=True)
server_address = ('172.21.11.45', 9999)
from Bio.PDB import PDBParser, PDBIO, Superimposer
import mysql.connector
import requests

# Configurar as informações de conexão
config = {
    'user': 'root',
    'password': '',
    'host': 'localhost'
}
def is_chocolatey_installed():
    try:
        subprocess.call(['powershell.exe', '-Command', r'[System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH;C:\ProgramData\chocolatey\bin", [System.EnvironmentVariableTarget]::Machine)'])

        subprocess.check_output(['choco', '--version'],
                                stderr=subprocess.STDOUT, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


def install_chocolatey():
    if platform.system() == 'Windows':
        print("Instalando Chocolatey...")

        try:
            # Instala o Chocolatey usando o PowerShell
            subprocess.run([
                'powershell.exe',
                '-NoProfile',
                '-ExecutionPolicy', 'Bypass',
                '-Command', 
                'iex ((New-Object System.Net.WebClient).DownloadString("https://chocolatey.org/install.ps1"))'
            ], check=True)

            print("Chocolatey foi instalado com sucesso.")

        except subprocess.CalledProcessError as e:
            print(f"Falha ao instalar o Chocolatey: {e}")

state_file = 'state.json'
if is_chocolatey_installed():
    print("Chocolatey já está instalado no cliente.")
# Se não tiver, instale usando Chocolatey ou outro método
else:
    install_chocolatey()

try:
    # Tente executar o comando mysql no terminal
    subprocess.check_output(['mysql', '--version'])
    print("MySQL já está instalado no cliente.")

except FileNotFoundError:
    print("MySQL não está instalado no cliente. Instalando...")
    try:
        if platform.system() == 'Windows':
            # Instale o MySQL Server usando o Chocolatey.
            subprocess.run(['choco', 'install', 'mysql', '--yes'], check=True)
            mysql_bin_path = r'C:\Program Files\MySQL\MySQL Server\8.0\bin'  # Substitua pelo caminho correto
            os.environ['PATH'] += os.pathsep + mysql_bin_path

            print("MySQL foi instalado com sucesso no cliente.")
        else:
            subprocess.run(['sudo', 'apt', 'install', 'mysql-server', '-y'], check=True)
    except subprocess.CalledProcessError:
        print("Falha ao instalar o MySQL usando o Chocolatey.")
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
    
# Selecionar o banco de dados recém-criado

if os.path.exists(state_file):
    with open(state_file, 'r') as f:
        state = json.load(f)
        last_processed_index = state.get('last_processed_index', None)
else:
    last_processed_index = None

pdb_directory = "arquivos"
# Obtenha a lista de arquivos PDB


# Determine o índice de início do loop
start_index = last_processed_index if last_processed_index is not None else 0

from Bio.PDB import PDBParser, PDBIO, Superimposer
parser = PDBParser(QUIET=True)
pdb_directory = "arquivos"

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


def connect_to_server(i):
    target_host = "172.21.11.45"  # Substitua pelo IP do servidor
    target_port = 9999
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((target_host, target_port))
    if(last_processed_index == None):
        client.send(f"Initiating".encode())
        response = client.recv(1024)
        range = (response.decode())
        split = range.split('-')
        print(split[0], split[1])
        # Réplica do banco de dados MySQL no cliente
        if(i==0):
            receive_mysql_dump_from_server(client)
            replicate_mysql_to_client(client)
        client.close()
   
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
    if(quantidade_arquivos == 20):
        os.remove(state_file)
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
            with socket.create_connection(('172.21.11.45', 9999)) as client_socket:
                # Envie a flag indicando que um arquivo será enviado
                client_socket.sendall(b'file')
                # Envie o nome do arquivo
                client_socket.sendall(f"{output_pdb_filename}\n".encode())

                # Envie os dados do arquivo
                with open(output_pdb_filename, 'rb') as file:
                    while True:
                        data = file.read(1024)
                        if not data:
                            break
                        client_socket.sendall(data)
                print(f"Arquivo {output_pdb_filename} enviado com sucesso.")
        with open(state_file, 'w') as f:
            json.dump(state, f)
            # Enviar o arquivo e o nome de volta para o servidor por socket
            """ with open(output_pdb_filename, "rb") as file:
                file_data = file.read()
                file_name_data = json.dumps({"file_name": output_pdb_filename}).encode()

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(server_address)
                    s.send(f"file".encode())
                    s.sendall(file_name_data)
                    s.sendall(file_data)
                    s.close() """
    #send_files_over_socket(server_address[0], server_address[1], 'relatorios')
# Função para replicar o MySQL no cliente


def replicate_mysql_to_client(client):

    """ # Verifique se o cliente tem o MySQL instalado
    if is_chocolatey_installed():
        print("Chocolatey já está instalado no cliente.")
    # Se não tiver, instale usando Chocolatey ou outro método
    else:
        install_chocolatey() """

    """ try:
        # Tente executar o comando mysql no terminal
        subprocess.check_output(['mysql', '--version'])
        print("MySQL já está instalado no cliente.")

    except FileNotFoundError:
        print("MySQL não está instalado no cliente. Instalando...")
        try:
            # Instale o MySQL Server usando o Chocolatey.
            subprocess.run(['choco', 'install', 'mysql', '--yes'], check=True)
            mysql_bin_path = r'C:\Program Files\MySQL\MySQL Server\8.0\bin'  # Substitua pelo caminho correto
            os.environ['PATH'] += os.pathsep + mysql_bin_path

            print("MySQL foi instalado com sucesso no cliente.")
        except subprocess.CalledProcessError:
            print("Falha ao instalar o MySQL usando o Chocolatey.") """

    # Comandos para replicar o banco de dados MySQL
    # Substitua-os pelos comandos reais para exportar e importar o banco de dados

    import_command = "mysql -u root sd_repl < received_dump.sql"

    # Execute os comandos no cliente
    print("Replicando banco de dados MySQL...")
    
    os.system(import_command)  # Execute o comando de importação
    print('Replicação bem sucedida!')
    

def send_files_over_socket(host, port, directory_path):
    with socket.create_connection((host, port)) as client_socket:
        # Obtenha a lista de arquivos no diretório
        file_list = os.listdir(directory_path)
        client_socket.send(b'file')
        # Envie o número total de arquivos
        client_socket.sendall(str(len(file_list)).encode())

        # Agora, envie cada arquivo individualmente
        for file_name in file_list:
            file_path = os.path.join(directory_path, file_name)

            with open(file_path, 'rb') as file:
                # Envie o nome do arquivo seguido pelos dados do arquivo
                client_socket.sendall(f"{file_name}\n".encode())
                client_socket.sendall(file.read())
            print(f"Arquivo {file_name} enviado com sucesso.")
        client_socket.close()


i = 0
# Conecte-se ao servidor
while True:
    connect_to_server(i)
    i+=1