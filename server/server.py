import socket
import ssl
import threading
import os
import subprocess
import time
import re
# Funcao para atender clientes individualmente
subprocess.call(
    ['mkdir', 'relatorios'], shell=True)
# Configuracões do servidor
HOST = '0.0.0.0'
PORT = 12345
HOST_REPL = 'localhost'
PORT_REPL = 12346
CERTIFICATE_FILE = 'server-cert.pem'  # Certificado do servidor
PRIVATE_KEY_FILE = 'server-key.pem'  # Chave privada do servidor
CLIENT_CERT = 'client-cert.pem' # Chave client
def escrever_no_log(mensagem, nome_arquivo='logfile.txt'):
    hora_atual = time.strftime('%Y-%m-%d %H:%M:%S')
    mensagem_com_hora = f'{hora_atual} - {mensagem}'
    with open(nome_arquivo, 'a') as arquivo:
        arquivo.write(f'{mensagem_com_hora}\n')
# Criacao do soquete
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Configuracao do contexto SSL
ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
ssl_context.load_cert_chain(certfile=CERTIFICATE_FILE, keyfile=PRIVATE_KEY_FILE)

# Vincula o soquete ao endereco e porta especificados
server_socket.bind((HOST, PORT))
server_socket.listen()
def find_client(clients, target_addr):
    for client_socket, addr in clients:
        if addr == target_addr:
            return client_socket
    return None  # Retorna None se o cliente nao for encontrado na lista
def replicate_mysql_to_client(client_socket):
    export_command = "mysqldump --skip-column-statistics --skip-comments -u root sd > dump.sql"

    # Execute os comandos no cliente
    os.system(export_command)  # Execute o comando de exportacao
    escrever_no_log('Arquivo MySQL exportado com sucesso!')
    send_mysql_dump_to_client(client_socket)
def send_mysql_dump_to_client(client_socket):
    dump_file = "dump.sql"
    try:
        with open(dump_file, "rb") as file:
            while True:
                data = file.read(1024)
                if not data:
                    break
                client_socket.sendall(data)
            client_socket.sendall(b'Finalizado')
            print("[*] Arquivo MySQL dump enviado para o cliente")
            escrever_no_log("[*] Arquivo MySQL dump enviado para o cliente")

    except Exception as e:
        print(f"[*] Erro ao enviar arquivo: {e}")
def handle_client(client_socket,client_address,message,clients):
    if message == 'Init':
        # O servidor atribuirá um intervalo de 10 números para cada cliente
        client_id = len(clients) * 10 - 9
        start = client_id
        end = start + 9
        """ for i in range(len(clients)):
            if(clients[i] == client_socket):
                clients[i] = [client_socket, ([f"{start} - {end}"])] """
        # Envie o intervalo atribuído para o cliente
        escrever_no_log(f'Cliente {client_address} -> Faixa: {start} - {end}')
        client_socket.send(f"{start}-{end}".encode())

    # Réplica do banco de dados MySQL no cliente
    replicate_mysql_to_client(client_socket)

print(f"Servidor seguro iniciado em {HOST}:{PORT}")
escrever_no_log(f"Servidor seguro iniciado em {HOST}:{PORT}")
def extrair_clientes_do_log(nome_arquivo='logfile.txt'):
    clients = []

    with open(nome_arquivo, 'r') as arquivo:
        linhas = arquivo.readlines()

    # Padrão de expressão regular ajustado para lidar com parênteses e vírgulas
    padrao = r"Cliente \('\d+\.\d+\.\d+\.\d+', \d+\) -> Faixa: (\d+) - (\d+)"

    for linha in linhas:
        correspondencia = re.search(padrao, linha)
        if correspondencia:
            start = int(correspondencia.group(1))
            end = int(correspondencia.group(2))
            clients.append({'start': start, 'end': end})

    return clients

def replicate_logfile():
     # Criacao do soquete
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Configuracao do contexto SSL
    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CLIENT_CERT)
    # Configuracao do contexto SSL com verificacao de certificado desativada
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    # Conectar-se ao servidor
    ssl_socket = ssl_context.wrap_socket(client_socket, server_hostname=HOST_REPL)
    ssl_socket.connect((HOST_REPL, PORT_REPL))
    ssl_socket.sendall('Repl'.encode())
    with open('logfile.txt', 'rb') as file:
            dados = file.read(1024)
            while dados:
                ssl_socket.send(dados)
                dados = file.read(1024)
    ssl_socket.close()

""" def replicate_reports():
   
    relatorios_directory = 'relatorios'
    lista_arquivos = os.listdir(relatorios_directory)


    for file_name in lista_arquivos:
        print(file_name)
         # Criacao do soquete
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Configuracao do contexto SSL
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=CERTIFICATE_FILE)
        # Configuracao do contexto SSL com verificacao de certificado desativada
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        # Conectar-se ao servidor
        ssl_socket = ssl_context.wrap_socket(client_socket, server_hostname=HOST_REPL)
        ssl_socket.connect((HOST_REPL, PORT_REPL))
        ssl_socket.sendall(b'ReFi')
        file_path = os.path.join(relatorios_directory, file_name)
        ssl_socket.sendall(f"{file_path}\n".encode())
        with open(file_path, 'rb') as file:
            while True:
                data = file.read(1024)
                if not data:
                    break
                ssl_socket.sendall(data)
            ssl_socket.sendall(b'Finalizado')
        ssl_socket.close() """
# Exemplo de uso para extrair clientes do arquivo de log
while True:
    clients = extrair_clientes_do_log()
    # Aceita a conexao do cliente
    client_socket, client_address = server_socket.accept()
    # Adiciona a camada SSL/TLS ao soquete
    ssl_socket = ssl_context.wrap_socket(client_socket, server_side=True)
    escrever_no_log(f'Conexao aceita de {client_address}')
    response = ssl_socket.recv(4)
    message = response.decode().strip()
    if(message == 'Init'):
        escrever_no_log('Flag Init recebida!')
        clients.append(client_socket)
        client_handler = threading.Thread(target=handle_client, args=(ssl_socket,client_address,message,clients))
        client_handler.start()
    elif('file' in message):
        escrever_no_log('Flag File recebida!')
        """

        file_name = ssl_socket.recv(35).decode().strip()
        print('filename: ', file_name)
        escrever_no_log(f'Arquivo {file_name} criado!')

        file_data = ssl_socket.recv(1024)
        # Salve o arquivo no servidor
        with open(file_name, 'wb') as file:
            while True:
                data = ssl_socket.recv(1024)
                if not data:
                    break
                file.write(data)

        print(f"Arquivo {file_name} recebido com sucesso.")
        """
        arquivos_str = ssl_socket.recv(1024).decode()
        arquivos = arquivos_str.split("\n")
        diretorio_destino = "relatorios"
        for arquivo in arquivos:
            with open(os.path.join(diretorio_destino, arquivo), 'wb') as file:
                dados = ssl_socket.recv(1024)
                while dados:
                    file.write(dados)
                    dados = ssl_socket.recv(1024)
            escrever_no_log(f'Arquivo {arquivo} recebido com sucesso')
    elif('FaTo' in message):
        client_handler = threading.Thread(target=handle_client, args=(ssl_socket,client_address,message,clients))
        client_handler.start()
    elif('Repl' in message):
       with open('logfile.txt', 'wb') as file:
            while True:
                data = ssl_socket.recv(1024)
                if b"Finalizado" in data:
                    file.write(data.replace(b"Finalizado", b""))
                    break
                file.write(data)
    replicate_logfile()
    
    #replicate_logfile_handler.join()
