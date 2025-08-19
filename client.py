import pika
import json
import os
import socket
import re
import threading
import time
from io import StringIO

class FileClient:
    def __init__(self):
        os.makedirs("ClientStorage", exist_ok=True)
        self.rabbitMQHost = '10.2.13.29'
        self.serverMapping = {
            1: '10.2.13.28',
            2: '10.2.13.29',
            3: '10.2.13.30'
        }
        self.clientIP =  self.getLocalIPAddress()
        self.credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('ccscloud.dlsu.edu.ph', 21529, '/', self.credentials)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='fileOperationRequests')
        self.channel.queue_declare(queue=self.clientIP+'-checkFileSizeResponses')
        self.channel.queue_declare(queue=self.clientIP+'-deleteFileResponses')
        self.channel.queue_declare(queue=self.clientIP+'-serverResponseForSettingsAndQueries')
        self.channel.queue_declare(queue=self.clientIP+'-getFileResponses')
        self.getQueue = self.clientIP + '-recieve'
        self.channel.queue_declare(queue=self.getQueue)
        threading.Thread(target=self.start_polling_retrieved_files, daemon=True).start()

    def start_polling_retrieved_files(self):
        poll_connection = pika.BlockingConnection(
            pika.ConnectionParameters('ccscloud.dlsu.edu.ph', 21529, '/', self.credentials)
        )
        poll_channel = poll_connection.channel()
        poll_channel.queue_declare(queue=self.getQueue)

        while True:
            method, header, body = poll_channel.basic_get(queue=self.getQueue, auto_ack=True)
            if method:
                self.handleFileRetrieve(None, None, None, body)
            time.sleep(1)

    def getLocalIPAddress(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))  # Connect to a public IP (Google DNS)
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    def extractIP(self, ip: str) -> str:
        match = re.match(r'^(\d{1,3}(?:\.\d{1,3}){3})-', ip)
        if match:
            return match.group(1)
        return None

    def saveFile(self, fileName):
        if not os.path.exists(fileName):
            print("File not found.")
            return

        fileSize = os.path.getsize(fileName)

        checkFileSizeRequest = {
            'clientIP': self.clientIP,
            'fileName': fileName,
            'fileSize': fileSize,
            'operation': 'saveFile'
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(checkFileSizeRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-checkFileSizeResponses', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-checkFileSizeResponses', auto_ack=True)

        response = json.loads(body)

        if response['canBeStored']:
            with open(fileName, 'rb') as f:
                fileData = f.read()

            fileServer = self.extractIP(response['uploadTo'])

            fileUpload = {
                'clientIP': self.clientIP,
                'fileName': fileName,
                'fileSize': fileSize,
                'fileData': fileData.decode('latin1'),
                'fileServer': fileServer,
                'operation': 'fileUpload'
            }

            self.channel.queue_declare(queue=response['uploadTo'])

            self.channel.basic_publish(
                exchange='',
                routing_key=response['uploadTo'],
                body=json.dumps(fileUpload)
            )

            print(f"[CLIENT] Saved file '{fileName}' ({fileSize} bytes) to {fileServer}\n")
        elif not response['canBeStored']:
            print(f"[CLIENT] Upload rejected: {response['rejectResponse']}\n")
        elif response['duplicateFile']:
            print(f"[CLIENT] Upload rejected: {response['rejectResponse']}\n")
        else:
            print(f"[CLIENT] Upload rejected: {response['rejectResponse']}\n")

    def deleteFile(self, fileName):
        deleteFileRequest = {
            'clientIP': self.clientIP,
            'fileName': fileName,
            'operation': 'deleteFile'
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(deleteFileRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-deleteFileResponses', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-deleteFileResponses', auto_ack=True)

        response = json.loads(body)

        if response['canBeDeleted']:
            deleteInServer = response['deleteIn'] + "-handleFileOperations"
            fileDelete = {
                'clientIP': self.clientIP,
                'fileName': fileName,
                'fileServer': response['deleteIn'],
                'operation': 'fileDelete'
            }

            self.channel.basic_publish(
                exchange='',
                routing_key=deleteInServer,
                body=json.dumps(fileDelete)
            )
            print(f"[CLIENT] Deleted file '{fileName}' in {response['deleteIn']}\n")
        else:
            print(f"[CLIENT] Delete rejected: {response['rejectResponse']}\n")

    def getFile(self, fileName):
        getFileRequest = {
            'clientIP': self.clientIP,
            'fileName': fileName,
            'operation': 'getFile'
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(getFileRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-getFileResponses', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-getFileResponses', auto_ack=True)

        response = json.loads(body)

        if response['canBeRetrieved']:
            getFromServer = response['getFrom'] + "-handleFileOperations"

            getFile = {
                'clientIP': self.clientIP,
                'fileName': fileName,
                'fileServer': response['getFrom'],
                'operation': 'getFile'
            }

            self.channel.basic_publish(
                exchange='',
                routing_key=getFromServer,
                body=json.dumps(getFile)
            )
        else:
            print(f"[CLIENT] File unretrieveble: {response['rejectResponse']}\n")
    def listFiles(self):
        getListOfFilesRequest = {
            'clientIP': self.clientIP,
            'operation': "listFiles"
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(getListOfFilesRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries', auto_ack=True)

        response = json.loads(body)

        listOfFiles = response['listOfFiles']
        serverMap = response['serverMap']

        output = StringIO()

        output.write("Server Stats: \n")
        for ip, info in serverMap.items():
            used = info['usedStorage']
            capacity = info['capacity']
            threshold = info.get('threshold', 1)
            output.write(
                f"{ip}: {{ 'usedStorage': {used}, 'capacity': {capacity}, 'threshold': {threshold} }}\n"
            )
        output.write("\nFile List: \n")
        if listOfFiles:
            for fileName, fileInfo in listOfFiles.items():
                output.write(
                    f"{fileName}: {{ 'fileSize': {fileInfo['fileSize']}, 'server': '{fileInfo['server']}' }}\n")
        else:
            output.write("No files in the server\n")

        print(output.getvalue())

    def setServerStorageSize(self, serverIp, megaBytes):
        setServerSizeRequest = {
            'clientIP': self.clientIP,
            'fileServer': serverIp,
            'size': megaBytes,
            'operation': "setServerSize"
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(setServerSizeRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries', auto_ack=True)

        response = json.loads(body)

        if response['resized']:
            print(response['acceptResponse'])
        else:
            print(response['rejectResponse'])

    def setServerThresholdValue(self, serverIp, threshold):
        setServerThresholdRequest = {
            'clientIP': self.clientIP,
            'fileServer': serverIp,
            'threshold': threshold,
            'operation': "setServerThreshold"
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='fileOperationRequests',
            body=json.dumps(setServerThresholdRequest)
        )

        method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries', auto_ack=True)
        while not method_frame:
            method_frame, header, body = self.channel.basic_get(queue=self.clientIP + '-serverResponseForSettingsAndQueries',
                                                                auto_ack=True)

        response = json.loads(body)

        if response['thresholdSet']:
            print(response['acceptResponse'])
        else:
            print(response['rejectResponse'])

    def handleFileRetrieve(self, ch, method, properties, body):
        receivedFile = json.loads(body)
        clientIP = receivedFile['clientIP']
        fileName = receivedFile['fileName']
        fileSize = receivedFile['fileSize']
        fileData = receivedFile['fileData'].encode('latin1')
        fileServer = receivedFile['fileServer']

        with open(f"ClientStorage/{fileName}", "wb") as f:
            f.write(fileData)

        print(f"[CLIENT] Received file '{fileName}' ({fileSize} bytes) from {fileServer}\n")

    def closeConnection(self):
        self.connection.close()

    def commandTokenizer(self, command):
        return command.strip().split()

if __name__ == '__main__':
    client = FileClient()

    try:
        while True:
            userInput = input("Enter file operation: ").strip()
            fileOperation = client.commandTokenizer(userInput)

            if len(fileOperation) == 0:
                continue
            elif len(fileOperation) == 1:
                command = fileOperation[0].lower()
                if command == 'exit':
                    break
                elif command == "list":
                    client.listFiles()
                else:
                    print("File operation not found.\n")
            elif len(fileOperation) == 2:
                command = fileOperation[0].upper()
                fileName = fileOperation[1]
                if command == "SAVE":
                    client.saveFile(fileName)
                elif command == "DELETE":
                    client.deleteFile(fileName)
                elif command == "GET":
                    client.getFile(fileName)
                    time.sleep(1)
                else:
                    print("File operation not found.\n")
            elif len(fileOperation) == 3:
                command = fileOperation[0].upper()
                serverPart = fileOperation[1]
                valuePart = fileOperation[2]

                try:
                    serverNumber = int(serverPart)
                except ValueError:
                    print("Server number must be an integer.\n")
                    continue

                if not (1 <= serverNumber <= 3):
                    print("Server number must be between 1 and 3.\n")
                    continue
                if command == "SIZE":
                    try:
                        intSize = int(valuePart)
                        if intSize >= 0:
                            client.setServerStorageSize(client.serverMapping[serverNumber], intSize)
                        else:
                            print("Size must be a non-negative integer.\n")
                    except ValueError:
                        print("Size must be an integer.\n")
                elif command == "THOLD":
                    try:
                        floatThreshold = float(valuePart)
                        if 0 <= floatThreshold <= 1:
                            client.setServerThresholdValue(client.serverMapping[serverNumber], floatThreshold)
                        else:
                            print("Threshold must be between 0 and 1.\n")
                    except ValueError:
                        print("Threshold must be a number.\n")
                else:
                    print("File operation not found.\n")
    finally:
        client.closeConnection()



