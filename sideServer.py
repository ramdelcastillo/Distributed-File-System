import pika
import json
import os
import socket

class SideFileServer:
    def __init__(self):
        os.makedirs("SideServerStorage", exist_ok=True)
        self.rabbitMQHost = '10.2.13.29'
        self.serverIP = self.getLocalIPAddress()
        self.credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.rabbitMQHost, 8000, '/', self.credentials)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.serverIP+'-handleFileOperations')
        self.channel.queue_declare(queue='sideServerUpdates')
        self.channel.basic_consume(queue=self.serverIP+'-handleFileOperations', on_message_callback=self.handleFileOperations, auto_ack=True)
        self.channel.start_consuming()
    def getLocalIPAddress(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))  # Connect to a public IP (Google DNS)
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    def handleFileUpload(self, fileUpload):
        clientIP = fileUpload['clientIP']
        fileName = fileUpload['fileName']
        fileSize = fileUpload['fileSize']
        fileData = fileUpload['fileData'].encode('latin1')
        fileServer = fileUpload['fileServer']

        with open(f"SideServerStorage/{fileName}", "wb") as f:
            f.write(fileData)

        print(f"[SIDE-{fileServer}] Stored '{fileName}' ({fileSize} bytes) from {clientIP}.")

        updateServerFilesInformation = {
            'clientIP': clientIP,
            'fileName': fileName,
            'fileSize': fileSize,
            'fileServer': fileServer,
            'sideServerStorageUpdate': 'upload'
        }

        self.channel.basic_publish(exchange='', routing_key='sideServerUpdates', body=json.dumps(updateServerFilesInformation))

    def handleFileDelete(self, fileDelete):
        clientIP = fileDelete['clientIP']
        fileName = fileDelete['fileName']
        fileServer = fileDelete['fileServer']

        os.remove(f"SideServerStorage/{fileName}")

        print(f"[SIDE-{fileServer}] Deleted '{fileName}' by {clientIP}.")

        updateServerFilesInformation = {
            'fileName': fileName,
            'fileServer': fileServer,
            'clientIP': clientIP,
            'sideServerStorageUpdate': 'delete'
        }

        self.channel.basic_publish(exchange='', routing_key='sideServerUpdates', body=json.dumps(updateServerFilesInformation))

    def handleGetFile(self, getFile):
        clientIP = getFile['clientIP']
        fileName = getFile['fileName']
        fileServer = getFile['fileServer']

        with open(f"SideServerStorage/{fileName}", "rb") as f:
            fileData = f.read()

        fileSize = os.path.getsize(f"SideServerStorage/{fileName}")

        retrieveFile = {
            'clientIP': clientIP,
            'fileName': fileName,
            'fileSize': fileSize,
            'fileData': fileData.decode('latin1'),
            'fileServer': fileServer
        }

        sendToClient = clientIP + "-recieve"

        self.channel.queue_declare(queue=sendToClient)

        self.channel.basic_publish(
            exchange='',
            routing_key=sendToClient,
            body=json.dumps(retrieveFile)
        )

        print(f"[SIDE-{fileServer}] Sent '{fileName}' ({fileSize} bytes) to {clientIP}.")

    def handleFileOperations(self, ch, method, properties, body):
        fileOperation = json.loads(body)

        fileOperationType = fileOperation['operation']

        if fileOperationType == 'fileUpload':
            self.handleFileUpload(fileOperation)
        elif fileOperationType == 'fileDelete':
            self.handleFileDelete(fileOperation)
        elif fileOperationType == 'getFile':
            self.handleGetFile(fileOperation)



if __name__ == '__main__':
    server = SideFileServer()
