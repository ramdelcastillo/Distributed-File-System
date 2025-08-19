import pika
import json
import os
import socket

class FileServer:
    def __init__(self):
        os.makedirs("MainServerStorage", exist_ok=True)
        self.storingAlgo = "rr" # greedy or thresholding
        self.currentRoundRobinServer = 1
        self.fileAndSizeMap = {}
        self.serverSizeMap = {
            '10.2.13.28': {'usedStorage': 0, 'capacity': 100 * 1048576, 'threshold': 1.0},  # 100MB
            '10.2.13.29': {'usedStorage': 0, 'capacity': 100 * 1048576, 'threshold': 1.0},
            '10.2.13.30': {'usedStorage': 0, 'capacity': 100 * 1048576, 'threshold': 1.0}
        }
        self.serverMapping = {
            1: '10.2.13.28',
            2: '10.2.13.29',
            3: '10.2.13.30'
        }
        self.rabbitMQHost = '10.2.13.29'
        self.serverIP = self.getLocalIPAddress()
        self.credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.rabbitMQHost, 8000, '/', self.credentials)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='fileOperationRequests')
        self.channel.queue_declare(queue=self.serverIP +'-handleFileOperations')
        self.channel.queue_declare(queue='sideServerUpdates')
        self.channel.basic_consume(queue='fileOperationRequests', on_message_callback=self.handleFileOperationRequests,auto_ack=True)
        self.channel.basic_consume(queue=self.serverIP +'-handleFileOperations', on_message_callback=self.handleFileOperations, auto_ack=True)
        self.channel.basic_consume(queue='sideServerUpdates', on_message_callback=self.handleSideServerUpdates, auto_ack=True)
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

    def setServerSize(self, serverIp, megaBytes):
        if serverIp in self.serverSizeMap:
            usedStorage = self.getUsedStorageForServer(serverIp) / 1048576
            if megaBytes >= usedStorage and megaBytes >= 0:
                self.serverSizeMap[serverIp]['capacity'] = megaBytes * 1048576
                return True
            else:
                print(f"Invalid size for {serverIp}.")
                return False
        else:
            print(f"Server {serverIp} not found.")
            return False

    def setThreshold(self, serverIp, threshold):
        if serverIp in self.serverSizeMap:
            currentStorageUtilization = self.getStorageUtilizationForServer(serverIp)
            if (threshold >= 0 and threshold <= 1) and threshold >= currentStorageUtilization:
                self.serverSizeMap[serverIp]['threshold'] = threshold
                return True
            else:
                print(f"Invalid threshold for {serverIp}. Must be >= to current threshold and within 0 and 100%.")
                return False
        else:
            print(f"Server {serverIp} not found.")
            return False

    def addFileToServer(self, fileName, fileSize, serverIp):
        if serverIp in self.serverSizeMap:
            self.fileAndSizeMap[fileName] = {
                'fileSize': fileSize,
                'server': serverIp
            }
            self.serverSizeMap[serverIp]['usedStorage'] += fileSize
        else:
            print(f"Server {serverIp} not recognized.")

    def deleteFileFromServer(self, fileName):
        if fileName in self.fileAndSizeMap:
            fileInfo = self.fileAndSizeMap[fileName]
            serverIp = fileInfo['server']
            fileSize = fileInfo['fileSize']
            if serverIp in self.serverSizeMap:
                self.serverSizeMap[serverIp]['usedStorage'] -= fileSize
            del self.fileAndSizeMap[fileName]
        else:
            print(f"File '{fileName}' not found.")

    def getUsedStorageForServer(self, serverIp):
        if serverIp in self.serverSizeMap:
            return self.serverSizeMap[serverIp]['usedStorage']
        else:
            print(f"Server {serverIp} not found.")
            return None

    def getCapacityForServer(self, serverIp):
        if serverIp in self.serverSizeMap:
            return self.serverSizeMap[serverIp]['capacity']
        else:
            print(f"Server {serverIp} not found.")
            return None

    def getStorageUtilizationForServer(self, serverIp):
        if serverIp in self.serverSizeMap:
            used = self.serverSizeMap[serverIp]['usedStorage']
            capacity = self.serverSizeMap[serverIp]['capacity']
            if capacity > 0:
                return used / capacity  # Returns a float between 0.0 and 1.0
            else:
                return 0.0
        else:
            print(f"Server {serverIp} not found.")
            return None

    def getThresholdForServer(self, serverIp):
        if serverIp in self.serverSizeMap:
            return self.serverSizeMap[serverIp]['threshold']
        else:
            print(f"Server {serverIp} not found.")
            return None

    def getNextAvailableRRServer(self, fileSize):
        start = self.currentRoundRobinServer
        checkServerCapacity = 0

        while checkServerCapacity < len(self.serverMapping):
            ip = self.serverMapping[self.currentRoundRobinServer]
            usedStorage = self.getUsedStorageForServer(ip)
            capacity = self.getCapacityForServer(ip)
            threshold = self.getThresholdForServer(ip)
            nextStorageUtilization = (usedStorage + fileSize) / capacity

            if usedStorage + fileSize <= capacity and nextStorageUtilization <= threshold:
                # Update pointer to next server for next time
                self.currentRoundRobinServer = (self.currentRoundRobinServer % 3) + 1
                return ip  # Server has enough space

            # Try next server
            self.currentRoundRobinServer = (self.currentRoundRobinServer % 3) + 1
            checkServerCapacity += 1

        return None  # No server had enough space

    def getFileAndSizeMap(self):
        return self.fileAndSizeMap

    def isFileAlreadyExistingInServer(self, fileName):
        return fileName in self.fileAndSizeMap

    def getServerSizeMap(self):
        return self.serverSizeMap

    def getWhereFileIsLocated(self, fileName):
        if fileName in self.fileAndSizeMap:
            return self.fileAndSizeMap[fileName]['server']
        else:
            return None

    def handleFileOperationRequests(self, ch, method, properties, body):
        fileOperationRequest = json.loads(body)
        fileOperation = fileOperationRequest['operation']
        clientIP = fileOperationRequest['clientIP']

        if fileOperation == 'saveFile':
            self.handleSaveFileRequest(fileOperationRequest)
        elif fileOperation == 'deleteFile':
            self.handleDeleteFileRequest(fileOperationRequest)
        elif fileOperation == 'getFile':
            self.handleGetFileRequest(fileOperationRequest)
        elif fileOperation in ['listFiles', 'setServerSize', 'setServerThreshold']:
            self.changeServerSettingsAndQueries(fileOperationRequest, clientIP, fileOperation)

    def handleSaveFileRequest(self, saveFileRequest):
        fileName = saveFileRequest['fileName']
        fileSize = saveFileRequest['fileSize']
        clientIP = saveFileRequest['clientIP']

        if not self.isFileAlreadyExistingInServer(fileName):
            if self.storingAlgo == "rr":
                targetServerIP = self.getNextAvailableRRServer(fileSize)
                if targetServerIP:
                    response = {'canBeStored': True, 'uploadTo': targetServerIP + '-handleFileOperations'}
                else:
                    response = {'canBeStored': False, 'rejectResponse': f"'{fileName}' cannot be uploaded to server due to insufficient space."}
                    print(f"'{fileName}' cannot be uploaded to server due to insufficient space.")

            self.channel.basic_publish(exchange='', routing_key=clientIP+'-checkFileSizeResponses', body=json.dumps(response))
        else:
            response = {'canBeStored': False, 'duplicateFile': True, 'rejectResponse': f"'{fileName}' already exists in server."}
            print(f"'{fileName}' already exists in server.")
            self.channel.basic_publish(exchange='', routing_key=clientIP+'-checkFileSizeResponses', body=json.dumps(response))

    def handleDeleteFileRequest(self, deleteFileRequest):
        fileName = deleteFileRequest['fileName']
        clientIP = deleteFileRequest['clientIP']

        if self.isFileAlreadyExistingInServer(fileName):
            fileIsLocatedAt = self.getWhereFileIsLocated(fileName)
            response = {'canBeDeleted': True, 'deleteIn': fileIsLocatedAt}

            self.channel.basic_publish(
                exchange='',
                routing_key=clientIP + '-deleteFileResponses',
                body=json.dumps(response)
            )
        else:
            response = {'canBeDeleted': False, 'rejectResponse': f'{fileName} does not exist in server.'}
            self.channel.basic_publish(
                exchange='',
                routing_key=clientIP + '-deleteFileResponses',
                body=json.dumps(response)
            )

            print(f"'{fileName}' does not exist in server.")

    def handleGetFileRequest(self, getFileRequest):
        fileName = getFileRequest['fileName']
        clientIP = getFileRequest['clientIP']

        if self.isFileAlreadyExistingInServer(fileName):
            fileIsLocatedAt = self.getWhereFileIsLocated(fileName)
            response = {'canBeRetrieved': True, 'getFrom': fileIsLocatedAt}

            self.channel.basic_publish(
                exchange='',
                routing_key=clientIP+'-getFileResponses',
                body=json.dumps(response)
            )
        else:
            response = {'canBeRetrieved': False, 'rejectResponse': f'{fileName} does not exist in server.'}
            self.channel.basic_publish(
                exchange='',
                routing_key=clientIP+'-getFileResponses',
                body=json.dumps(response)
            )

            print(f"'{fileName}' does not exist in server.")

    def handleFileOperations(self, ch, method, properties, body):
        fileOperation = json.loads(body)

        fileOperationType = fileOperation['operation']

        if fileOperationType == 'fileUpload':
            self.handleFileUpload(fileOperation)
        elif fileOperationType == 'fileDelete':
            self.handleFileDelete(fileOperation)
        elif fileOperationType == 'getFile':
            self.handleGetFile(fileOperation)

    def handleFileUpload(self, fileUpload):
        clientIP = fileUpload['clientIP']
        fileName = fileUpload['fileName']
        fileSize = fileUpload['fileSize']
        fileData = fileUpload['fileData'].encode('latin1')
        fileServer = fileUpload['fileServer']

        self.addFileToServer(fileName, fileSize, fileServer)

        usedStorage = self.getUsedStorageForServer(fileServer)
        capacity = self.getCapacityForServer(fileServer)

        with open(f"MainServerStorage/{fileName}", "wb") as f:
            f.write(fileData)

        print(f"[MAIN-{fileServer}] Stored '{fileName}' ({fileSize} bytes) from {clientIP}. Used: {usedStorage}/{capacity}")

    def handleFileDelete(self, fileDelete):
        clientIP = fileDelete['clientIP']
        fileName = fileDelete['fileName']
        fileServer = fileDelete['fileServer']

        os.remove(f"MainServerStorage/{fileName}")

        self.deleteFileFromServer(fileName)

        usedStorage = self.getUsedStorageForServer(fileServer)
        capacity = self.getCapacityForServer(fileServer)

        print(f"[MAIN-{fileServer}] Deleted '{fileName}' in {fileServer}. Used: {usedStorage}/{capacity}")

    def handleGetFile(self, getFile):
        clientIP = getFile['clientIP']
        fileName = getFile['fileName']
        fileServer = getFile['fileServer']

        with open(f"MainServerStorage/{fileName}", "rb") as f:
            fileData = f.read()

        fileSize = os.path.getsize(f"MainServerStorage/{fileName}")

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

        print(f"[MAIN-{fileServer}] Sent '{fileName}' ({fileSize} bytes) to {clientIP}.")

    def handleSideServerUpdates(self, ch, method, properties, body):
        sideServerStorageUpdate = json.loads(body)
        storageUpdate = sideServerStorageUpdate['sideServerStorageUpdate']

        if storageUpdate == 'upload':
            self.handleSideServerFileUploadUpdates(sideServerStorageUpdate)
        elif storageUpdate == 'delete':
            self.handleSideServerFileDeleteUpdates(sideServerStorageUpdate)

    def handleSideServerFileUploadUpdates(self, updateServerFileInformation):
        clientIP = updateServerFileInformation['clientIP']
        fileName = updateServerFileInformation['fileName']
        fileSize = updateServerFileInformation['fileSize']
        fileServer = updateServerFileInformation['fileServer']

        self.addFileToServer(fileName, fileSize, fileServer)

        usedStorage = self.getUsedStorageForServer(fileServer)
        capacity = self.getCapacityForServer(fileServer)

        print(f"[SIDE-{fileServer}] Stored '{fileName}' ({fileSize} bytes) from {clientIP}. Used: {usedStorage}/{capacity}")

    def handleSideServerFileDeleteUpdates(self, updateServerFileInformation):
        clientIP = updateServerFileInformation['clientIP']
        fileName = updateServerFileInformation['fileName']
        fileServer = updateServerFileInformation['fileServer']

        self.deleteFileFromServer(fileName)

        usedStorage = self.getUsedStorageForServer(fileServer)
        capacity = self.getCapacityForServer(fileServer)

        print(
            f"[SIDE-{fileServer}] Deleted '{fileName}' in {fileServer}. Used: {usedStorage}/{capacity}")

    def changeServerSettingsAndQueries(self, serverQuery, clientIP, query):
        if (query == "listFiles"):
            serverResponse = {
                'listOfFiles': self.getFileAndSizeMap(),
                'serverMap': self.getServerSizeMap()
            }

            self.channel.basic_publish(exchange='', routing_key=clientIP+'-serverResponseForSettingsAndQueries', body=json.dumps(serverResponse))
        elif (query == "setServerSize"):
            isServerSizeSet = self.setServerSize(serverQuery['fileServer'], serverQuery['size'])

            if isServerSizeSet:
                serverResponse = {
                    'resized': True,
                    'acceptResponse': f"'{serverQuery['fileServer']}' resized to {serverQuery['size']} megabytes.\n"
                }

                self.channel.basic_publish(exchange='', routing_key=clientIP+'-serverResponseForSettingsAndQueries',
                                           body=json.dumps(serverResponse))
                print(serverResponse['acceptResponse'])
            else:
                serverResponse = {
                    'resized': False,
                    'rejectResponse': f"'{serverQuery['fileServer']}' cannot be resized to {serverQuery['size']} megabytes.\n"
                }

                self.channel.basic_publish(exchange='', routing_key=clientIP+'-serverResponseForSettingsAndQueries',
                                           body=json.dumps(serverResponse))
                print(serverResponse['rejectResponse'])
        elif (query == "setServerThreshold"):
            isServerThresholdSet = self.setThreshold(serverQuery['fileServer'], serverQuery['threshold'])

            if isServerThresholdSet:
                serverResponse = {
                    'thresholdSet': True,
                    'acceptResponse': f"'{serverQuery['fileServer']}' threshold ratio set to {serverQuery['threshold']}.\n"
                }

                self.channel.basic_publish(exchange='', routing_key=clientIP+'-serverResponseForSettingsAndQueries',
                                           body=json.dumps(serverResponse))
                print(serverResponse['acceptResponse'])
            else:
                serverResponse = {
                    'thresholdSet': False,
                    'rejectResponse': f"'{serverQuery['fileServer']}' threshold ratio cannot be set to {serverQuery['threshold']}.\n"
                }

                self.channel.basic_publish(exchange='', routing_key=clientIP+'-serverResponseForSettingsAndQueries',
                                           body=json.dumps(serverResponse))
                print(serverResponse['rejectResponse'])


if __name__ == '__main__':
    server = FileServer()

