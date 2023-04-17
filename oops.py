import os
from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from json import dumps
import json

class App:
    
    bootstrap_servers = ['65.0.129.155:9092']

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, #change ip here
                            value_serializer=lambda x:
                            dumps(x).encode('utf-8'))
        self.list_file_path = self.get_all_files()
        self.topics = self.get_all_topics()
        self.dictionary = self.get_list()[0]
        self.list_2d = self.get_list()[1]
        print(self.topics)


    def get_all_files(self):
        folder_path = 'yogesh'  # replace with the path to your folder
        files = os.listdir(folder_path)
        list_file_patah = []
        for file_name in files:
            if file_name.endswith('.json'):  # check if file has .py extension
                file_path = os.path.join(folder_path, file_name)
                if os.path.isfile(file_path):
                    list_file_patah.append(file_path)
                        # do something with the contents of the file
        return list_file_patah
    
    
    def get_all_topics(self):
        # Create a KafkaAdminClient instance with the specified brokers
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            # List the topics in the Kafka cluster
            topics = admin_client.list_topics()
            # print(f"Available topics: {topics}")
        except KafkaError as e:
            topics = None
            print(f"Failed to list topics: {e}")
        return topics
    

    def get_list(self):
        l1 = [self.topics[0], self.list_file_path[0]]
        l2 = [self.topics[1], self.list_file_path[3]]
        l3 = [self.topics[2], self.list_file_path[2]]
        l4 = [self.topics[3], self.list_file_path[1]]
        list_combined = [l1,l2,l3,l4]
        dictionary = {self.topics[0] :self.list_file_path[0],self.topics[1]:self.list_file_path[3],
                self.topics[2]:self.list_file_path[2],self.topics[3]:self.list_file_path[1] }
        return (dictionary, list_combined)
    

    def fun(self,option):
        with open(self.dictionary.get(option), 'r') as file:
            # producer.send(item[1], value= file)
            data = json.load(file)
            self.producer.send(option, value = data)
            self.producer.flush()
            print('message send successfully -------', option)


if __name__ == "__main__":
    
    o = App()
    print('------This Script is also running-----')

    for item in o.get_list()[1]:
        with open(item[1], 'r') as file:
            # producer.send(item[1], value= file)
            data = json.load(file)
            o.producer.send(item[0], value = data)
            o.producer.flush()
            print('message send successfully -------', item[0])

