import time
from cryptography.fernet import Fernet
import boto3
import hashlib, json

class PodDataPipeline:
    def generatePODFiles(self):
        files = []

        for u in range(10):
            file = {
                "data": [],
                "name": "",
                "owner": {}
            }
            for i in range(1000):
                recordTemplate = {
                    "client_id": "B456789",
                    "writer": "SCS.sdc_test_1.read_stream_data",
                    "reader": "SCS.sdc_test_1.data_polling",
                    "action_type": "sppa",
                    "action_value": {"aspiration_job_profile_id": "1b1f6d48-226c-38d4-70b9-cdfc49ac073344444_"},
                    "signature":  {},
                    "created_at": 0
                }
                recordTemplate["signature"]["email"] = "user{}@scs.com.sg".format(u)
                recordTemplate["created_at"] = int(time.time() - 3600*(i%72))
                file["data"].append(recordTemplate)
            file["name"] = "B456789_sppa_{}".format(u)
            file["owner"] = "user{}@scs.com.sg".format(u)
            files.append(file)

        # print(files)
        return files

    def encryptBySignature(self, data):
        key = Fernet.generate_key()
        f = Fernet(key)
        print("encrypt key {}".format(key.decode()))
        encrypted_data = f.encrypt(data.encode())
        return encrypted_data

    def uploadS3(self, file):
        s3 = boto3.resource('s3')
        BUCKET = "poddatademo"
        owner = hashlib.sha224(json.dumps(file["owner"]).encode()).hexdigest()
        s3.Bucket(BUCKET).upload_file(file["name"], "{}/{}".format(owner,file["name"]))

