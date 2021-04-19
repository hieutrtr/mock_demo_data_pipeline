from pod_data_pipeline import PodDataPipeline
import json

if __name__ == '__main__':
    podPipe = PodDataPipeline()
    files = podPipe.generatePODFiles()
    for f in files:
        edata = podPipe.encryptBySignature(json.dumps(f["data"]))
        wf = open(f["name"], "wb")
        wf.write(edata)
        wf.close()
        # # write file into local
        podPipe.uploadS3(f)
