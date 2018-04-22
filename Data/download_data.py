from datetime import datetime
import json
import sys
import subprocess as sp

from numerapi.numerapi import NumerAPI

def main(login_file):
    
    # load the login file
    with open(login_file) as f:
        content = f.readlines()

    # strip the newline characters and any whitespace
    content = [x.strip() for x in content] 

    # set up paths for download of dataset and upload of predictions
    now = datetime.now().strftime("%Y%m%d")
    dataset_parent_folder = "./datasets/"
    dataset_name = "numerai_dataset_{0}/example_predictions.csv".format(now)
    dataset_path = "{0}/{1}".format(dataset_parent_folder, dataset_name)
    
    # most API calls do not require logging in
    napi = NumerAPI(verbosity="info")

    # log in
    credentials = napi.login(email=content[0], password=content[1], mfa_enabled=True)
    print(json.dumps(credentials, indent=2))

    # download current dataset
    dl_succeeded = napi.download_current_dataset(dest_path=dataset_parent_folder,
                                                 unzip=True)
    print("download succeeded: " + str(dl_succeeded))
    
    sp.call("mv " + dataset_parent_folder + "/*.zip ZipFiles/", shell=True)
    sp.call("rm " + dataset_parent_folder + "/*/example*")
    
if __name__=='__main__':
	
	main(sys.argv[1])
