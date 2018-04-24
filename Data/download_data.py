from datetime import datetime
import subprocess as sp

from numerapi import NumerAPI

def downloadNumeraiData():

    # set up paths for download of dataset and upload of predictions
    dataset_parent_folder = "./datasets/"
    
    # We don't need to login in order to download the dataset
    napi = NumerAPI(verbosity="info")

    # download current dataset
    napi.download_current_dataset(dest_path=dataset_parent_folder,unzip=True)
    
    sp.call("mv " + dataset_parent_folder + "/*.zip ZipFiles/", shell=True)
    sp.call("rm " + dataset_parent_folder + "/*/example*")
    
if __name__=='__main__':
	
	downloadNumeraiData()
