from types import prepare_class
import BI_utility as bi
import scanDetails_utility as scan
import pandas as pd
import json
import scan_queuereport_details as scan_stuck

def main():
    try:
        print("BI job Started")
      print("BI job Started")
        bi.data_load_job()
        print("BI job finished")
        print("Report sending to team")
        scan.scan_details_job()
        print("Report sent to internal team")        
        scan_stuck.scan_details_job()
        print("Report sent to developer team")       
        print("Job end")
    except Exception as error:
        print(error)

if __name__ == "__main__":
    main()
