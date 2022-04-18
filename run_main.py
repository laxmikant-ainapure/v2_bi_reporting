from types import prepare_class
import BI_utility as bi
import scanDetails_utility as scan
import pandas as pd
import json

def main():
    try:
        print("BI job Started")
        bi.data_load_job()
        print("BI job finished")
        print("Report sending to team")
        scan.scan_details_job()
        print("Report sent")
    except Exception as error:
        print(error)

if __name__ == "__main__":
    main()
